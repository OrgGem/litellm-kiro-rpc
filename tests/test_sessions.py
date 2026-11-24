import asyncio

import pytest

from litellm_kiro_rpc.contracts import Config, Event, GatewayError, Request
from litellm_kiro_rpc.sessions import SessionPool


class Agent:
    """External agent boundary: reports whether input was bootstrap or delta."""

    def __init__(self, model, system):
        self.turns = 0

    async def connect(self):
        pass

    async def stream(self, prompt):
        self.turns += 1
        yield Event(text=f"turn-{self.turns}:" + prompt)
        yield Event(done=True)

    async def close(self):
        pass

    def kill(self):
        pass


async def answer(pool, messages, cid="chat", commit=True):
    request = Request.parse(
        {"model": "kiro-default", "messages": messages}, cid, pool.config
    )
    async with pool.turn(request) as turn:
        text = "".join([event.text async for event in turn])
        if commit:
            turn.commit()
        return text


async def test_completed_history_reuses_only_new_user_message():
    pool = SessionPool(Config(), Agent)
    try:
        messages = [{"role": "user", "content": "hello"}]
        first = await answer(pool, messages)
        messages += [
            {"role": "assistant", "content": first},
            {"role": "user", "content": "next"},
        ]
        assert await answer(pool, messages) == "turn-2:next"
        assert (await answer(pool, [{"role": "user", "content": "edited"}])).startswith(
            "turn-1:"
        )
    finally:
        await pool.close()


@pytest.mark.parametrize("cid,commit", [(None, True), ("chat", False)])
async def test_no_id_or_failed_delivery_never_retained(cid, commit):
    pool = SessionPool(Config(), Agent)
    try:
        await answer(pool, [{"role": "user", "content": "hello"}], cid, commit)
        assert not pool.sessions
    finally:
        await pool.close()


async def test_idle_expiry_and_lru_capacity():
    pool = SessionPool(Config(max_sessions=1, idle_ttl=0.03), Agent)
    try:
        await answer(pool, [{"role": "user", "content": "one"}], "a")
        await answer(pool, [{"role": "user", "content": "two"}], "b")
        assert len(pool.sessions) == 1
        await asyncio.sleep(0.1)
        assert not pool.sessions
    finally:
        await pool.close()


@pytest.mark.parametrize(
    "behavior,code",
    [
        ("missing", "missing_terminal_result"),
        ("overflow", "backpressure_exceeded"),
        ("timeout", "deadline_exceeded"),
    ],
)
async def test_failed_turn_evicts_session(behavior, code):
    class Bad(Agent):
        async def stream(self, prompt):
            if behavior == "timeout":
                await asyncio.sleep(100)
            for _ in range(100 if behavior == "overflow" else 1):
                yield Event(text="x")

    pool = SessionPool(Config(timeout=0.05, queue_size=4), Bad)
    try:
        with pytest.raises(GatewayError, match=code):
            await answer(pool, [{"role": "user", "content": "hello"}])
        assert not pool.sessions
        assert pool.pending == 0
    finally:
        await pool.close()


async def test_cancellation_closes_in_owner_task():
    entered, closed = asyncio.Event(), asyncio.Event()

    class Waiting(Agent):
        async def connect(self):
            self.owner = asyncio.current_task()

        async def stream(self, prompt):
            entered.set()
            await asyncio.sleep(100)
            yield Event(done=True)

        async def close(self):
            assert asyncio.current_task() is self.owner
            closed.set()

    pool = SessionPool(Config(), Waiting)
    task = asyncio.create_task(answer(pool, [{"role": "user", "content": "hello"}]))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set() and not pool.sessions
    await pool.close()


async def test_same_conversation_waits_and_rechecks_history():
    entered, release = asyncio.Event(), asyncio.Event()
    created = []

    class Waiting(Agent):
        def __init__(self, *args):
            super().__init__(*args)
            created.append(self)

        async def stream(self, prompt):
            entered.set()
            await release.wait()
            yield Event(text="answer")
            yield Event(done=True)

    pool = SessionPool(Config(), Waiting)
    messages = [{"role": "user", "content": "hello"}]
    first = asyncio.create_task(answer(pool, messages))
    await entered.wait()
    second = asyncio.create_task(
        answer(
            pool,
            messages
            + [
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "next"},
            ],
        )
    )
    await asyncio.sleep(0.01)
    assert len(created) == 1
    release.set()
    await asyncio.gather(first, second)
    assert len(created) == 1
    await pool.close()


async def test_shutdown_during_idle_reap_does_not_swallow_cancellation():
    closing = asyncio.Event()

    class SlowClose(Agent):
        async def close(self):
            closing.set()
            await asyncio.sleep(100)

    pool = SessionPool(Config(idle_ttl=0.01, cleanup_timeout=0.05), SlowClose)
    await answer(pool, [{"role": "user", "content": "hello"}])
    await asyncio.wait_for(closing.wait(), 1)
    await asyncio.wait_for(pool.close(), 1)
    assert not pool.sessions


async def test_slash_leading_text_is_preserved_as_literal_delta():
    pool = SessionPool(Config(), Agent)
    try:
        messages = [{"role": "user", "content": "hello"}]
        first = await answer(pool, messages)
        response = await answer(
            pool,
            messages
            + [
                {"role": "assistant", "content": first},
                {"role": "user", "content": "/home/report"},
            ],
        )
        assert response.startswith("turn-2:Literal user message")
        assert '"content": "/home/report"' in response
    finally:
        await pool.close()


async def test_waiting_same_conversation_does_not_hold_concurrency_slot():
    a1_entered, release_a1 = asyncio.Event(), asyncio.Event()

    class Gated(Agent):
        async def stream(self, prompt):
            self.turns += 1
            if "a1" in prompt and self.turns == 1:
                a1_entered.set()
                await release_a1.wait()
            yield Event(text=f"turn-{self.turns}:{prompt}")
            yield Event(done=True)

    pool = SessionPool(Config(concurrency=2, max_sessions=4), Gated)
    try:
        a1 = asyncio.create_task(
            answer(pool, [{"role": "user", "content": "a1"}], "conv-a")
        )
        await a1_entered.wait()
        bootstrap_a1 = Request.parse(
            {"model": "kiro-default", "messages": [{"role": "user", "content": "a1"}]},
            "conv-a",
            pool.config,
        ).bootstrap()
        a2 = asyncio.create_task(
            answer(
                pool,
                [
                    {"role": "user", "content": "a1"},
                    {"role": "assistant", "content": "turn-1:" + bootstrap_a1},
                    {"role": "user", "content": "a2"},
                ],
                "conv-a",
            )
        )
        await asyncio.sleep(0.03)
        b_res = await asyncio.wait_for(
            answer(pool, [{"role": "user", "content": "b1"}], "conv-b"),
            timeout=0.5,
        )
        assert b_res.startswith("turn-1:")
        release_a1.set()
        _, a2_res = await asyncio.gather(a1, a2)
        assert a2_res == "turn-2:a2"
    finally:
        release_a1.set()
        await pool.close()
        await asyncio.gather(a1, a2, return_exceptions=True)

