"""Bounded live sessions; an actor owns each SDK connection for its entire life."""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from .contracts import Event, GatewayError, fingerprints


class Actor:
    def __init__(self, driver, config):
        self.driver, self.config = driver, config
        self.inbox = asyncio.Queue(maxsize=1)
        self.ready = asyncio.get_running_loop().create_future()
        self.task = asyncio.create_task(self.run())
        self.history = ()
        self.busy = True
        self.used = time.monotonic()
        self.model = None
        self.failure = None

    async def run(self):
        try:
            await self.driver.connect()
            self.ready.set_result(None)
            while True:
                prompt, output = await self.inbox.get()
                try:
                    terminal = False
                    async for event in self.driver.stream(prompt):
                        if terminal or not isinstance(event, Event):
                            raise GatewayError("invalid_backend_event")
                        terminal = event.done
                        output.put_nowait(event)
                    if not terminal:
                        raise GatewayError("missing_terminal_result")
                    output.put_nowait(None)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.failure = (
                        GatewayError("backpressure_exceeded")
                        if isinstance(error, asyncio.QueueFull)
                        else error
                        if isinstance(error, GatewayError)
                        else GatewayError()
                    )
                    if not output.full():
                        output.put_nowait(self.failure)
                    return
        except BaseException:
            if not self.ready.done():
                self.ready.set_exception(GatewayError("backend_startup_failed", 503))
        finally:
            # Same task as connect: SDK AnyIO task groups require this ownership.
            try:
                await self.driver.close()
            finally:
                self.driver.kill()

    async def stop(self):
        owner = asyncio.current_task()
        cancellation_count = owner.cancelling()
        self.task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.shield(self.task), self.config.cleanup_timeout
            )
        except asyncio.CancelledError:
            self.driver.kill()
            self.task.cancel()
            # A cancelled child is normal cleanup; a NEW cancellation of the
            # caller must propagate (particularly when stopping the reaper).
            if owner.cancelling() > cancellation_count:
                raise
        except TimeoutError:
            self.driver.kill()
            self.task.cancel()
        except Exception:
            self.driver.kill()
        finally:
            if self.ready.done() and not self.ready.cancelled():
                self.ready.exception()  # consume a failed startup even if caller was cancelled


class Turn:
    def __init__(self, actor, prompt, config):
        self.output = asyncio.Queue(maxsize=config.queue_size)
        self.actor, self.config = actor, config
        self.text = []
        self.size = 0
        self.terminal = None
        self.finished = False
        self.committed = False
        actor.inbox.put_nowait((prompt, self.output))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.actor.failure:
            raise self.actor.failure
        event = await self.output.get()
        if isinstance(event, Exception):
            raise event
        if event is None:
            self.finished = True
            raise StopAsyncIteration
        if event.done:
            self.terminal = event
        else:
            self.size += len(event.text.encode("utf-8"))
            if self.size > self.config.max_output_bytes:
                raise GatewayError("output_too_large")
            self.text.append(event.text)
        return event

    def commit(self):
        if not self.finished or self.terminal is None:
            raise GatewayError("incomplete_turn")
        self.committed = True


class SessionPool:
    def __init__(self, config, factory):
        self.config, self.factory = config, factory
        self.sessions = {}
        self.condition = asyncio.Condition()
        self.slots = asyncio.Semaphore(config.concurrency)
        self.pending = 0
        self.closed = False
        self.reaper = None

    async def reap(self):
        try:
            while True:
                await asyncio.sleep(min(self.config.idle_ttl, 1))
                async with self.condition:
                    for key, actor in list(self.sessions.items()):
                        if (
                            not actor.busy
                            and time.monotonic() - actor.used >= self.config.idle_ttl
                        ):
                            await actor.stop()
                            del self.sessions[key]
                    self.condition.notify_all()
        except asyncio.CancelledError:
            pass

    async def acquire(self, request, key):
        async with self.condition:
            while True:
                if self.closed:
                    raise GatewayError("shutting_down", 503)
                actor = self.sessions.get(key)
                if actor and actor.busy:
                    await self.condition.wait()
                    continue
                if actor:
                    reusable = (
                        not actor.task.done()
                        and actor.model
                        == (request.model, self.config.models[request.model])
                        and actor.history == fingerprints(request.messages[:-1])
                        and time.monotonic() - actor.used < self.config.idle_ttl
                    )
                    if reusable:
                        actor.busy = True
                        return actor, request.delta()
                    await actor.stop()
                    del self.sessions[key]
                if len(self.sessions) >= self.config.max_sessions:
                    idle = [
                        (a.used, k, a) for k, a in self.sessions.items() if not a.busy
                    ]
                    if not idle:
                        await self.condition.wait()
                        continue
                    _, old_key, old_actor = min(idle)
                    await old_actor.stop()
                    del self.sessions[old_key]
                actor = Actor(
                    self.factory(self.config.models[request.model], request.system),
                    self.config,
                )
                actor.model = (request.model, self.config.models[request.model])
                self.sessions[key] = actor
                return actor, request.bootstrap()

    @asynccontextmanager
    async def turn(self, request):
        if self.closed:
            raise GatewayError("shutting_down", 503)
        if self.pending >= self.config.max_pending:
            raise GatewayError("queue_full", 429)
        self.pending += 1
        if self.reaper is None:
            self.reaper = asyncio.create_task(self.reap())
        key = request.conversation or uuid.uuid4().hex
        actor = turn = None
        try:
            async with asyncio.timeout(self.config.timeout):
                async with self.slots:
                    actor, prompt = await self.acquire(request, key)
                    await asyncio.shield(actor.ready)
                    turn = Turn(actor, prompt, self.config)
                    yield turn
        except TimeoutError:
            raise GatewayError("deadline_exceeded", 504) from None
        finally:
            if actor:
                async with self.condition:
                    if (
                        turn
                        and turn.committed
                        and request.conversation
                        and not self.closed
                    ):
                        actor.history = fingerprints(
                            (*request.messages, ("assistant", "".join(turn.text)))
                        )
                        actor.busy = False
                        actor.used = time.monotonic()
                    else:
                        await actor.stop()
                        if self.sessions.get(key) is actor:
                            del self.sessions[key]
                    self.condition.notify_all()
            self.pending -= 1

    async def close(self):
        self.closed = True
        if self.reaper:
            self.reaper.cancel()
            await self.reaper
        async with self.condition:
            for actor in self.sessions.values():
                await actor.stop()
            self.sessions.clear()
            self.condition.notify_all()
