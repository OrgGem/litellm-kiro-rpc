import asyncio
import json

import httpx
import pytest
from test_sessions import Agent

from litellm_kiro_rpc.app import App
from litellm_kiro_rpc.contracts import Config, Event
from litellm_kiro_rpc.sessions import SessionPool

KEY = "sk-fixture-key-123456789012345"


@pytest.mark.parametrize("stream", [False, True])
async def test_auth_completion_and_sse(stream):
    pool = SessionPool(Config(api_key=KEY), Agent)
    app = App(pool)
    app.ready = True
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            assert (await client.get("/v1/models")).status_code == 401
            client.headers["Authorization"] = "Bearer " + KEY
            assert (await client.get("/v1/models")).json()["data"][0][
                "id"
            ] == "kiro-default"
            body = {
                "model": "kiro-default",
                "messages": [{"role": "user", "content": "héllo"}],
                "stream": stream,
            }
            response = await client.post(
                "/v1/chat/completions", json=body, headers={"X-Conversation-ID": "chat"}
            )
            assert response.status_code == 200
            if stream:
                assert response.text.endswith("data: [DONE]\n\n")
                chunks = [
                    json.loads(line[6:])
                    for line in response.text.splitlines()
                    if line.startswith("data: {")
                ]
                assert chunks[-1]["choices"][0]["finish_reason"] == "stop"
            else:
                assert response.json()["choices"][0]["message"]["content"].startswith(
                    "turn-1:"
                )
            assert len(pool.sessions) == 1
    finally:
        await pool.close()


async def test_disconnect_cancels_backend_before_first_chunk():
    entered, closed = asyncio.Event(), asyncio.Event()

    class Waiting(Agent):
        async def stream(self, prompt):
            entered.set()
            await asyncio.sleep(100)
            yield Event(done=True)

        async def close(self):
            closed.set()

    pool = SessionPool(Config(api_key=KEY), Waiting)
    app = App(pool)
    app.ready = True
    first = True

    async def receive():
        nonlocal first
        if first:
            first = False
            return {
                "type": "http.request",
                "body": json.dumps(
                    {
                        "model": "kiro-default",
                        "messages": [{"role": "user", "content": "hello"}],
                    }
                ).encode(),
            }
        await entered.wait()
        return {"type": "http.disconnect"}

    sent = []

    async def send(event):
        sent.append(event)

    await app(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"authorization", ("Bearer " + KEY).encode())],
        },
        receive,
        send,
    )
    assert closed.is_set() and not pool.sessions and not sent
    await pool.close()
