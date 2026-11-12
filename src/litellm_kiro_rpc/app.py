"""Authenticated ASGI host; commit a reusable turn only after HTTP delivery."""

import asyncio
import contextlib
import hmac
import json
import time
import uuid

from .contracts import GatewayError, Request, strict_json


def completion(model, text, usage, request_id):
    result = {
        "id": "chatcmpl-" + request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
    }
    if usage is not None:
        result["usage"] = usage
    return result


class App:
    def __init__(self, pool, preflight=None):
        self.pool, self.config, self.preflight = pool, pool.config, preflight
        key = self.config.api_key
        if (
            not isinstance(key, str)
            or len(key) < 24
            or not key.isascii()
            or any(c.isspace() for c in key)
        ):
            raise ValueError("Set a 24+ character ASCII gateway key")
        self.ready = False

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    try:
                        if self.preflight:
                            await self.preflight()
                        self.ready = True
                        await send({"type": "lifespan.startup.complete"})
                    except Exception:
                        await self.pool.close()
                        await send(
                            {
                                "type": "lifespan.startup.failed",
                                "message": "Backend readiness failed; run doctor",
                            }
                        )
                        return
                elif event["type"] == "lifespan.shutdown":
                    self.ready = False
                    await self.pool.close()
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        started = False
        request_id = uuid.uuid4().hex

        async def bounded_send(message):
            nonlocal started
            async with asyncio.timeout(10):
                await send(message)
            if message["type"] == "http.response.start":
                started = True

        async def respond(status, body):
            encoded = json.dumps(body, ensure_ascii=True, allow_nan=False).encode()
            await bounded_send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"x-request-id", request_id.encode()),
                    ],
                }
            )
            await bounded_send({"type": "http.response.body", "body": encoded})

        try:
            auth = [
                v for k, v in scope.get("headers", []) if k.lower() == b"authorization"
            ]
            if len(auth) != 1 or not hmac.compare_digest(
                auth[0], ("Bearer " + self.config.api_key).encode()
            ):
                raise GatewayError("invalid_api_key", 401)
            if not self.ready:
                raise GatewayError("not_ready", 503)
            path, method = scope["path"], scope["method"]
            if method == "GET" and path == "/v1/models":
                return await respond(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {
                                "id": alias,
                                "object": "model",
                                "created": 0,
                                "owned_by": "agent-cli",
                            }
                            for alias in self.config.models
                        ],
                    },
                )
            if method == "GET" and path == "/health/ready":
                return await respond(
                    200,
                    {
                        "status": "ready",
                        "model_access_verified": False,
                        "role_semantics": "text-transcript",
                        "session_reuse": "X-Conversation-ID",
                    },
                )
            if method != "POST" or path != "/v1/chat/completions":
                raise GatewayError("not_found", 404)
            ids = [
                v
                for k, v in scope.get("headers", [])
                if k.lower() == b"x-conversation-id"
            ]
            if len(ids) > 1:
                raise GatewayError("invalid_conversation_id", 400)
            cid = ids[0].decode("ascii") if ids else None
            body = bytearray()
            async with asyncio.timeout(10):
                while True:
                    event = await receive()
                    if event["type"] == "http.disconnect":
                        return
                    body.extend(event.get("body", b""))
                    if len(body) > self.config.max_input_bytes:
                        raise GatewayError("request_too_large", 413)
                    if not event.get("more_body", False):
                        break
            try:
                payload = strict_json(body.decode("utf-8"))
            except (ValueError, UnicodeError):
                raise GatewayError("invalid_json", 400) from None
            request = Request.parse(payload, cid, self.config)

            async def generate():
                async with self.pool.turn(request) as turn:
                    if not request.stream:
                        async for _ in turn:
                            pass
                        await respond(
                            200,
                            completion(
                                request.model,
                                "".join(turn.text),
                                turn.terminal.usage,
                                request_id,
                            ),
                        )
                    else:
                        first = await anext(turn)
                        await bounded_send(
                            {
                                "type": "http.response.start",
                                "status": 200,
                                "headers": [
                                    (b"content-type", b"text/event-stream"),
                                    (b"cache-control", b"no-cache"),
                                    (b"x-accel-buffering", b"no"),
                                ],
                            }
                        )

                        async def emit(choices, usage=None):
                            chunk = {
                                "id": "chatcmpl-" + request_id,
                                "object": "chat.completion.chunk",
                                "created": int(time.time()),
                                "model": request.model,
                                "choices": choices,
                            }
                            if request.include_usage:
                                chunk["usage"] = usage
                            await bounded_send(
                                {
                                    "type": "http.response.body",
                                    "body": (
                                        "data: " + json.dumps(chunk) + "\n\n"
                                    ).encode(),
                                    "more_body": True,
                                }
                            )

                        await emit(
                            [
                                {
                                    "index": 0,
                                    "delta": {"role": "assistant"},
                                    "finish_reason": None,
                                }
                            ]
                        )
                        event = first
                        while event is not None:
                            if not event.done:
                                await emit(
                                    [
                                        {
                                            "index": 0,
                                            "delta": {"content": event.text},
                                            "finish_reason": None,
                                        }
                                    ]
                                )
                            event = await anext(turn, None)
                        await emit([{"index": 0, "delta": {}, "finish_reason": "stop"}])
                        if request.include_usage:
                            await emit([], turn.terminal.usage)
                        await bounded_send(
                            {
                                "type": "http.response.body",
                                "body": b"data: [DONE]\n\n",
                                "more_body": False,
                            }
                        )
                    turn.commit()

            async def disconnected():
                while (await receive())["type"] != "http.disconnect":
                    pass

            task, watcher = (
                asyncio.create_task(generate()),
                asyncio.create_task(disconnected()),
            )
            try:
                done, _ = await asyncio.wait(
                    {task, watcher}, return_when=asyncio.FIRST_COMPLETED
                )
                if watcher in done:
                    task.cancel()
                else:
                    await task
            finally:
                task.cancel()
                watcher.cancel()
                await asyncio.gather(task, watcher, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = (
                exc
                if isinstance(exc, GatewayError)
                else GatewayError("internal_error", 500)
            )
            if isinstance(exc, UnicodeError):
                error = GatewayError("invalid_request", 400)
            with contextlib.suppress(OSError, TimeoutError):
                if not started:
                    await respond(error.status, error.payload())
                else:
                    await bounded_send(
                        {
                            "type": "http.response.body",
                            "body": (
                                "data: " + json.dumps(error.payload()) + "\n\n"
                            ).encode(),
                            "more_body": False,
                        }
                    )
