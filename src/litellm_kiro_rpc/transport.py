import asyncio
import json
import os

from .contracts import GatewayError
from .process import ProcessTree, stop_process


def remote_error(error):
    return GatewayError("backend_rpc_error")


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("Nonfinite JSON number")

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


class ACPClient:
    def __init__(self, config, on_update):
        self.config = config
        self.on_update = on_update
        self.pending = {}
        self.counter = 0
        self.proc = self.tree = self.reader = None
        self.closing = False
        self.failure = asyncio.get_running_loop().create_future()
        self.write_lock = asyncio.Lock()

    async def start(self, command, cwd, env, read=True):
        # Shield spawn so cancellation cannot strand a process before assignment.
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *command,
                cwd=cwd,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=self.config.frame_limit,
                start_new_session=os.name != "nt",
                creationflags=0x00000004 if os.name == "nt" else 0,
            )
        )
        try:
            self.proc = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            self.proc = await spawn
            try:
                self.tree = ProcessTree(self.proc.pid)
            finally:
                await self.close()
            raise
        try:
            self.tree = ProcessTree(self.proc.pid)
            self.tree.resume()
        except OSError:
            await self.close()
            raise GatewayError("process_isolation_failed", 503) from None
        if read:
            self.reader = asyncio.create_task(self._read())

    def fail(self, error):
        if not self.failure.done():
            self.failure.set_result(error)
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)

    async def send(self, message):
        data = (json.dumps(message, ensure_ascii=True) + "\n").encode()
        if len(data) > self.config.frame_limit:
            raise GatewayError("request_too_large", 413)
        async with self.write_lock:
            self.proc.stdin.write(data)
            await self.proc.stdin.drain()

    async def request(self, method, params):
        if self.failure.done():
            raise self.failure.result()
        self.counter += 1
        request_id = self.counter
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            await self.send(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
            )
            return await future
        finally:
            self.pending.pop(request_id, None)
            if future.done() and not future.cancelled():
                future.exception()  # Also retrieve exceptions on a send/cancel race.

    async def notify(self, method, params):
        await self.send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _server_request(self, message):
        request_id = message["id"]
        if type(request_id) not in (str, int):
            raise GatewayError()
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise GatewayError()
        if message["method"] == "session/request_permission":
            options = params.get("options", [])
            if not isinstance(options, list):
                raise GatewayError()
            reject = next(
                (
                    o
                    for o in options
                    if isinstance(o, dict)
                    and o.get("kind") == "reject_once"
                    and isinstance(o.get("optionId"), str)
                ),
                None,
            )
            outcome = (
                {"outcome": "selected", "optionId": reject["optionId"]}
                if reject
                else {"outcome": "cancelled"}
            )
            await self.send(
                {"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}}
            )
        else:
            await self.send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": "Unsupported client capability",
                    },
                }
            )
        # Permission/tool activity violates the text-only contract even if denied.
        raise GatewayError("unexpected_tool_request", 502)

    async def _read(self):
        try:
            count = 0
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    if not self.closing:
                        raise GatewayError("upstream_eof")
                    return
                if len(line) > self.config.frame_limit or not line.endswith(b"\n"):
                    raise GatewayError("invalid_frame")
                message = strict_json(line.decode("utf-8"))
                if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                    raise GatewayError()
                if "method" in message:
                    if (
                        not isinstance(message["method"], str)
                        or "result" in message
                        or "error" in message
                    ):
                        raise GatewayError()
                    if "id" in message:
                        await self._server_request(message)
                    else:
                        self.on_update(message)
                else:
                    request_id = message.get("id")
                    if type(request_id) is not int or request_id not in self.pending:
                        raise GatewayError("unexpected_response_id")
                    future = self.pending[request_id]
                    if future.done() or ("result" in message) == ("error" in message):
                        raise GatewayError()
                    if "error" in message:
                        future.set_exception(remote_error(message["error"]))
                    elif not isinstance(message["result"], dict):
                        raise GatewayError()
                    else:
                        future.set_result(message["result"])
                count += 1
                if count % 32 == 0:
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except GatewayError as exc:
            self.fail(exc)
        except (ValueError, UnicodeError, OSError, TypeError, KeyError):
            self.fail(GatewayError("invalid_frame"))

    async def close(self):
        self.closing = True
        if self.reader:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        for future in self.pending.values():
            if not future.done():
                future.cancel()
        if self.proc:
            await stop_process(self.proc, self.tree, self.config.cleanup_timeout)
            self.proc = None
