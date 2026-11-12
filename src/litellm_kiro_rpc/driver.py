"""Kiro V3 ACP adapter. Production remains gated on profile isolation proof.

The wire adapter is usable with a controlled transport for protocol conformance
tests. The production factory never launches an unverified default agent.
"""

import asyncio
import contextlib

from .contracts import Event, GatewayError
from .profile import Profile
from .transport import ACPClient

CLI_VERSION = "3.1.0"


class ACPDriver:
    def __init__(self, client, config, cwd, model=None):
        self.client, self.config, self.cwd, self.model = client, config, cwd, model
        self.session = None
        self.queue = asyncio.Queue(config.queue_size)
        self.active = None
        client.on_update = self.update

    async def connect(self):
        info = await self.client.request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientInfo": {"name": "litellm-kiro-rpc", "version": "0.1.0.dev0"},
                "clientCapabilities": {
                    "fs": {"readTextFile": False, "writeTextFile": False},
                    "terminal": False,
                },
            },
        )
        if info.get("protocolVersion") != 1:
            raise GatewayError("unsupported_protocol", 503)
        result = await self.client.request(
            "session/new",
            {
                "cwd": self.cwd,
                "additionalDirectories": [],
                "mcpServers": [],
                "_meta": {"kiro": {"modeId": "gateway-text-only"}},
            },
        )
        self.session = result.get("sessionId")
        if not isinstance(self.session, str) or not self.session:
            raise GatewayError("invalid_session")
        if result.get("modes", {}).get("currentModeId") != "gateway-text-only":
            raise GatewayError("unsafe_backend_profile", 503)
        if self.model:
            options = result.get("configOptions", [])
            if not any(
                option.get("id") == "model"
                for option in options
                if isinstance(option, dict)
            ):
                raise GatewayError("model_selection_unavailable", 503)
            changed = await self.client.request(
                "session/set_config_option",
                {"sessionId": self.session, "configId": "model", "value": self.model},
            )
            if not any(
                o.get("id") == "model" and o.get("currentValue") == self.model
                for o in changed.get("configOptions", [])
                if isinstance(o, dict)
            ):
                raise GatewayError("model_selection_failed", 503)

    def update(self, message):
        if message.get("method") != "session/update":
            return
        params = message.get("params", {})
        if params.get("sessionId") != self.session:
            # Session initialization notifications contain no generated text.
            if self.session is None:
                return
            raise GatewayError("unexpected_session")
        update = params.get("update", {})
        kind = update.get("sessionUpdate")
        if kind in ("tool_call", "tool_call_update"):
            raise GatewayError("unexpected_tool_activity")
        if kind == "agent_message_chunk":
            if self.active is None or update.get("_meta", {}).get("kiro", {}).get(
                "replay"
            ):
                raise GatewayError("unexpected_agent_output")
            block = update.get("content", {})
            if block.get("type") != "text" or not isinstance(block.get("text"), str):
                raise GatewayError("unsupported_backend_content")
            try:
                self.queue.put_nowait(Event(text=block["text"]))
            except asyncio.QueueFull:
                raise GatewayError("backpressure_exceeded") from None

    async def stream(self, prompt):
        self.active = asyncio.create_task(
            self.client.request(
                "session/prompt",
                {
                    "sessionId": self.session,
                    "prompt": [{"type": "text", "text": prompt}],
                },
            )
        )
        try:
            while not self.active.done() or not self.queue.empty():
                if self.client.failure.done():
                    raise self.client.failure.result()
                if not self.queue.empty():
                    yield self.queue.get_nowait()
                    continue
                get = asyncio.create_task(self.queue.get())
                try:
                    done, _ = await asyncio.wait(
                        {get, self.active, self.client.failure},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if get in done:
                        yield get.result()
                finally:
                    get.cancel()
                    await asyncio.gather(get, return_exceptions=True)
            result = await self.active
            if self.client.failure.done():
                raise self.client.failure.result()
            if result.get("stopReason") != "end_turn":
                raise GatewayError("backend_turn_failed")
            yield Event(done=True)  # Kiro usage units are not assumed to be tokens.
        finally:
            if self.active and not self.active.done():
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(min(self.config.cleanup_timeout / 2, 2)):
                        await self.client.notify(
                            "session/cancel", {"sessionId": self.session}
                        )
                        await asyncio.shield(self.active)
                self.active.cancel()
            if self.active:
                await asyncio.gather(self.active, return_exceptions=True)
            self.active = None

    async def close(self):
        self.kill()
        await self.client.close()

    def kill(self):
        if self.client.tree:
            self.client.tree.kill()


class KiroDriver:
    def __init__(self, config, model, system):
        self.config, self.model = config, model
        self.profile = self.client = self.adapter = None

    async def connect(self):
        # No override flag: release this gate only alongside a verified profile
        # launcher preserving official CLI auth without reading/copying tokens.
        await doctor(self.config)
        self.profile = Profile()
        self.client = ACPClient(self.config, lambda message: None)
        self.adapter = ACPDriver(
            self.client, self.config, str(self.profile.workspace), self.model
        )
        try:
            await self.client.start(
                [
                    self.config.executable or "kiro-cli",
                    "acp",
                    "--agent-engine=v3",
                    "--auth-method=cli",
                ],
                self.profile.workspace,
                self.profile.env,
            )
            await self.adapter.connect()
        except BaseException:
            await self.close()
            raise

    async def stream(self, prompt):
        async for event in self.adapter.stream(prompt):
            yield event

    async def close(self):
        try:
            if self.adapter:
                await self.adapter.close()
        finally:
            if self.profile:
                self.profile.close()

    def kill(self):
        if self.adapter:
            self.adapter.kill()


async def doctor(config):
    proc = await asyncio.create_subprocess_exec(
        config.executable or "kiro-cli",
        "--version",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), 10)
        if proc.returncode or CLI_VERSION not in out.decode().split():
            raise GatewayError("unsupported_cli_version", 503)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    raise GatewayError("kiro_profile_isolation_not_verified", 503)
