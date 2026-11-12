import asyncio

import pytest

from litellm_kiro_rpc.contracts import Config, GatewayError
from litellm_kiro_rpc.driver import ACPDriver, KiroDriver


class Peer:
    tree = None

    def __init__(self):
        self.failure = asyncio.get_running_loop().create_future()
        self.calls = []

    async def request(self, method, params):
        self.calls.append((method, params))
        if method == "initialize":
            return {"protocolVersion": 1}
        if method == "session/new":
            return {
                "sessionId": "session",
                "modes": {"currentModeId": "gateway-text-only"},
            }
        if method == "session/prompt":
            self.on_update(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session",
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "hello"},
                        },
                    },
                }
            )
            self.on_update(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session",
                        "update": {
                            "sessionUpdate": "session_info_update",
                            "_meta": {"kiro": {"kind": "turn_end"}},
                        },
                    },
                }
            )
            await asyncio.sleep(0.01)
            return {"stopReason": "end_turn"}

    async def close(self):
        pass


async def test_prompt_response_completes_turn_not_turn_end_notification():
    peer = Peer()
    driver = ACPDriver(peer, Config(), "isolated-workspace")
    await driver.connect()
    events = [event async for event in driver.stream("hi")]
    assert events[0].text == "hello" and events[-1].done
    assert events[-1].usage is None
    assert peer.calls[1][1]["mcpServers"] == []


async def test_production_gate_never_spawns_unverified_profile(monkeypatch):
    from litellm_kiro_rpc import driver as module

    class VersionProcess:
        returncode = 0

        async def communicate(self):
            return b"kiro-cli 3.1.0", b""

    async def spawn(*args, **kwargs):
        assert args[-1] == "--version", "Gate must not start ACP"
        return VersionProcess()

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", spawn)
    driver = KiroDriver(Config(), None, "")
    with pytest.raises(GatewayError, match="kiro_profile_isolation_not_verified"):
        await driver.connect()


async def test_wrong_session_and_tools_are_rejected():
    peer = Peer()
    driver = ACPDriver(peer, Config(), "isolated-workspace")
    await driver.connect()
    for session, kind in [("other", "agent_message_chunk"), ("session", "tool_call")]:
        with pytest.raises(GatewayError):
            driver.update(
                {
                    "method": "session/update",
                    "params": {"sessionId": session, "update": {"sessionUpdate": kind}},
                }
            )
