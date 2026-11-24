import asyncio
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

import httpx
import pytest
from jsonschema import Draft202012Validator

from litellm_kiro_rpc import native_v2
from litellm_kiro_rpc.app import App
from litellm_kiro_rpc.contracts import Config, GatewayError


def context():
    now = time.monotonic()
    return {
        "key_scope_id": "key-1",
        "key_epoch": "epoch-1",
        "environment": "test",
        "request_id": "req-1",
        "chain_id": "chain-1",
        "locator": "1" * 64,
        "session_scope": "2" * 64,
        "profile_id": "profile",
        "profile_revision": "rev-1",
        "limits": {
            "inference_seconds": 10,
            "native_deadline": now + 10,
            "chain_deadline": now + 10,
            "tool_wait_seconds": 5,
            "cleanup_seconds": 2,
            "max_handoffs": 2,
        },
    }


def profile():
    return {
        "profile_id": "profile",
        "profile_revision": "rev-1",
        "transport": "stdio",
        "models": {"kiro-default": "fixture-model"},
    }


def turn_input():
    return {
        "model_alias": "kiro-default",
        "resolved_model": "fixture-model",
        "messages": [{"role": "user", "content": "hello"}],
        "history_digest": "3" * 64,
        "toolset_digest": "4" * 64,
        "tools": [],
        "round_policy": {"tool_choice": "auto", "parallel_tool_calls": False},
        "reasoning_output": "off",
        "reasoning_effort": None,
    }


def candidate(monkeypatch, mode="normal"):
    async def allow_candidate(self):
        return None

    monkeypatch.setattr(native_v2.KiroNativeDriver, "_ensure_ready", allow_candidate)
    start = native_v2.ACPClient.start

    async def fake_start(client, command, cwd, env, read=True):
        assert command[1:] == ["acp", "--agent-engine=v3", "--auth-method=cli"]
        peer = Path(__file__).parent / "fixtures/fake_kiro_v3.py"
        await start(
            client, [sys.executable, "-u", str(peer), mode], cwd, env, read=read
        )

    monkeypatch.setattr(native_v2.ACPClient, "start", fake_start)
    return native_v2.KiroNativeDriver(Config(cleanup_timeout=2))


def test_frozen_contract_snapshot_and_all_fixtures():
    path = Path(native_v2.__file__).parent / "contract_v0_1"
    manifest = json.loads((path / "manifest.json").read_bytes())
    assert manifest["contract_hash"] == native_v2.CONTRACT_HASH
    assert all(
        hashlib.sha256((path / name).read_bytes()).hexdigest() == digest
        for name, digest in manifest["files"].items()
    )
    validator = Draft202012Validator(native_v2._contract())
    for case in json.loads((path / "fixtures.json").read_bytes()):
        assert validator.is_valid(case["document"]) == case["valid"], case["name"]


async def test_public_driver_gate_and_offline_manifest():
    driver = native_v2.KiroNativeDriver()
    manifest = await driver.capabilities({"profile_id": "profile"}, "fixture-model")
    native_v2._validate(native_v2._contract(), "CapabilityManifest", manifest)
    assert all(
        not item["live_verified"] and item["state"] == "blocked"
        for item in manifest["features"].values()
    )
    with pytest.raises(GatewayError, match="kiro_profile_isolation_not_verified"):
        await driver.open_session(context(), profile())


async def test_real_fake_acp_subprocess_text_unknown_usage_and_cleanup(monkeypatch):
    driver = candidate(monkeypatch)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    events = [event async for event in driver.events(turn)]
    assert [event["type"] for event in events] == [
        "text_delta",
        "usage",
        "native_turn_ended",
    ]
    assert events[0]["payload"] == {"text": "done"}
    assert events[1]["payload"]["input_tokens"] is None
    assert events[1]["payload"]["output_tokens"] is None
    assert events[1]["payload"]["completeness"] == "unknown"
    assert events[2]["payload"]["finish_reason"] == "stop"
    assert "do-not-export" not in repr(events) and "credits" not in repr(events)
    for event in events:
        native_v2._validate(native_v2._contract(), "AdapterEvent", event)
    proc, directory = session.client.proc, session.isolated.workspace.parent
    await driver.close_session(session, time.monotonic() + 5)
    assert proc.returncode is not None and not directory.exists()


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("tool", "unexpected_tool_activity"),
        ("replay", "unexpected_agent_output"),
        ("wrong_turn_session", "unexpected_session"),
    ],
)
async def test_invalid_updates_end_without_success(monkeypatch, mode, expected):
    driver = candidate(monkeypatch, mode)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    events = [event async for event in driver.events(turn)]
    assert events[-1]["type"] == "backend_error"
    assert events[-1]["payload"]["code"] == expected
    assert not any(
        event["type"] in ("tool_call_ready", "tool_batch_ready", "native_turn_ended")
        for event in events
    )
    await driver.close_session(session, time.monotonic() + 5)


async def test_early_wrong_session_fails_during_startup(monkeypatch):
    driver = candidate(monkeypatch, "wrong_initial_session")
    session = await driver.open_session(context(), profile())
    with pytest.raises(GatewayError, match="backend_startup_failed"):
        await driver.begin_turn(session, turn_input())
    proc = session.client.proc
    await driver.close_session(session, time.monotonic() + 5)
    assert proc is None or proc.returncode is not None


@pytest.mark.parametrize("mode", ["protocol_true", "protocol_float"])
async def test_protocol_version_must_be_an_integer(monkeypatch, mode):
    driver = candidate(monkeypatch, mode)
    session = await driver.open_session(context(), profile())
    try:
        with pytest.raises(GatewayError, match="backend_startup_failed"):
            await driver.begin_turn(session, turn_input())
    finally:
        proc = session.client.proc
        await driver.close_session(session, time.monotonic() + 5)
        assert proc is None or proc.returncode is not None


@pytest.mark.parametrize(
    "mode,reason", [("refusal", "content_filter"), ("max_tokens", "length")]
)
async def test_finish_reason_does_not_reuse_changed_native_context(
    monkeypatch, mode, reason
):
    driver = candidate(monkeypatch, mode)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    events = [event async for event in driver.events(turn)]
    assert events[-1]["payload"]["finish_reason"] == reason
    with pytest.raises(GatewayError, match="session_not_reusable"):
        await driver.begin_turn(session, turn_input())
    await driver.close_session(session, time.monotonic() + 5)


async def test_unproven_stop_reason_is_not_synthesized(monkeypatch):
    driver = candidate(monkeypatch, "max_turn_requests")
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    events = [event async for event in driver.events(turn)]
    assert events[-1]["payload"]["code"] == "backend_turn_failed"
    assert not any(event["type"] == "native_turn_ended" for event in events)
    await driver.close_session(session, time.monotonic() + 5)


async def test_history_reuse_and_slash_literal(monkeypatch):
    driver = candidate(monkeypatch)
    session = await driver.open_session(context(), profile())
    first = await driver.begin_turn(session, turn_input())
    assert [event async for event in driver.events(first)][-1][
        "type"
    ] == "native_turn_ended"
    later = turn_input()
    later["messages"] += [
        {"role": "assistant", "content": "done"},
        {"role": "user", "content": "/literal"},
    ]
    second = await driver.begin_turn(session, later)
    assert [event async for event in driver.events(second)][0]["payload"][
        "text"
    ] == "literal"
    await driver.close_session(session, time.monotonic() + 5)


async def test_cancel_pending_not_stale_turn(monkeypatch):
    driver = candidate(monkeypatch, "hold")
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    iterator = driver.events(turn)
    assert (await asyncio.wait_for(anext(iterator), 5))["type"] == "text_delta"
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(iterator), 0.05)  # turn_end event is not terminal
    proc = session.client.proc
    await driver.cancel(turn, "client_cancelled", time.monotonic() + 5)
    assert proc.returncode is not None and session.owner.done()


async def test_backpressure_preserves_error_after_queued_content(monkeypatch):
    driver = candidate(monkeypatch, "flood")
    driver.config = Config(cleanup_timeout=2, queue_size=1)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    await asyncio.wait_for(asyncio.shield(session.owner), 5)
    events = [event async for event in driver.events(turn)]
    assert [event["type"] for event in events] == ["text_delta", "backend_error"]
    assert events[-1]["payload"]["code"] == "backpressure_exceeded"
    assert [event["sequence"] for event in events] == [0, 1]
    await driver.close_session(session, time.monotonic() + 5)


async def test_terminal_releases_session_even_when_queue_is_full(monkeypatch):
    driver = candidate(monkeypatch, "hold_for_release")
    driver.config = Config(cleanup_timeout=2, queue_size=2)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    stream = driver.events(turn)
    try:
        assert (await asyncio.wait_for(anext(stream), 5))["type"] == "text_delta"
        await session.client.notify("fixture/release", {})
        assert (await asyncio.wait_for(anext(stream), 5))["type"] == "usage"
        assert (await asyncio.wait_for(anext(stream), 5))["type"] == "native_turn_ended"
        assert session.active is None
    finally:
        await stream.aclose()
        await driver.close_session(session, time.monotonic() + 5)


async def test_stale_cancel_cannot_close_a_new_turn(monkeypatch):
    driver = candidate(monkeypatch, "hold_second")
    session = await driver.open_session(context(), profile())
    first = await driver.begin_turn(session, turn_input())
    assert [event async for event in driver.events(first)][-1][
        "type"
    ] == "native_turn_ended"
    request = turn_input()
    request["messages"] += [
        {"role": "assistant", "content": "done"},
        {"role": "user", "content": "again"},
    ]
    second = await driver.begin_turn(session, request)
    stream = driver.events(second)
    assert (await asyncio.wait_for(anext(stream), 5))["type"] == "text_delta"
    await driver.cancel(first, "late_client_cancel", time.monotonic() + 5)
    assert not session.closed and not second.finished
    await stream.aclose()
    proc = session.client.proc
    await driver.cancel(second, "client_cancelled", time.monotonic() + 5)
    assert session.closed and proc.returncode is not None


async def test_startup_deadline_cleans_up_owned_peer(monkeypatch):
    driver = candidate(monkeypatch, "hang_init")
    ctx = context()
    ctx["limits"]["native_deadline"] = time.monotonic() + 0.1
    session = await driver.open_session(ctx, profile())
    with pytest.raises(GatewayError, match="deadline_exceeded"):
        await driver.begin_turn(session, turn_input())
    proc = session.client.proc
    await driver.close_session(session, time.monotonic() + 5)
    assert proc.returncode is not None


async def test_cleanup_failure_is_not_reported_as_success(monkeypatch):
    driver = candidate(monkeypatch)
    close = native_v2.ACPClient.close
    profile_closes = []
    close_profile = native_v2.Profile.close

    async def failing_close(client):
        raise OSError("private detail must stay internal")

    def track_profile_close(profile):
        profile_closes.append(profile)
        close_profile(profile)

    monkeypatch.setattr(native_v2.ACPClient, "close", failing_close)
    monkeypatch.setattr(native_v2.Profile, "close", track_profile_close)
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    assert [event async for event in driver.events(turn)][-1][
        "type"
    ] == "native_turn_ended"
    proc = session.client.proc
    directory = session.isolated.workspace.parent
    try:
        with pytest.raises(GatewayError, match="cleanup_failed") as error:
            await driver.close_session(session, time.monotonic() + 5)
        assert "private detail" not in str(error.value)
        assert proc.returncode is None and directory.exists()
        assert not profile_closes
        # A dropped session must not trigger temporary-profile auto-cleanup.
        session.isolated = None
        assert directory.exists()
    finally:
        await close(session.client)
        shutil.rmtree(directory, ignore_errors=True)


async def test_external_tool_result_rejected_without_touching_active_peer(monkeypatch):
    driver = candidate(monkeypatch, "hold")
    session = await driver.open_session(context(), profile())
    turn = await driver.begin_turn(session, turn_input())
    stream = driver.events(turn)
    assert (await asyncio.wait_for(anext(stream), 5))["type"] == "text_delta"
    result = {
        "handoff_id": "handoff-1",
        "request_id": "req-2",
        "locator": "1" * 64,
        "content_digest": "5" * 64,
        "results": [
            {"public_id": "call-public", "native_ref": "opaque", "content": "result"}
        ],
        "round_policy": {"tool_choice": "auto", "parallel_tool_calls": False},
    }
    with pytest.raises(GatewayError, match="client_tools_unavailable"):
        await driver.submit_tool_results(turn, result)
    assert session.active is turn and not session.closed and not turn.finished
    await stream.aclose()
    await driver.cancel(turn, "client_cancelled", time.monotonic() + 5)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda request: request.update(
            tools=[
                {
                    "type": "function",
                    "function": {"name": "x", "parameters": {"type": "object"}},
                }
            ]
        ),
        lambda request: request["round_policy"].update(tool_choice="required"),
        lambda request: request["round_policy"].update(parallel_tool_calls=True),
        lambda request: request["round_policy"].update(max_completion_tokens=1),
        lambda request: request.update(reasoning_output="summary"),
        lambda request: request.update(reasoning_effort="high"),
    ],
)
async def test_unsupported_features_fail_before_spawn(monkeypatch, mutation):
    driver = candidate(monkeypatch)
    session = await driver.open_session(context(), profile())
    request = turn_input()
    mutation(request)
    with pytest.raises(GatewayError):
        await driver.begin_turn(session, request)
    assert session.client is None and session.owner is None and session.isolated is None
    await driver.close_session(session, time.monotonic() + 5)


async def test_standalone_v1_host_rejects_tools_without_spawning():
    class Pool:
        config = Config(api_key="sk-fixture-key-123456789012345")

        def turn(self, request):
            raise AssertionError("unsupported tools must not reach the backend")

    app = App(Pool())
    app.ready = True
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Bearer " + Pool.config.api_key},
    ) as client:
        response = await client.post(
            "/v1/chat/completions",
            json={
                "model": "kiro-default",
                "messages": [{"role": "user", "content": "hello"}],
                "tools": [
                    {
                        "type": "function",
                        "function": {"name": "x", "parameters": {"type": "object"}},
                    }
                ],
            },
        )
    assert response.status_code == 400


async def test_close_before_owner_first_step_settles_begin_waiter(monkeypatch):
    driver = candidate(monkeypatch)
    session = await driver.open_session(context(), profile())
    begin = asyncio.create_task(driver.begin_turn(session, turn_input()))
    await asyncio.sleep(0)
    assert session.owner is not None and not session.owner_started
    await driver.close_session(session, time.monotonic() + 5)
    with pytest.raises(GatewayError, match="backend_startup_failed"):
        await asyncio.wait_for(begin, 2)
    assert session.owner.done() and session.client is None
