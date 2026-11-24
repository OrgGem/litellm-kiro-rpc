"""Gated ACP V3 candidate; no client tools without a verified result channel.

The host owns authentication, public HTTP/session admission and leases. This
driver owns the ACP peer, native turn and process tree, all on one event loop.
"""

import asyncio
import contextlib
import hashlib
import json
import math
import re
import secrets
import shutil
import tempfile
import time
from importlib import resources

from jsonschema import Draft202012Validator

from .contracts import Config, GatewayError
from .profile import Profile
from .transport import ACPClient

CONTRACT_VERSION = "0.1"
CONTRACT_HASH = "749182d456eb363be2d63b2a98d66d0f1bf203b93d8a16ed4a8bdd90834e5fc1"
_END = object()


class _RetainedDirectory:
    """Explicitly owned: garbage collection must not remove a live profile."""

    def __init__(self, prefix):
        self.name = tempfile.mkdtemp(prefix=prefix)

    def cleanup(self):
        shutil.rmtree(self.name)


def _contract():
    directory = resources.files("litellm_kiro_rpc").joinpath("contract_v0_1")
    try:
        manifest = json.loads(directory.joinpath("manifest.json").read_bytes())
        files = manifest["files"]
        canonical = (json.dumps(files, sort_keys=True, indent=2) + "\n").encode()
        if (
            manifest["contract_version"] != CONTRACT_VERSION
            or manifest["status"] != "frozen"
            or manifest["contract_hash"] != CONTRACT_HASH
            or set(files) != {"schema.json", "fixtures.json", "SEMANTICS.md"}
            or hashlib.sha256(canonical).hexdigest() != CONTRACT_HASH
            or any(
                hashlib.sha256(directory.joinpath(name).read_bytes()).hexdigest()
                != digest
                for name, digest in files.items()
            )
        ):
            raise ValueError
        return json.loads(directory.joinpath("schema.json").read_bytes())
    except (OSError, KeyError, TypeError, ValueError):
        raise GatewayError("contract_mismatch", 503) from None


def _validate(schema, dto, value):
    if not Draft202012Validator(schema).is_valid({"dto": dto, "value": value}):
        raise GatewayError("invalid_" + dto.lower(), 400)


def _encoded(value):
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise GatewayError("invalid_json", 400) from None


def _message_prefix(messages):
    if not messages or messages[-1].get("role") != "user":
        raise GatewayError("unsupported_history", 400)
    if any(m.get("role") == "tool" or m.get("tool_calls") for m in messages):
        raise GatewayError("tool_history_unavailable", 410)
    if any(m.get("role") == "assistant" and m.get("content") is None for m in messages):
        raise GatewayError("unsupported_history", 400)
    non_system = False
    for message in messages:
        if message["role"] == "system" and non_system:
            raise GatewayError("unsupported_history", 400)
        non_system |= message["role"] != "system"
    return json.loads(_encoded(messages))


def _capability(reason):
    return {
        "state": "blocked",
        "evidence_level": "source",
        "live_verified": False,
        "evidence": ["kiro_cli_3.1.0_source"],
        "reason": reason,
    }


class _Turn:
    def __init__(self, session, data, prompt):
        self.session, self.data, self.prompt = session, data, prompt
        self.invocation_id = "inv-" + secrets.token_hex(16)
        self.epoch = "epoch-" + secrets.token_hex(16)
        self.queue = asyncio.Queue(maxsize=session.config.queue_size)
        self.sequence = 0
        self.ready = asyncio.get_running_loop().create_future()
        self.ready.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        self.consumer = False
        self.finished = False
        self.cancelled = False
        self.answer = []
        self.output_bytes = 0
        self.error_event = None
        self.error_reported = False

    def emit(self, kind, payload):
        if self.finished or self.cancelled:
            raise GatewayError("turn_closed", 409)
        event = {
            "type": kind,
            "invocation_id": self.invocation_id,
            "sequence": self.sequence,
            "payload": payload,
        }
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            raise GatewayError("backpressure_exceeded", 502) from None
        self.sequence += 1

    def fail(self, code, status):
        if self.finished or self.cancelled or self.error_reported:
            return
        self.error_reported = True
        event = {
            "type": "backend_error",
            "invocation_id": self.invocation_id,
            "sequence": self.sequence,
            "payload": {"code": code, "status": status},
        }
        self.sequence += 1
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # Preserve the terminal error if data consumed every bounded slot.
            self.error_event = event


class _Session:
    def __init__(self, driver, context, config, profile):
        self.driver, self.context, self.config, self.profile = (
            driver,
            context,
            config,
            profile,
        )
        self.queue = asyncio.Queue(maxsize=1)
        self.owner = self.client = self.isolated = None
        self.owner_started = False
        self.native_session = None
        self.initial_updates = []
        self.phase = "starting"
        self.active = None
        self.prompt_task = None
        self.expected = None
        self.immutable = None
        self.reusable = True
        self.closed = False
        self.cancel_requested = False
        self.loop = asyncio.get_running_loop()


class KiroNativeDriver:
    """Frozen v0.1 native seam. Public session creation remains gated."""

    def __init__(self, config=None, *, _isolate_environment=False):
        self.config = config or Config()
        self._isolate_environment = _isolate_environment

    async def capabilities(self, profile, model):
        _contract()
        if (
            not isinstance(profile, dict)
            or not isinstance(profile.get("profile_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", profile["profile_id"])
            or (model is not None and not isinstance(model, str))
        ):
            raise GatewayError("invalid_profile", 400)
        return {
            "contract_version": CONTRACT_VERSION,
            "contract_hash": CONTRACT_HASH,
            "backend": "kiro",
            "cli_version": None,
            "sdk_version": None,
            "model": model,
            "profile_id": profile["profile_id"],
            "transport": "stdio",
            "features": {
                name: _capability("kiro_profile_isolation_not_verified")
                for name in (
                    "text",
                    "client_tools",
                    "parallel_tools",
                    "reasoning_output",
                    "reasoning_control",
                    "usage",
                    "output_token_limit",
                    "profile_isolation",
                )
            },
            "tool_choices": [],
            "reasoning_efforts": [],
            "reasoning_kinds": [],
            "limitations": [
                "Kiro CLI profile/auth and v2 capability isolation require live acceptance"
            ],
        }

    async def _ensure_ready(self):
        # No config, environment variable or HTTP argument bypasses this gate.
        raise GatewayError("kiro_profile_isolation_not_verified", 503)

    async def open_session(self, context, config):
        schema = _contract()
        _validate(schema, "ExecutionContext", context)
        if (
            not isinstance(config, dict)
            or set(config) != {"profile_id", "profile_revision", "transport", "models"}
            or config["profile_id"] != context["profile_id"]
            or config["profile_revision"] != context["profile_revision"]
            or config["transport"] != "stdio"
            or not isinstance(config["models"], dict)
        ):
            raise GatewayError("invalid_profile", 400)
        await self._ensure_ready()
        return _Session(
            self,
            json.loads(_encoded(context)),
            self.config,
            json.loads(_encoded(config)),
        )

    def _check_session(self, session):
        if (
            not isinstance(session, _Session)
            or session.driver is not self
            or session.loop is not asyncio.get_running_loop()
        ):
            raise GatewayError("invalid_session", 400)
        if session.closed:
            raise GatewayError("session_closed", 410)

    async def begin_turn(self, session, turn_input):
        self._check_session(session)
        _validate(_contract(), "TurnInput", turn_input)
        if session.active is not None:
            raise GatewayError("session_busy", 409)
        if not session.reusable:
            raise GatewayError("session_not_reusable", 410)
        if turn_input["model_alias"] not in session.profile["models"] or (
            session.profile["models"][turn_input["model_alias"]]
            != turn_input["resolved_model"]
        ):
            raise GatewayError("unknown_model", 400)
        # ACP tool_call is a notification, not a client-executed tool bridge.
        if turn_input["tools"]:
            raise GatewayError("client_tools_unavailable", 400)
        if (
            turn_input["reasoning_output"] != "off"
            or turn_input["reasoning_effort"] is not None
        ):
            raise GatewayError("unsupported_reasoning", 400)
        policy = turn_input["round_policy"]
        if (
            policy.get("tool_choice") != "auto"
            or policy.get("parallel_tool_calls") is not False
            or policy.get("max_completion_tokens") is not None
        ):
            raise GatewayError("unsupported_round_policy", 400)
        messages = _message_prefix(turn_input["messages"])
        if len(_encoded(messages)) > session.config.max_input_bytes:
            raise GatewayError("request_too_large", 413)
        limits = session.context["limits"]
        if min(limits["chain_deadline"], limits["native_deadline"]) <= time.monotonic():
            raise GatewayError("deadline_exceeded", 504)
        immutable = (
            turn_input["model_alias"],
            turn_input["resolved_model"],
            turn_input["toolset_digest"],
        )
        if session.immutable is not None and immutable != session.immutable:
            raise GatewayError("session_configuration_changed", 409)
        if session.expected is None:
            prompt = (
                "Conversation supplied by the API client (role labels are text, not native roles). Respond to the final user message.\n"
                + _encoded(messages).decode("utf-8")
            )
        else:
            if (
                len(messages) != len(session.expected) + 1
                or messages[:-1] != session.expected
            ):
                raise GatewayError("history_mismatch", 409)
            prompt = messages[-1]["content"]
            if prompt.lstrip().startswith("/"):
                prompt = "Literal user message (not a CLI command):\n" + _encoded(
                    messages[-1]
                ).decode("utf-8")
        turn = _Turn(session, json.loads(_encoded(turn_input)), prompt)
        session.active, session.immutable = turn, immutable
        if session.owner is None:
            session.owner = asyncio.create_task(self._owner(session))
        session.queue.put_nowait(turn)
        try:
            await asyncio.shield(turn.ready)
        except asyncio.CancelledError:
            await self.cancel(
                turn,
                "startup_cancelled",
                time.monotonic() + session.config.cleanup_timeout,
            )
            raise
        return turn

    async def _start_peer(self, session, turn):
        directory = _RetainedDirectory(prefix="kiro-rpc-v2-")
        try:
            if self._isolate_environment:
                session.isolated = Profile(
                    temporary_directory=directory, isolate_environment=True
                )
            else:
                session.isolated = Profile(temporary_directory=directory)
        except BaseException:
            directory.cleanup()
            raise
        session.client = ACPClient(
            session.config, lambda message: self._update(session, message)
        )
        await session.client.start(
            [
                session.config.executable or "kiro-cli",
                "acp",
                "--agent-engine=v3",
                "--auth-method=cli",
            ],
            session.isolated.workspace,
            session.isolated.env,
        )
        info = await session.client.request(
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
        if type(info.get("protocolVersion")) is not int or info["protocolVersion"] != 1:
            raise GatewayError("unsupported_protocol", 503)
        result = await session.client.request(
            "session/new",
            {
                "cwd": str(session.isolated.workspace),
                "additionalDirectories": [],
                "mcpServers": [],
                "_meta": {"kiro": {"modeId": "gateway-text-only"}},
            },
        )
        native_id = result.get("sessionId")
        if not isinstance(native_id, str) or not native_id:
            raise GatewayError("invalid_session")
        if result.get("modes", {}).get("currentModeId") != "gateway-text-only":
            raise GatewayError("unsafe_backend_profile", 503)
        session.native_session = native_id
        for message in session.initial_updates:
            self._update(session, message)
        session.initial_updates.clear()
        model = turn.data["resolved_model"]
        if model:
            options = result.get("configOptions", [])
            if not isinstance(options, list) or not any(
                isinstance(o, dict) and o.get("id") == "model" for o in options
            ):
                raise GatewayError("model_selection_unavailable", 503)
            changed = await session.client.request(
                "session/set_config_option",
                {
                    "sessionId": native_id,
                    "configId": "model",
                    "value": model,
                },
            )
            options = changed.get("configOptions", [])
            if not isinstance(options, list) or not any(
                isinstance(o, dict)
                and o.get("id") == "model"
                and o.get("currentValue") == model
                for o in options
            ):
                raise GatewayError("model_selection_failed", 503)

    def _update(self, session, message):
        if not isinstance(message, dict) or message.get("method") != "session/update":
            raise GatewayError("unexpected_notification")
        params = message.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("update"), dict):
            raise GatewayError("invalid_backend_event")
        if session.native_session is None:
            if len(session.initial_updates) >= session.config.queue_size:
                raise GatewayError("backpressure_exceeded")
            session.initial_updates.append(message)
            return
        if params.get("sessionId") != session.native_session:
            raise GatewayError("unexpected_session")
        update = params["update"]
        kind = update.get("sessionUpdate")
        if kind in ("tool_call", "tool_call_update"):
            raise GatewayError("unexpected_tool_activity")
        # Thought, context-window usage and credit metering are not token usage
        # or verified exportable reasoning. Never log or emit their content.
        if kind != "agent_message_chunk":
            return
        turn = session.active
        metadata = update.get("_meta") or {}
        kiro = metadata.get("kiro") if isinstance(metadata, dict) else None
        if (
            session.phase != "running"
            or not turn
            or (isinstance(kiro, dict) and kiro.get("replay"))
        ):
            raise GatewayError("unexpected_agent_output")
        block = update.get("content")
        if (
            not isinstance(block, dict)
            or block.get("type") != "text"
            or not isinstance(block.get("text"), str)
        ):
            raise GatewayError("unsupported_backend_content")
        text = block["text"]
        try:
            turn.output_bytes += len(text.encode("utf-8"))
        except UnicodeError:
            raise GatewayError("invalid_backend_event") from None
        if turn.output_bytes > session.config.max_output_bytes:
            raise GatewayError("output_too_large")
        turn.answer.append(text)
        turn.emit("text_delta", {"text": text})

    async def _owner(self, session):
        session.owner_started = True
        try:
            while True:
                turn = await session.queue.get()
                deadline = min(
                    session.context["limits"]["chain_deadline"],
                    session.context["limits"]["native_deadline"],
                    time.monotonic() + session.context["limits"]["inference_seconds"],
                )
                async with asyncio.timeout_at(deadline):
                    if session.client is None:
                        await self._start_peer(session, turn)
                    if session.client.failure.done():
                        raise session.client.failure.result()
                    if not turn.ready.done():
                        turn.ready.set_result(None)
                    await self._run_turn(session, turn)
                session.active = None
        except asyncio.CancelledError:
            pass
        except Exception as error:
            turn = session.active
            if turn and not turn.ready.done():
                turn.ready.set_exception(
                    GatewayError("deadline_exceeded", 504)
                    if isinstance(error, TimeoutError)
                    else GatewayError("backend_startup_failed", 503)
                )
            elif turn and not turn.finished and not turn.cancelled:
                code, status = (
                    (error.code, error.status)
                    if isinstance(error, GatewayError)
                    else ("deadline_exceeded", 504)
                    if isinstance(error, TimeoutError)
                    else ("backend_error", 502)
                )
                turn.fail(code, status)
        finally:
            session.closed = True
            turn = session.active
            if turn:
                if not turn.ready.done():
                    turn.ready.set_exception(
                        GatewayError("backend_startup_failed", 503)
                    )
                turn.finished = True
                with contextlib.suppress(asyncio.QueueFull):
                    turn.queue.put_nowait(_END)
            # Preserve the restrictive profile if process shutdown fails or
            # times out; the host must keep the native lease quarantined.
            if session.client:
                await session.client.close()
            if session.isolated:
                session.isolated.close()

    async def _run_turn(self, session, turn):
        session.phase = "running"
        prompt = session.prompt_task = asyncio.create_task(
            session.client.request(
                "session/prompt",
                {
                    "sessionId": session.native_session,
                    "prompt": [{"type": "text", "text": turn.prompt}],
                },
            )
        )
        try:
            done, _ = await asyncio.wait(
                {prompt, session.client.failure}, return_when=asyncio.FIRST_COMPLETED
            )
            if session.client.failure in done:
                raise session.client.failure.result()
            result = await prompt
            if session.client.failure.done():
                raise session.client.failure.result()
            mapping = {
                "end_turn": "stop",
                "max_tokens": "length",
                "refusal": "content_filter",
            }
            reason = mapping.get(result.get("stopReason"))
            if reason is None:
                raise GatewayError("backend_turn_failed")
            session.phase = "idle"
            if reason != "stop":
                # ACP refusal may omit this turn from future context; do not reuse.
                session.reusable = False
            else:
                session.expected = [
                    *turn.data["messages"],
                    {"role": "assistant", "content": "".join(turn.answer)},
                ]
            turn.emit(
                "usage",
                {
                    "invocation_id": turn.invocation_id,
                    "step_id": None,
                    "epoch": turn.epoch,
                    "event_id": "usage-" + secrets.token_hex(16),
                    "sequence": 0,
                    "scope": "native_turn",
                    "semantics": "snapshot",
                    "coverage": None,
                    "source": "kiro_acp_v3_unknown",
                    "unit": "tokens",
                    "completeness": "unknown",
                    "final": True,
                    "input_tokens": None,
                    "output_tokens": None,
                    "cache_read_tokens": None,
                    "cache_write_tokens": None,
                    "reasoning_tokens": None,
                },
            )
            turn.emit(
                "native_turn_ended",
                {
                    "kind": "native_terminal",
                    "finish_reason": reason,
                    "usage_completeness": "unknown",
                },
            )
            turn.finished = True
            with contextlib.suppress(asyncio.QueueFull):
                turn.queue.put_nowait(_END)
        finally:
            session.phase = "closing" if not prompt.done() else session.phase
            if not prompt.done():
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(
                        min(session.config.cleanup_timeout / 2, 2)
                    ):
                        await session.client.notify(
                            "session/cancel", {"sessionId": session.native_session}
                        )
                        await asyncio.shield(prompt)
                prompt.cancel()
                await asyncio.gather(prompt, return_exceptions=True)
            else:
                # A completed prompt can be inspected without suspending after
                # native_turn_ended, so the owner releases the session first.
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await prompt
            session.prompt_task = None

    async def events(self, turn):
        if (
            not isinstance(turn, _Turn)
            or turn.session.driver is not self
            or turn.session.loop is not asyncio.get_running_loop()
        ):
            raise GatewayError("invalid_turn", 400)
        if turn.consumer:
            raise GatewayError("turn_consumer_exists", 409)
        turn.consumer = True
        try:
            while True:
                if turn.finished and turn.queue.empty():
                    if turn.error_event is not None:
                        event, turn.error_event = turn.error_event, None
                        yield event
                    return
                item = await turn.queue.get()
                if item is _END:
                    if turn.error_event is not None:
                        event, turn.error_event = turn.error_event, None
                        yield event
                    return
                yield item
        finally:
            turn.consumer = False

    async def submit_tool_results(self, turn, results):
        if (
            not isinstance(turn, _Turn)
            or turn.session.driver is not self
            or turn.session.loop is not asyncio.get_running_loop()
        ):
            raise GatewayError("invalid_turn", 400)
        _validate(_contract(), "ToolResults", results)
        raise GatewayError("client_tools_unavailable", 409)

    async def cancel(self, turn, reason, cleanup_deadline):
        if (
            not isinstance(turn, _Turn)
            or turn.session.driver is not self
            or turn.session.loop is not asyncio.get_running_loop()
        ):
            raise GatewayError("invalid_turn", 400)
        if (
            not isinstance(reason, str)
            or type(cleanup_deadline) not in (int, float)
            or not math.isfinite(cleanup_deadline)
        ):
            raise GatewayError("invalid_cancellation", 400)
        if turn.finished or turn.session.active is not turn:
            return
        turn.cancelled = True
        await self._close(turn.session, cleanup_deadline)

    async def _close(self, session, cleanup_deadline):
        already_closing = session.closed
        session.closed = True
        if (
            session.owner
            and not session.owner.done()
            and not session.cancel_requested
            and not already_closing
        ):
            session.cancel_requested = True
            session.owner.cancel()
        if session.owner:
            done, _ = await asyncio.wait(
                {session.owner}, timeout=max(0, cleanup_deadline - time.monotonic())
            )
            if not done and session.client and session.client.tree:
                with contextlib.suppress(Exception):
                    session.client.tree.kill()
            if session.active and not session.active.ready.done():
                session.active.ready.set_exception(
                    GatewayError(
                        "backend_startup_failed" if done else "cleanup_timeout", 503
                    )
                )
            if not done:
                raise GatewayError("cleanup_timeout", 503)
            outcome = (await asyncio.gather(session.owner, return_exceptions=True))[0]
            if isinstance(outcome, BaseException) and (
                not isinstance(outcome, asyncio.CancelledError) or session.owner_started
            ):
                raise GatewayError("cleanup_failed", 503) from None

    async def close_session(self, session, cleanup_deadline):
        if (
            not isinstance(session, _Session)
            or session.driver is not self
            or session.loop is not asyncio.get_running_loop()
        ):
            raise GatewayError("invalid_session", 400)
        if type(cleanup_deadline) not in (int, float) or not math.isfinite(
            cleanup_deadline
        ):
            raise GatewayError("invalid_cancellation", 400)
        await self._close(session, cleanup_deadline)
