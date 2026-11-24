"""Single-invocation adapter for the answer-only gateway profile.

The production profile remains closed until its exact CLI and isolation have
been accepted. The native wire implementation is reused with an empty toolset;
no result-submission or persistent-session API is exposed here.
"""

import asyncio
import hashlib
import json
import platform
import shutil
from dataclasses import dataclass
from pathlib import Path

from .contracts import Config
from .contracts import GatewayError as DriverError
from .native_v2 import KiroNativeDriver

CONTRACT_HASH = "6bd9b3983a27890913ac9bf18a3dade5ac8cbc3cbe5d0630f70f04f564544097"

# Live receipts are reviewed, versioned source data; request/config cannot add one.
# Required fields: backend, profile_id, profile_revision, transport, model,
# platform, executable_sha256, cli_version, sdk_version, features, tool_choices,
# evidence. Features are restricted to the operations implemented by this driver.
ACCEPTED_RECEIPTS = ()


def _receipt(profile, model, backend):
    required = {
        "backend",
        "profile_id",
        "profile_revision",
        "transport",
        "model",
        "platform",
        "executable_sha256",
        "cli_version",
        "sdk_version",
        "features",
        "tool_choices",
        "evidence",
    }
    for receipt in ACCEPTED_RECEIPTS:
        if not isinstance(receipt, dict) or set(receipt) != required:
            continue
        if any(
            receipt.get(field) != profile.get(field)
            for field in ("profile_id", "profile_revision", "transport")
        ):
            continue
        if (
            receipt["backend"] != backend
            or receipt["model"] != model
            or receipt["platform"] != platform.system()
        ):
            continue
        digest = receipt["executable_sha256"]
        features = receipt["features"]
        choices = receipt["tool_choices"]
        evidence = receipt["evidence"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or not isinstance(receipt["cli_version"], str)
            or not receipt["cli_version"]
            or receipt["sdk_version"] is not None
            and not isinstance(receipt["sdk_version"], str)
            or not isinstance(features, (tuple, list))
            or len(features) != len(set(features))
            or not set(features)
            <= {"text", "profile_isolation", "client_tools", "usage"}
            or not {"text", "profile_isolation"} <= set(features)
            or not isinstance(choices, (tuple, list))
            or len(choices) != len(set(choices))
            or not set(choices) <= {"auto", "none", "required", "named"}
            or bool(choices) != ("client_tools" in features)
            or not isinstance(evidence, str)
            or not evidence
            or len(evidence) > 256
        ):
            continue
        return receipt
    return None


def _resolve_executable(profile):
    value = profile.get("executable") or "kiro-cli"
    try:
        path = shutil.which(value)
    except (TypeError, ValueError, OSError):
        path = None
    if not path:
        raise DriverError("cli_not_found", 503)
    return str(Path(path).resolve())


def _digest(value):
    data = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _file_digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@dataclass
class Invocation:
    owner: object
    driver: object
    context: dict
    profile: dict
    turn_input: dict
    attestation_profile: dict
    model: object
    manifest: dict
    session: object = None
    turn: object = None
    started: bool = False
    closed: bool = False


class AnswerOnlyDriver:
    def __init__(self):
        self._driver = KiroNativeDriver()

    async def capabilities(self, profile, model):
        manifest = await self._driver.capabilities(profile, model)
        manifest["contract_hash"] = CONTRACT_HASH
        manifest["profile_revision"] = profile.get("profile_revision", "unverified")
        manifest["platform"] = None
        manifest["executable_sha256"] = None
        for feature in manifest["features"].values():
            feature.update(
                state="blocked",
                evidence_level="source",
                live_verified=False,
                reason="answer_only_live_acceptance_pending",
            )
        manifest["tool_choices"] = []
        manifest["limitations"] = ["answer_only_live_acceptance_pending"]
        receipt = _receipt(profile, model, manifest["backend"])
        if receipt is not None:
            for field in (
                "platform",
                "executable_sha256",
                "cli_version",
                "sdk_version",
            ):
                manifest[field] = receipt[field]
            for name in receipt["features"]:
                manifest["features"][name].update(
                    state="verified",
                    evidence_level="live",
                    live_verified=True,
                    evidence=[receipt["evidence"]],
                    reason=None,
                )
            manifest["tool_choices"] = list(receipt["tool_choices"])
            manifest["limitations"] = []
        return manifest

    async def verify_live_binding(self, profile, model, manifest):
        expected = await self.capabilities(profile, model)
        receipt = _receipt(profile, model, expected["backend"])
        if receipt is None or manifest != expected:
            raise DriverError("answer_only_live_acceptance_pending", 503)
        path = _resolve_executable(profile)
        try:
            actual = await asyncio.to_thread(_file_digest, path)
        except (OSError, ValueError):
            raise DriverError("answer_only_profile_unverified", 503) from None
        if actual != receipt["executable_sha256"]:
            raise DriverError("answer_only_profile_unverified", 503)
        return True

    async def start(self, context, input):
        if not ACCEPTED_RECEIPTS:
            raise DriverError("answer_only_live_acceptance_pending", 503)
        attestation_profile = dict(input["config"])
        model = input["model"]
        manifest = await self.capabilities(attestation_profile, model)
        await self.verify_live_binding(attestation_profile, model, manifest)
        profile = dict(attestation_profile)
        profile.pop("execution_mode", None)
        profile.pop("executable", None)
        executable = _resolve_executable(attestation_profile)
        attestation_profile["executable"] = executable
        model = input["model"]
        alias = next(
            (key for key, value in profile["models"].items() if value == model), None
        )
        if alias is None or not isinstance(input.get("prompt"), str):
            raise DriverError("invalid_request", 400)
        messages = [{"role": "user", "content": input["prompt"]}]
        turn_input = {
            "messages": messages,
            "history_digest": _digest(messages),
            "toolset_digest": _digest([]),
            "model_alias": alias,
            "resolved_model": model,
            "tools": [],
            "round_policy": {"tool_choice": "auto", "parallel_tool_calls": False},
            "reasoning_output": "off",
            "reasoning_effort": None,
        }
        driver = _RestrictedDriver(
            Config(
                models=profile["models"],
                executable=executable,
                cleanup_timeout=context["limits"]["cleanup_seconds"],
            ),
            _isolate_environment=True,
        )
        driver._answer_only_verified = True
        return Invocation(
            self,
            driver,
            context,
            profile,
            turn_input,
            attestation_profile,
            model,
            manifest,
        )

    async def events(self, invocation):
        if (
            not isinstance(invocation, Invocation)
            or invocation.owner is not self
            or invocation.closed
        ):
            raise DriverError("invalid_invocation", 409)
        if invocation.started:
            raise DriverError("invocation_already_started", 409)
        invocation.started = True
        await self.verify_live_binding(
            invocation.attestation_profile, invocation.model, invocation.manifest
        )
        invocation.session = await invocation.driver.open_session(
            invocation.context, invocation.profile
        )
        invocation.turn = await invocation.driver.begin_turn(
            invocation.session, invocation.turn_input
        )
        async for event in invocation.driver.events(invocation.turn):
            if event.get("type") not in {
                "text_delta",
                "usage",
                "native_turn_ended",
                "backend_error",
            }:
                raise DriverError("unexpected_backend_activity", 502)
            yield event

    async def cancel(self, invocation, deadline):
        if not isinstance(invocation, Invocation) or invocation.owner is not self:
            raise DriverError("invalid_invocation", 409)
        if not invocation.closed and invocation.turn is not None:
            await invocation.driver.cancel(
                invocation.turn, "answer_only_cancelled", deadline
            )

    async def close(self, invocation, deadline):
        if not isinstance(invocation, Invocation) or invocation.owner is not self:
            raise DriverError("invalid_invocation", 409)
        if not invocation.closed:
            if invocation.session is not None:
                await invocation.driver.close_session(invocation.session, deadline)
            invocation.closed = True


class _RestrictedDriver(KiroNativeDriver):
    async def _ensure_ready(self):
        if getattr(self, "_answer_only_verified", False) is not True:
            raise DriverError("answer_only_live_acceptance_pending", 503)
