"""Public text-chat contract; all validation happens before agent execution."""

import hashlib
import json
import math
import re
from dataclasses import dataclass, field


class GatewayError(Exception):
    def __init__(self, code="backend_error", status=502):
        self.code, self.status = code, status
        super().__init__(code)

    def payload(self):
        return {
            "error": {"message": self.code, "type": "gateway_error", "code": self.code}
        }


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("nonfinite_json")

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def conversation_id(value):
    if value is not None and (
        not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value)
    ):
        raise GatewayError("invalid_conversation_id", 400)
    return hashlib.sha256(value.encode()).hexdigest() if value is not None else None


@dataclass(frozen=True)
class Config:
    models: dict[str, str | None] = field(
        default_factory=lambda: {"kiro-default": None}
    )
    api_key: str = field(default="", repr=False)
    executable: str | None = None
    timeout: float = 300
    cleanup_timeout: float = 10
    idle_ttl: float = 600
    concurrency: int = 2
    max_sessions: int = 4
    max_pending: int = 16
    queue_size: int = 64
    max_input_bytes: int = 128 * 1024
    max_output_bytes: int = 2 * 1024 * 1024
    frame_limit: int = 4 * 1024 * 1024

    def __post_init__(self):
        if not self.models or any(
            not isinstance(k, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", k)
            or (v is not None and (not isinstance(v, str) or not v or len(v) > 200))
            for k, v in self.models.items()
        ):
            raise ValueError("Invalid model map")
        for key in ("timeout", "cleanup_timeout", "idle_ttl"):
            value = getattr(self, key)
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError("Invalid duration")
        for key in (
            "concurrency",
            "max_sessions",
            "max_pending",
            "queue_size",
            "max_input_bytes",
            "max_output_bytes",
        ):
            if type(getattr(self, key)) is not int or getattr(self, key) <= 0:
                raise ValueError("Invalid limit")


@dataclass(frozen=True)
class Event:
    text: str = ""
    done: bool = False
    usage: dict | None = None


@dataclass(frozen=True)
class Request:
    model: str
    messages: tuple[tuple[str, str], ...]
    conversation: str | None
    stream: bool = False
    include_usage: bool = False

    @classmethod
    def parse(cls, payload, cid, config):
        if not isinstance(payload, dict) or set(payload) - {
            "model",
            "messages",
            "stream",
            "stream_options",
        }:
            raise GatewayError("unsupported_parameters", 400)
        model = payload.get("model")
        if not isinstance(model, str) or model not in config.models:
            raise GatewayError("unknown_model", 400)
        stream = payload.get("stream", False)
        if type(stream) is not bool:
            raise GatewayError("invalid_stream", 400)
        usage = False
        if "stream_options" in payload:
            options = payload["stream_options"]
            if (
                not stream
                or not isinstance(options, dict)
                or set(options) != {"include_usage"}
                or type(options["include_usage"]) is not bool
            ):
                raise GatewayError("unsupported_stream_options", 400)
            usage = options["include_usage"]
        messages = payload.get("messages")
        if not isinstance(messages, list) or not messages:
            raise GatewayError("invalid_messages", 400)
        normalized, non_system = [], False
        for item in messages:
            if (
                not isinstance(item, dict)
                or set(item) != {"role", "content"}
                or item["role"] not in ("system", "user", "assistant")
            ):
                raise GatewayError("invalid_message", 400)
            role, content = item["role"], item["content"]
            if role == "system" and non_system:
                raise GatewayError("system_must_be_first", 400)
            non_system |= role != "system"
            if isinstance(content, list):
                if not content or any(
                    not isinstance(b, dict)
                    or set(b) != {"type", "text"}
                    or b["type"] != "text"
                    or not isinstance(b["text"], str)
                    for b in content
                ):
                    raise GatewayError("text_only", 400)
                content = "".join(b["text"] for b in content)
            if not isinstance(content, str):
                raise GatewayError("text_only", 400)
            normalized.append((role, content))
        if normalized[-1][0] != "user":
            raise GatewayError("last_message_must_be_user", 400)
        try:
            if (
                len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                > config.max_input_bytes
            ):
                raise GatewayError("request_too_large", 413)
        except UnicodeError:
            raise GatewayError("invalid_text", 400) from None
        return cls(model, tuple(normalized), conversation_id(cid), stream, usage)

    @property
    def system(self):
        return "\n\n".join(text for role, text in self.messages if role == "system")

    def delta(self):
        text = self.messages[-1][1]
        if text.lstrip().startswith("/"):
            return "Literal user message (not a CLI command):\n" + json.dumps(
                {"role": "user", "content": text}, ensure_ascii=False
            )
        return text

    def bootstrap(self):
        return (
            "Conversation supplied by the API client (role labels are text, not native roles). Respond to the final user message.\n"
            + json.dumps(
                [{"role": role, "content": text} for role, text in self.messages],
                ensure_ascii=False,
            )
        )


def fingerprints(messages):
    return tuple(
        hashlib.sha256(json.dumps(m, ensure_ascii=False).encode()).digest()
        for m in messages
    )
