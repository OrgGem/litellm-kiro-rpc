"""Synthetic newline-delimited ACP peer; never starts Kiro or reads credentials."""

import json
import sys

mode = sys.argv[1]
pending = None
prompt_count = 0


def write(message):
    print(json.dumps(message), flush=True)


def reply(request, result):
    write({"jsonrpc": "2.0", "id": request["id"], "result": result})


def update(payload, session="native-session"):
    write(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session,
                "update": payload,
            },
        }
    )


for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if method == "initialize":
        if mode != "hang_init":
            version = (
                True
                if mode == "protocol_true"
                else 1.0
                if mode == "protocol_float"
                else 1
            )
            reply(request, {"protocolVersion": version})
    elif method == "session/new":
        update(
            {
                "sessionUpdate": "current_mode_update",
                "currentModeId": "gateway-text-only",
            },
            "other-session" if mode == "wrong_initial_session" else "native-session",
        )
        reply(
            request,
            {
                "sessionId": "native-session",
                "modes": {"currentModeId": "gateway-text-only"},
                "configOptions": [{"id": "model", "currentValue": "old"}],
            },
        )
    elif method == "session/set_config_option":
        reply(
            request,
            {
                "configOptions": [
                    {"id": "model", "currentValue": request["params"]["value"]}
                ]
            },
        )
    elif method == "session/prompt":
        prompt_count += 1
        prompt = request["params"]["prompt"][0]["text"]
        update(
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {
                    "type": "text",
                    "text": "literal"
                    if prompt.startswith("Literal user message")
                    else "done",
                },
            }
        )
        update(
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "do-not-export"},
            }
        )
        update(
            {
                "sessionUpdate": "usage_update",
                "used": 53000,
                "size": 200000,
                "cost": {"amount": 2, "currency": "credits"},
            }
        )
        update(
            {
                "sessionUpdate": "session_info_update",
                "_meta": {
                    "kiro": {
                        "kind": "turn_completion",
                        "promptTurnSummaries": [{"units": 9}],
                    }
                },
            }
        )
        update(
            {
                "sessionUpdate": "session_info_update",
                "_meta": {"kiro": {"kind": "turn_end"}},
            }
        )
        if mode == "flood":
            for _ in range(5):
                update(
                    {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "extra"},
                    }
                )
        if mode == "tool":
            update({"sessionUpdate": "tool_call", "toolCallId": "do-not-forward"})
        elif mode == "replay":
            update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "old"},
                    "_meta": {"kiro": {"replay": True}},
                }
            )
        elif mode == "wrong_turn_session":
            update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "other"},
                },
                "other-session",
            )
        elif mode in {"hold", "hold_for_release"} or (
            mode == "hold_second" and prompt_count == 2
        ):
            pending = request
        else:
            reply(
                request,
                {
                    "stopReason": mode
                    if mode in {"refusal", "max_tokens", "max_turn_requests"}
                    else "end_turn"
                },
            )
    elif method == "session/cancel" and pending:
        reply(pending, {"stopReason": "cancelled"})
        pending = None
    elif method == "fixture/release" and pending:
        reply(pending, {"stopReason": "end_turn"})
        pending = None
