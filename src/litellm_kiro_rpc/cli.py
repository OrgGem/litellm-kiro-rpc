"""Local service commands. Doctor never submits a prompt."""

import argparse
import asyncio
import json
import os

import uvicorn

from .app import App
from .contracts import Config, GatewayError
from .driver import KiroDriver, doctor
from .sessions import SessionPool


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("doctor", "serve"))
    parser.add_argument("--port", type=int, default=4104)
    parser.add_argument("--cli-bin", default=None)
    args = parser.parse_args()
    try:
        limits = json.loads(os.environ.get("KIRO_RPC_LIMITS", "{}"))
        allowed = {
            "timeout",
            "cleanup_timeout",
            "idle_ttl",
            "concurrency",
            "max_sessions",
            "max_pending",
            "queue_size",
            "max_input_bytes",
            "max_output_bytes",
        }
        if not isinstance(limits, dict) or set(limits) - allowed:
            raise ValueError("Invalid limits")
        config = Config(
            **limits,
            api_key=os.environ.get("LITELLM_MASTER_KEY", ""),
            executable=args.cli_bin,
            models=json.loads(
                os.environ.get("KIRO_MODEL_MAP", '{"kiro-default":null}')
            ),
        )
        if args.action == "doctor":
            print(json.dumps(asyncio.run(doctor(config))))
            return 0

        async def preflight():
            await doctor(config)

        app = App(
            SessionPool(
                config, lambda model, system: KiroDriver(config, model, system)
            ),
            preflight,
        )
        uvicorn.run(
            app, host="127.0.0.1", port=args.port, access_log=False, log_level="warning"
        )
        return 0
    except (GatewayError, ValueError, OSError) as exc:
        print(
            json.dumps(
                {
                    "status": "not_ready",
                    "reason": exc.code
                    if isinstance(exc, GatewayError)
                    else "invalid_config_or_cli",
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
