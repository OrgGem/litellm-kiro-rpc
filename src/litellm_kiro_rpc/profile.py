"""Candidate isolated V3 profile; production gate stays closed until verified.

KIRO_HOME redirects configuration AND some sign-in state. Retaining the user's
official CLI login without copying credentials must be proved on Windows before
this profile can be released. Empty tools alone does not suppress global hooks.
"""

import json
import os
import tempfile
from pathlib import Path


class Profile:
    def __init__(self, *, temporary_directory=None, isolate_environment=False):
        self.temp = (
            temporary_directory
            if temporary_directory is not None
            else tempfile.TemporaryDirectory(prefix="kiro-rpc-")
        )
        root = Path(self.temp.name)
        self.workspace = root / "workspace"
        self.workspace.mkdir()
        (self.workspace / ".git").mkdir()
        home = root / "kiro-home"
        (home / "agents").mkdir(parents=True)
        (home / "settings").mkdir()
        (home / "agents/gateway-text-only.json").write_text(
            json.dumps(
                {
                    "name": "gateway-text-only",
                    "description": "Text-only API session",
                    "tools": [],
                    "resources": [],
                    "hooks": [],
                    "mcpServers": {},
                    "includeMcpJson": False,
                }
            ),
            encoding="utf-8",
        )
        (home / "settings/cli.json").write_text(
            json.dumps({"chat.disableInheritingDefaultResources": True}),
            encoding="utf-8",
        )
        allowed = {
            "PATH",
            "SYSTEMROOT",
            "WINDIR",
            "COMSPEC",
            "PATHEXT",
            "TEMP",
            "TMP",
            "HOME",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "PROGRAMFILES",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
        }
        self.env = {k: v for k, v in os.environ.items() if k.upper() in allowed}
        if isolate_environment:
            for key in (
                "HOME",
                "USERPROFILE",
                "APPDATA",
                "LOCALAPPDATA",
                "TEMP",
                "TMP",
            ):
                folder = root / key.lower()
                folder.mkdir()
                self.env[key] = str(folder)
        self.env.update(KIRO_HOME=str(home), NO_COLOR="1", KIRO_LOG_NO_COLOR="1")

    def close(self):
        self.temp.cleanup()
