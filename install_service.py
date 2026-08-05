"""One-time installer: register the MusicApp Supervisor as a Windows service via nssm.

MUST be run ELEVATED (Administrator). It is still run as the interactive user
(jwcolby), so it can read the read-class Vault token from that user's keyring and
inject it into the LocalSystem service's environment. LocalSystem cannot read the
per-user keyring itself, which is exactly why the token is copied in here once.

Decision + costs are documented in the MyMusic wiki:
    MusicApp_Wiki/wiki/docs/supervisor_service_deployment.md

Re-runnable: it stops and removes any existing definition first.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import keyring

NSSM = r"E:\DevPython\DataSourceQueue\Supervisor\tools\nssm.exe"
SERVICE = "MusicAppSupervisor"
PYTHON = r"C:\Users\jwcol\AppData\Local\Programs\Python\Python310\python.exe"
SCRIPT = r"E:\DevPython\DataSourceQueue\Supervisor\supervisor.py"
APPDIR = r"E:\DevPython\DataSourceQueue\Supervisor"
LOGDIR = r"E:\DevPython\DataSourceQueue\Supervisor\logs"
VAULT_ADDR = "http://127.0.0.1:18200"

TOKEN_KEYRING_SERVICE = "shared-vault-operator-secrets"
TOKEN_KEYRING_KEY = "SA_SECRET_VAULT_DEV_MIGRATION_TOKEN"

_log_lines: list[str] = []


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}"
    print(line)
    _log_lines.append(line)


def flush_log() -> None:
    Path(LOGDIR).mkdir(parents=True, exist_ok=True)
    Path(LOGDIR, "install_service.log").write_text("\n".join(_log_lines) + "\n", encoding="utf-8")


def get_token() -> str:
    raw = keyring.get_password(TOKEN_KEYRING_SERVICE, TOKEN_KEYRING_KEY)
    if not raw:
        log("FATAL: Vault token not found in keyring")
        flush_log()
        sys.exit(2)
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and "value" in obj:
            return obj["value"]
    except Exception:
        pass
    return raw


def nssm(*args: str, secret: bool = False) -> int:
    result = subprocess.run([NSSM, *args], capture_output=True, text=True)
    shown = list(args)
    if secret:
        shown = list(args[:2]) + ["<secret>"]
    out = (result.stdout or "").strip()
    err = (result.stderr or "").strip()
    log(f"nssm {' '.join(shown)} -> rc={result.returncode} {out} {err}".rstrip())
    return result.returncode


def main() -> None:
    log(f"installing service '{SERVICE}' (LocalSystem, auto-start)")
    token = get_token()

    # Idempotent: clear any prior definition first.
    subprocess.run([NSSM, "stop", SERVICE], capture_output=True, text=True)
    subprocess.run([NSSM, "remove", SERVICE, "confirm"], capture_output=True, text=True)

    nssm("install", SERVICE, PYTHON, SCRIPT)
    nssm("set", SERVICE, "AppDirectory", APPDIR)
    nssm("set", SERVICE, "DisplayName", "MusicApp Supervisor")
    nssm(
        "set", SERVICE, "Description",
        "Keeps the MBQueue/FMQueue fleet (and future discovery workers) alive. "
        "Design: MusicDiscoveryArchitecture DF sections 24-27.",
    )
    nssm("set", SERVICE, "Start", "SERVICE_AUTO_START")
    nssm("set", SERVICE, "ObjectName", "LocalSystem")
    nssm("set", SERVICE, "AppStdout", LOGDIR + r"\service_stdout.log")
    nssm("set", SERVICE, "AppStderr", LOGDIR + r"\service_stderr.log")
    nssm("set", SERVICE, "AppRotateFiles", "1")
    # Do not hot-loop a genuinely broken supervisor: throttle + delayed restart.
    nssm("set", SERVICE, "AppThrottle", "5000")
    nssm("set", SERVICE, "AppExit", "Default", "Restart")
    nssm("set", SERVICE, "AppRestartDelay", "3000")
    # The bootstrap secret + Vault address for the headless LocalSystem service.
    nssm("set", SERVICE, "AppEnvironmentExtra", f"VAULT_TOKEN={token}", f"VAULT_ADDR={VAULT_ADDR}", secret=True)

    rc = nssm("start", SERVICE)
    log(f"start rc={rc}")
    log("INSTALL DONE")
    flush_log()


if __name__ == "__main__":
    main()
