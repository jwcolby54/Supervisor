"""Elevated one-shot: prove nssm restarts the supervisor if it dies.

Kills the running supervisor process and polls for a new one with a different
pid, which only appears if the nssm service relaunched it. Writes the result to
logs/test_restart.log. Run elevated (a LocalSystem process cannot be killed, or
even have its command line read, without elevation).
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

LOGDIR = r"E:\DevPython\DataSourceQueue\Supervisor\logs"
_lines: list[str] = []


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line)
    _lines.append(line)


def find_supervisor() -> int | None:
    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -match 'supervisor\\.py' -and $_.Name -eq 'python.exe' } | "
        "Select-Object -First 1 -ExpandProperty ProcessId"
    )
    r = subprocess.run(["powershell.exe", "-NoProfile", "-Command", ps], capture_output=True, text=True)
    out = (r.stdout or "").strip()
    return int(out) if out.isdigit() else None


def main() -> None:
    before = find_supervisor()
    log(f"supervisor BEFORE kill: {before}")
    if before:
        subprocess.run(["taskkill", "/PID", str(before), "/F", "/T"], capture_output=True, text=True)
        log("killed supervisor; waiting for nssm to relaunch it...")

    after = None
    for _ in range(15):
        time.sleep(2)
        after = find_supervisor()
        if after and after != before:
            break

    log(f"supervisor AFTER: {after}")
    log("RESULT: RESTARTED by nssm" if (after and after != before) else "RESULT: NO RESTART detected")
    Path(LOGDIR, "test_restart.log").write_text("\n".join(_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
