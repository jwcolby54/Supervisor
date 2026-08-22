"""Post-reboot verification of the whole aliveness chain.

Reports, top to bottom:
  OS -> nssm service -> supervisor -> queues, plus the infra the queues need
  (Vault reachable + unsealed). Distinguishes "service/supervisor up" (nssm's
  job) from "fleet healthy" (needs Docker PG/Vault up and Vault unsealed), so a
  reboot result is easy to read even if infra is slow or Vault is sealed.
"""

from __future__ import annotations

import json
import subprocess
import urllib.request
from pathlib import Path

from supervisor_shared_logging import build_runtime_logger


SUPERVISOR_DIR = Path(__file__).resolve().parent
MBQUEUE_DIR = SUPERVISOR_DIR.parent / "MBQueue"
FMQUEUE_DIR = SUPERVISOR_DIR.parent / "FMQueue"
PYTHON = "python"

APP_LOGGER = build_runtime_logger(source="supervisor/verify_boot.py")
APP_LOGGER.install_unhandled_exception_hook()


def http_json(url: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def runtime_probe(queue_dir: Path, module: str, component: str) -> dict[str, object]:
    completed = subprocess.run(
        [
            PYTHON,
            "-m",
            module,
            "--component",
            component,
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        cwd=str(queue_dir),
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"{module} rc={completed.returncode}: {detail}")
    try:
        return json.loads((completed.stdout or "").strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{module} produced invalid JSON") from exc


def main() -> None:
    print("=== MusicApp boot verification ===\n")

    # 1. Service
    r = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", "(Get-Service MusicAppSupervisor).Status"],
        capture_output=True, text=True,
    )
    print(f"[service ] MusicAppSupervisor: {(r.stdout or r.stderr).strip()}")

    # 2. Vault reachable + unsealed
    status, body = http_json("http://127.0.0.1:18200/v1/sys/health")
    if isinstance(body, dict):
        print(f"[vault   ] reachable, sealed={body.get('sealed')} initialized={body.get('initialized')}")
    else:
        print(f"[vault   ] NOT reachable ({body})")

    # 3. Queue APIs
    for name, url in (("MBQueue", "http://127.0.0.1:18765/health"),
                      ("FMQueue", "http://127.0.0.1:18766/health")):
        status, body = http_json(url)
        ok = isinstance(body, dict) and body.get("status") == "ok"
        print(f"[api     ] {name}: {'OK' if ok else 'DOWN'} ({body if not ok else body.get('run_id')})")

    # 4. Queue worker runtime probes
    try:
        mb_worker = runtime_probe(MBQUEUE_DIR, "mbqueue.runtime_probe", "Worker")
        fm_worker = runtime_probe(FMQUEUE_DIR, "fmqueue.runtime_probe", "Worker")
        print(
            f"[workers ] MBQueue worker: {'OK' if mb_worker.get('ok') else 'DOWN'} "
            f"({mb_worker.get('reason')}, status={mb_worker.get('status')})"
        )
        print(
            f"[workers ] FMQueue worker: {'OK' if fm_worker.get('ok') else 'DOWN'} "
            f"({fm_worker.get('reason')}, status={fm_worker.get('status')})"
        )
    except Exception as exc:  # noqa: BLE001
        APP_LOGGER.log_handled_exception(exc, event_type="verify_boot_worker_probe_failed")
        print(f"[workers ] could not check queue worker runtime probes ({exc})")

    print("\nReading: service Running + both APIs OK + both queue workers OK == full chain up.")
    print("If service Running but APIs DOWN, infra (Docker/Vault) is not ready yet or Vault is sealed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        APP_LOGGER.log_handled_exception(exc, event_type="verify_boot_failed")
        raise
