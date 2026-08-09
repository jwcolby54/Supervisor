"""Bounded SysVar-flag probe for supervisor-owned MyMusic crawler workers.

The supervisor itself stays free of persistent DB/Vault state. When it needs to
check a discovery worker's durable heartbeat/progress contract, it shells out to
this helper with a hard timeout and reads one JSON line back.

Source of truth is the crawler SysVar status flags (`CR_<Part>_Sta_*`), written
every loop pass by each worker's shared StatusEmitter -- NOT the retired
crawler.worker_status table. The caller still passes a worker NAME via
--part-name; this maps it to its standard Part code (crawler_sysvars.
WORKER_PART_CODES) and reads that part's live status.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


def _iso(value):
    """Coerce a datetime (as SysVarStore deserializes them) to an ISO string."""
    return value.isoformat() if isinstance(value, datetime) else value


ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from crawler_sysvars import WORKER_PART_CODES  # noqa: E402
from jwc_pylib.sysvar_flags import SysVarFlags  # noqa: E402
from mymusic_vault import build_db_client  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe crawler SysVar status flags for one worker.")
    parser.add_argument("--part-name", required=True, help="Worker name, e.g. MT_song_hydrator_submit")
    parser.add_argument("--heartbeat-max-seconds", type=int, required=True)
    parser.add_argument("--progress-max-seconds", type=int, required=True)
    args = parser.parse_args()

    part = WORKER_PART_CODES.get(args.part_name)
    if part is None:
        print(
            json.dumps({"ok": False, "reason": "unknown_worker", "part_name": args.part_name}),
            flush=True,
        )
        return 0

    db = build_db_client(project_label="Supervisor worker-status probe")
    db.connect()
    try:
        status = SysVarFlags("CR", db=db).read_status(part)
    finally:
        db.close()

    if status is None:
        print(
            json.dumps({"ok": False, "reason": "missing_status_flags", "part_name": args.part_name}),
            flush=True,
        )
        return 0

    heartbeat_age = status.get("heartbeat_age_seconds")
    progress_age = status.get("progress_age_seconds")
    heartbeat_age = float(heartbeat_age) if heartbeat_age is not None else None
    progress_age = float(progress_age) if progress_age is not None else None
    ws_status = status.get("status")

    ok = True
    reason = "ok"
    if heartbeat_age is None or heartbeat_age > args.heartbeat_max_seconds:
        ok = False
        reason = "stale_heartbeat"
    elif ws_status == "healthy" and progress_age is not None and progress_age > args.progress_max_seconds:
        ok = False
        reason = "wedged_running_stale_progress"

    payload = {
        "ok": ok,
        "reason": reason,
        "part_name": args.part_name,
        "part": part,
        "ws_pid": status.get("pid"),
        "ws_host": status.get("host"),
        "ws_status": ws_status,
        "ws_current_phase": status.get("phase"),
        "ws_rows_processed": int(status.get("rows_processed") or 0),
        "heartbeat_age_seconds": heartbeat_age,
        "progress_age_seconds": progress_age,
        "ws_last_heartbeat_at": _iso(status.get("heartbeat_at")),
        "ws_last_progress_at": _iso(status.get("last_progress_at")),
    }
    print(json.dumps(payload), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
