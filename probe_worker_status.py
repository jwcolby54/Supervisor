"""Bounded worker_status probe for supervisor-owned MyMusic crawler workers.

The supervisor itself stays free of persistent DB/Vault state. When it needs to
check a discovery worker's durable heartbeat/progress contract, it shells out to
this helper with a hard timeout and reads one JSON line back.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from pg_music import connect_crawler  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe crawler.worker_status for one worker.")
    parser.add_argument("--part-name", required=True)
    parser.add_argument("--heartbeat-max-seconds", type=int, required=True)
    parser.add_argument("--progress-max-seconds", type=int, required=True)
    args = parser.parse_args()

    sql = """
    SELECT
        ws_pid,
        ws_host,
        ws_status,
        ws_current_phase,
        ws_rows_processed,
        ws_last_heartbeat_at,
        ws_last_progress_at,
        EXTRACT(EPOCH FROM (NOW() - ws_last_heartbeat_at)) AS heartbeat_age_seconds,
        CASE
            WHEN ws_last_progress_at IS NULL THEN NULL
            ELSE EXTRACT(EPOCH FROM (NOW() - ws_last_progress_at))
        END AS progress_age_seconds
    FROM crawler.worker_status
    WHERE ws_part_name = %s
    """

    with connect_crawler() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (args.part_name,))
            row = cur.fetchone()

    if row is None:
        payload = {
            "ok": False,
            "reason": "missing_worker_status_row",
            "part_name": args.part_name,
        }
        print(json.dumps(payload), flush=True)
        return 0

    (
        ws_pid,
        ws_host,
        ws_status,
        ws_current_phase,
        ws_rows_processed,
        ws_last_heartbeat_at,
        ws_last_progress_at,
        heartbeat_age_seconds,
        progress_age_seconds,
    ) = row

    heartbeat_age = float(heartbeat_age_seconds) if heartbeat_age_seconds is not None else None
    progress_age = float(progress_age_seconds) if progress_age_seconds is not None else None

    ok = True
    reason = "ok"
    if heartbeat_age is None or heartbeat_age > args.heartbeat_max_seconds:
        ok = False
        reason = "stale_heartbeat"
    elif ws_status == "running" and progress_age is not None and progress_age > args.progress_max_seconds:
        ok = False
        reason = "wedged_running_stale_progress"

    payload = {
        "ok": ok,
        "reason": reason,
        "part_name": args.part_name,
        "ws_pid": ws_pid,
        "ws_host": ws_host,
        "ws_status": ws_status,
        "ws_current_phase": ws_current_phase,
        "ws_rows_processed": int(ws_rows_processed or 0),
        "heartbeat_age_seconds": heartbeat_age,
        "progress_age_seconds": progress_age,
        "ws_last_heartbeat_at": ws_last_heartbeat_at.isoformat() if ws_last_heartbeat_at else None,
        "ws_last_progress_at": ws_last_progress_at.isoformat() if ws_last_progress_at else None,
    }
    print(json.dumps(payload), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
