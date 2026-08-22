"""Bounded SysVar-flag probe for supervisor-owned MyMusic crawler workers.

The supervisor itself stays free of persistent DB/Vault state. When it needs to
check a discovery worker's durable heartbeat/progress contract, it shells out to
this helper with a hard timeout and reads JSON back.

Source of truth is the crawler SysVar status flags (`CR_<Part>_Sta_*`), written
every loop pass by each worker's shared StatusEmitter -- NOT the retired
crawler.worker_status table.

Two modes:

- SINGLE (``--part-name NAME``): read one worker's status, print one JSON object.
  Used by the supervisor startup path (one part at a time).

- BATCH (``--specs-json '[...]'``): read MANY workers' status over ONE database
  connection and print ``{worker_name: payload, ...}``. The steady-state
  supervision loop uses this so the whole crawler fleet's status costs a single
  subprocess + a single short-lived connection per loop, instead of one
  subprocess + connection per worker per loop. The supervisor still holds no
  persistent DB connection -- this process opens one, reads everything, and exits.

Each spec in ``--specs-json`` is ``{"part_name","heartbeat_max_seconds",
"progress_max_seconds"}``. Both modes share the exact same evaluation logic.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from supervisor_shared_logging import build_runtime_logger


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


APP_LOGGER = build_runtime_logger(source="supervisor/probe_worker_status.py")
APP_LOGGER.install_unhandled_exception_hook()


def _evaluate_status(status: dict | None, part_name: str, part: str,
                     heartbeat_max_seconds: int, progress_max_seconds: int) -> dict:
    """Turn one worker's raw SysVar status into the supervisor probe payload.

    The ok/reason contract is identical for single and batch mode:
      - stale (or missing) heartbeat  -> not ok, 'stale_heartbeat'
      - status 'healthy' but progress older than the max -> not ok, 'wedged...'
        (an IDLE lane is intentionally exempt: no work is not a wedge)
    """
    if status is None:
        return {"ok": False, "reason": "missing_status_flags", "part_name": part_name, "part": part}

    heartbeat_age = status.get("heartbeat_age_seconds")
    progress_age = status.get("progress_age_seconds")
    heartbeat_age = float(heartbeat_age) if heartbeat_age is not None else None
    progress_age = float(progress_age) if progress_age is not None else None
    ws_status = status.get("status")

    ok = True
    reason = "ok"
    if heartbeat_age is None or heartbeat_age > heartbeat_max_seconds:
        ok = False
        reason = "stale_heartbeat"
    elif ws_status == "healthy" and progress_age is not None and progress_age > progress_max_seconds:
        ok = False
        reason = "wedged_running_stale_progress"

    return {
        "ok": ok,
        "reason": reason,
        "part_name": part_name,
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


def _read_one(flags: SysVarFlags, part_name: str, heartbeat_max_seconds: int,
              progress_max_seconds: int) -> dict:
    """Read + evaluate one worker on an already-open SysVarFlags connection."""
    part = WORKER_PART_CODES.get(part_name)
    if part is None:
        return {"ok": False, "reason": "unknown_worker", "part_name": part_name}
    status = flags.read_status(part)
    return _evaluate_status(status, part_name, part, heartbeat_max_seconds, progress_max_seconds)


def _run_single(part_name: str, heartbeat_max_seconds: int, progress_max_seconds: int) -> int:
    part = WORKER_PART_CODES.get(part_name)
    if part is None:
        print(json.dumps({"ok": False, "reason": "unknown_worker", "part_name": part_name}), flush=True)
        return 0
    try:
        db = build_db_client(project_label="Supervisor worker-status probe")
        db.connect()
        try:
            payload = _read_one(SysVarFlags("CR", db=db), part_name, heartbeat_max_seconds, progress_max_seconds)
        finally:
            db.close()
    except Exception as exc:  # noqa: BLE001 - bounded probe must surface failure as JSON
        APP_LOGGER.log_handled_exception(
            exc, event_type="supervisor_worker_probe_failed",
            context={"part_name": part_name, "part": part},
        )
        print(json.dumps({"ok": False, "reason": "probe_exception", "part_name": part_name, "part": part}), flush=True)
        return 0
    print(json.dumps(payload), flush=True)
    return 0


def _run_batch(specs: list[dict]) -> int:
    """Read every requested worker's status over ONE connection.

    Prints ``{part_name: payload, ...}``. A whole-batch DB failure surfaces as a
    single ``{"__batch_error__": reason}`` object so the caller can treat every
    part as inconclusive (and, per the supervisor's debounce, hold rather than
    tear the fleet down on one transient blip).
    """
    try:
        db = build_db_client(project_label="Supervisor worker-status batch probe")
        db.connect()
        try:
            flags = SysVarFlags("CR", db=db)
            results = {
                spec["part_name"]: _read_one(
                    flags,
                    spec["part_name"],
                    int(spec["heartbeat_max_seconds"]),
                    int(spec["progress_max_seconds"]),
                )
                for spec in specs
            }
        finally:
            db.close()
    except Exception as exc:  # noqa: BLE001 - one JSON error object for the whole batch
        APP_LOGGER.log_handled_exception(exc, event_type="supervisor_worker_batch_probe_failed")
        print(json.dumps({"__batch_error__": f"{type(exc).__name__}: {exc}"}), flush=True)
        return 0
    print(json.dumps(results), flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe crawler SysVar status flags for one or many workers.")
    parser.add_argument("--part-name", help="Single mode: worker name, e.g. MT_song_hydrator_identity")
    parser.add_argument("--heartbeat-max-seconds", type=int, help="Single mode heartbeat staleness ceiling")
    parser.add_argument("--progress-max-seconds", type=int, help="Single mode progress staleness ceiling")
    parser.add_argument("--specs-json", help="Batch mode: JSON list of {part_name,heartbeat_max_seconds,progress_max_seconds}")
    args = parser.parse_args()

    if args.specs_json is not None:
        try:
            specs = json.loads(args.specs_json)
            if not isinstance(specs, list):
                raise ValueError("specs-json must be a JSON list")
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"__batch_error__": f"bad specs-json: {exc}"}), flush=True)
            return 0
        return _run_batch(specs)

    if not args.part_name or args.heartbeat_max_seconds is None or args.progress_max_seconds is None:
        parser.error("single mode requires --part-name, --heartbeat-max-seconds, and --progress-max-seconds")
    return _run_single(args.part_name, args.heartbeat_max_seconds, args.progress_max_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
