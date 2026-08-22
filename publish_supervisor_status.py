"""Publish the Supervisor's own `SUP_Sta_*` result-flags into crawler.sysvar.

Rule 2 of the process_io_contract says every process publishes at least a
heartbeat, a status, and a progress signal. The Supervisor -- the one process
that reads everyone else's flags -- published none of its own, so the thing
watching the fleet was itself only observable by tailing a log file.

This runs as a CHILD PROCESS, exactly like `read_supervisor_sysvars.py`, and
for the same reason: the Supervisor's whole point is to keep working when
PostgreSQL or Vault is down, so it holds no long-lived DB or Vault state in its
main loop. Publishing status outward is an ordinary DB write and must never
become something the lifecycle path can block on. A failure here is a lost
heartbeat sample, never a stalled fleet.

This is deliberately NOT the reverse channel. The Supervisor still takes no
commands from SysVars -- its control plane stays file-flag based and DB-free
(see `set_maintenance.py`). This publishes outward only.

Usage (the Supervisor calls it; the JSON payload arrives on stdin):

    echo {"status": "healthy", ...} | python publish_supervisor_status.py

To read the flags back instead of writing them -- the answer to "is the
supervisor alive?" without an ad-hoc script:

    python publish_supervisor_status.py --read
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from supervisor_shared_logging import build_runtime_logger

ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from jwc_pylib.sysvar_flags import SysVarFlags  # noqa: E402
from mymusic_vault import build_db_client  # noqa: E402

APP_LOGGER = build_runtime_logger(source="supervisor/publish_supervisor_status.py")
APP_LOGGER.install_unhandled_exception_hook()

UPDATED_BY = "supervisor"

# Fields this helper is allowed to publish. Anything else is a caller bug, and
# the shared validator would reject the whole write -- which, as the crawler
# workers learned the hard way, means publishing nothing at all rather than
# publishing something imperfect.
ALLOWED_FIELDS = frozenset({
    "status",
    "phase",
    "reason",
    "last_heartbeat_at",
    "last_progress_at",
    "rows_processed",
    "error_count",
    "open_work_count",
    "suspected_cause",
    "pid",
    "host",
})


def publish(payload: dict) -> int:
    fields = {name: value for name, value in payload.items() if name in ALLOWED_FIELDS}
    unknown = sorted(set(payload) - ALLOWED_FIELDS)
    if unknown:
        # Report it rather than silently dropping: a typo'd field name would
        # otherwise mean a status column that quietly never appears.
        print(f"ignored unknown status fields: {unknown}", file=sys.stderr)
    if not fields:
        print("no publishable status fields given", file=sys.stderr)
        return 2

    db = build_db_client(project_label="Supervisor status publisher")
    db.connect()
    try:
        flags = SysVarFlags("SUP", db=db, updated_by=UPDATED_BY)
        flags.publish_status(note="supervisor loop heartbeat", **fields)
    finally:
        db.close()
    print(f"published {len(fields)} SUP_Sta_* fields", flush=True)
    return 0


def read_back() -> int:
    """Print the live SUP_Sta_* flags as JSON, including heartbeat age."""
    db = build_db_client(project_label="Supervisor status reader")
    db.connect()
    try:
        status = SysVarFlags("SUP", db=db).read_status()
    finally:
        db.close()
    if status is None:
        print("no SUP_Sta_* flags published", file=sys.stderr)
        return 1
    print(json.dumps(status, indent=2, default=str), flush=True)
    return 0


def main() -> int:
    if "--read" in sys.argv[1:]:
        return read_back()
    raw = sys.stdin.read().strip()
    if not raw:
        print("no payload on stdin", file=sys.stderr)
        return 2
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"bad json payload: {exc}", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        print("payload must be a JSON object", file=sys.stderr)
        return 2
    try:
        return publish(payload)
    except Exception as exc:  # noqa: BLE001 - caller expects stderr/rc on failure
        APP_LOGGER.log_handled_exception(
            exc,
            event_type="supervisor_status_publish_failed",
            context={"script": "publish_supervisor_status.py"},
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
