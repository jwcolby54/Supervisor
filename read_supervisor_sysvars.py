"""Read supervisor startup tuning from crawler.sysvar and print one JSON row."""

from __future__ import annotations

from dataclasses import asdict
import json
import sys
from pathlib import Path

from supervisor_shared_logging import build_runtime_logger


ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from crawler_sysvars import (  # noqa: E402
    resolve_supervisor_runtime_config,
    seed_missing_supervisor_runtime_config,
)
from mymusic_vault import build_db_client  # noqa: E402


APP_LOGGER = build_runtime_logger(source="supervisor/read_supervisor_sysvars.py")
APP_LOGGER.install_unhandled_exception_hook()


def main() -> int:
    try:
        db = build_db_client(project_label="Supervisor sysvar config reader")
        db.connect()
        try:
            seed_missing_supervisor_runtime_config(
                db,
                updated_by="Supervisor sysvar config reader",
            )
            config = resolve_supervisor_runtime_config(db)
            print(json.dumps(asdict(config)), flush=True)
            return 0
        finally:
            db.close()
    except Exception as exc:  # noqa: BLE001 - caller expects stderr/rc on failure
        APP_LOGGER.log_handled_exception(
            exc,
            event_type="supervisor_sysvar_reader_failed",
            context={"script": "read_supervisor_sysvars.py"},
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
