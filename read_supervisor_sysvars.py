"""Read supervisor startup tuning from crawler.sysvar and print one JSON row."""

from __future__ import annotations

from dataclasses import asdict
import json
import sys
from pathlib import Path


ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from crawler_sysvars import resolve_supervisor_runtime_config  # noqa: E402
from mymusic_vault import build_db_client  # noqa: E402


def main() -> int:
    db = build_db_client(project_label="Supervisor sysvar config reader")
    db.connect()
    try:
        config = resolve_supervisor_runtime_config(db)
        print(json.dumps(asdict(config)), flush=True)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
