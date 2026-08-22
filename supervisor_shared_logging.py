"""Shared Supervisor logging helpers backed by MyMusic's app_event_log contract.

This module keeps Supervisor-side scripts on the same logging stack as the
queue and hydrator runtimes:

- database-first event/error logging via ``jwc_pylib.app_log``
- automatic emergency-file fallback when the DB is unavailable
- optional bridge into stdlib ``logging`` handlers

Supervisor still keeps its local rotating/file logs for operator convenience.
The app-log path is additive observability, not the only sink.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path


_ACTIVE_ROOT = Path(r"E:\DevPython\MyMusicCollection\ActiveCode")
for _name in ("common", "pipeline", "crawler", "tools"):
    _path = _ACTIVE_ROOT / _name
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from jwc_pylib import app_log  # noqa: E402
from mymusic_vault import build_db_client  # noqa: E402


def build_runtime_logger(*, source: str) -> app_log.RuntimeAppLogger:
    """Return a shared app-log writer for one Supervisor script/source."""
    return app_log.RuntimeAppLogger(
        schema="crawler",
        source=source,
        connect=lambda: build_db_client(project_label=f"{source} shared logger").connect(),
    )


class AppLogLoggingHandler(logging.Handler):
    """Mirror stdlib log records into the shared app_event_log contract."""

    def __init__(self, *, source: str, event_type: str = "supervisor_log") -> None:
        super().__init__()
        self._runtime_logger = build_runtime_logger(source=source)
        self._event_type = event_type

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = record.levelname.upper()
            if level == "WARNING":
                level = "WARN"
            if level not in app_log.LEVELS:
                level = "INFO"
            context = {
                "logger_name": record.name,
                "module": record.module,
                "function": record.funcName,
                "line_no": record.lineno,
                "pathname": record.pathname,
            }
            if record.exc_info and record.exc_info[1] is not None:
                self._runtime_logger.log_handled_exception(
                    record.exc_info[1],
                    event_type=self._event_type,
                    context=context,
                )
                return
            self._runtime_logger.log_event(
                level=level,
                event_type=self._event_type,
                message=record.getMessage(),
                context=context,
            )
        except Exception:
            # Logging must never recurse or stop the caller.
            pass

    def close(self) -> None:
        try:
            self._runtime_logger.close()
        finally:
            super().close()

