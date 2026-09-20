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


# Boot/resume "expected" window. When the supervisor knows the host just
# restarted or resumed from sleep, it opens this window; WARN/ERROR lines
# mirrored into app_event_log while it is open are stamped expected=True so the
# ops surface folds them away as understood restart noise instead of alarming.
# Exceptions are NOT force-marked here -- jwc_pylib.app_log.classify_expected
# already decides those, so a real fault (permission denied, deadlock, I/O
# error) that happens to land in the window stays unexpected. Default: closed.
_EXPECTED_WINDOW: dict[str, object] = {"active": False, "reason": None}


def set_expected_window(active: bool, reason: str | None = None) -> None:
    """Open or close the boot/resume 'expected' window for mirrored log rows.

    Called by the supervisor loop as it enters/leaves resume grace or its
    startup grace. Idempotent and process-local (the handler reads it per emit).
    """
    _EXPECTED_WINDOW["active"] = bool(active)
    _EXPECTED_WINDOW["reason"] = reason if active else None


def _current_expected_window() -> tuple[bool, str | None]:
    return bool(_EXPECTED_WINDOW["active"]), _EXPECTED_WINDOW["reason"]  # type: ignore[return-value]


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
            window_active, window_reason = _current_expected_window()
            if record.exc_info and record.exc_info[1] is not None:
                # Let the shared classifier judge exceptions on their own merits
                # (the allow-list must still win inside the window).
                self._runtime_logger.log_handled_exception(
                    record.exc_info[1],
                    event_type=self._event_type,
                    context=context,
                )
                return
            # Non-exception WARN/ERROR lines (e.g. "mbqueue_worker exited
            # code=1") carry no exception to classify, so the window is what
            # marks them as expected restart noise.
            self._runtime_logger.log_event(
                level=level,
                event_type=self._event_type,
                message=record.getMessage(),
                context=context,
                expected=window_active,
                expected_reason=window_reason if window_active else None,
            )
        except Exception:
            # Logging must never recurse or stop the caller.
            pass

    def close(self) -> None:
        try:
            self._runtime_logger.close()
        finally:
            super().close()

