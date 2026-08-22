"""Tests for the Supervisor's own SUP_Sta_* result-flag publication.

Rule 2 of the process_io_contract says every long-running process publishes at
least a heartbeat and a status. The Supervisor -- the one process that reads
everyone else's flags -- published none of its own, so the thing watching the
fleet was observable only by tailing a log file.

Two invariants matter more than the payload shape:

1. Publishing is best-effort. The Supervisor's whole value is that it keeps
   working when PostgreSQL or Vault is down; a failed status write must be a
   lost sample, never a stalled loop.
2. A maintenance-disabled part is not a fault. Reporting the Supervisor
   degraded because an operator deliberately held a part down would train
   everyone to ignore the status.

Run from the Supervisor directory:

    python -m pytest -q test_supervisor_status.py
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import supervisor  # noqa: E402


class _FakeProc:
    """Stands in for subprocess.Popen: alive when poll() returns None."""

    def __init__(self, alive: bool = True):
        self._alive = alive

    def poll(self):
        return None if self._alive else 1


def _state(name: str, *, alive: bool = True) -> supervisor.PartState:
    spec = next((s for s in supervisor.PARTS if s.name == name), None)
    assert spec is not None, f"unknown part name in test: {name}"
    return supervisor.PartState(spec=spec, proc=_FakeProc(alive))


@pytest.fixture
def parts() -> list[str]:
    return [spec.name for spec in supervisor.PARTS[:4]]


@pytest.fixture(autouse=True)
def _no_maintenance_flags(monkeypatch):
    """Default: nothing is maintenance-disabled, whatever is on disk."""
    monkeypatch.setattr(supervisor, "disable_reason", lambda spec: None)


@pytest.fixture(autouse=True)
def _reset_publish_clock(monkeypatch):
    monkeypatch.setattr(supervisor, "_last_status_publish_at", 0.0, raising=False)


@pytest.fixture(autouse=True)
def _quiet_log(monkeypatch):
    """Keep test-only warnings out of the production supervisor.log.

    supervisor.LOG is the real fleet log handler. Several tests here
    deliberately simulate publisher failures, and those lines showing up in the
    live log would look exactly like a genuine incident to the next reader.
    """
    monkeypatch.setattr(supervisor, "LOG", logging.getLogger("test_supervisor_status"))


# ---------------------------------------------------------------------------
# Fleet summary
# ---------------------------------------------------------------------------


def test_an_all_running_fleet_is_healthy(parts):
    states = [_state(name) for name in parts]
    status, reason, running, down = supervisor._fleet_status_summary(states)
    assert status == "healthy"
    assert running == len(parts)
    assert down == 0
    assert str(len(parts)) in reason


def test_a_dead_part_makes_the_supervisor_degraded(parts):
    states = [_state(name) for name in parts]
    states[1].proc = _FakeProc(alive=False)
    status, reason, running, down = supervisor._fleet_status_summary(states)
    assert status == "degraded"
    assert down == 1
    assert running == len(parts) - 1
    assert states[1].spec.name in reason


def test_a_part_that_never_started_counts_as_down(parts):
    states = [_state(name) for name in parts]
    states[0].proc = None
    status, _reason, _running, down = supervisor._fleet_status_summary(states)
    assert status == "degraded"
    assert down == 1


def test_a_maintenance_disabled_part_is_not_a_fault(monkeypatch, parts):
    # It is doing exactly what an operator told it to do.
    states = [_state(name) for name in parts]
    states[2].proc = None
    held_down = states[2].spec.name
    monkeypatch.setattr(
        supervisor,
        "disable_reason",
        lambda spec: "held for maintenance" if spec.name == held_down else None,
    )
    status, reason, running, down = supervisor._fleet_status_summary(states)
    assert status == "healthy"
    assert down == 0
    assert running == len(parts) - 1
    assert "maintenance-disabled" in reason


def test_the_reason_stays_short_enough_to_store(parts):
    # Reason lands in a SysVar row; the publisher truncates at 500 chars, and
    # the summary should not be relying on that to stay sane.
    states = [_state(spec.name, alive=False) for spec in supervisor.PARTS[:20]]
    _status, reason, _running, _down = supervisor._fleet_status_summary(states)
    assert len(reason) < 200


# ---------------------------------------------------------------------------
# Publishing is best-effort and rate-limited
# ---------------------------------------------------------------------------


def test_publishing_sends_the_payload_to_the_child_process(monkeypatch, parts):
    captured = {}

    class _Completed:
        returncode = 0
        stdout = "published"
        stderr = ""

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["payload"] = json.loads(kwargs["input"])
        return _Completed()

    monkeypatch.setattr(supervisor.subprocess, "run", fake_run)
    supervisor.publish_supervisor_status([_state(name) for name in parts], force=True)

    assert str(supervisor.SUPERVISOR_STATUS_PUBLISHER) in captured["argv"]
    payload = captured["payload"]
    assert payload["status"] == "healthy"
    assert payload["phase"] == "poll"
    assert payload["last_heartbeat_at"]
    assert payload["pid"] and payload["host"]


def test_publishing_runs_in_a_child_process_not_in_the_loop(monkeypatch, parts):
    # The supervisor must hold no DB or Vault state in its main loop -- that is
    # what lets it stop a runaway worker while PostgreSQL is down. The only
    # sanctioned way to publish is by spawning the helper.
    calls = []
    monkeypatch.setattr(
        supervisor.subprocess,
        "run",
        lambda argv, **kw: calls.append(argv) or type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    )
    supervisor.publish_supervisor_status([_state(name) for name in parts], force=True)
    assert len(calls) == 1
    assert calls[0][0] == supervisor.PYTHON


def test_a_publisher_crash_never_propagates(monkeypatch, parts):
    def boom(argv, **kwargs):
        raise OSError("no such interpreter")

    monkeypatch.setattr(supervisor.subprocess, "run", boom)
    # Must return normally. A lost heartbeat is not a stalled fleet.
    supervisor.publish_supervisor_status([_state(name) for name in parts], force=True)


def test_a_publisher_nonzero_exit_never_propagates(monkeypatch, parts):
    class _Failed:
        returncode = 2
        stdout = ""
        stderr = "vault sealed"

    monkeypatch.setattr(supervisor.subprocess, "run", lambda argv, **kw: _Failed())
    supervisor.publish_supervisor_status([_state(name) for name in parts], force=True)


def test_publishing_is_rate_limited_between_loop_passes(monkeypatch, parts):
    calls = []
    monkeypatch.setattr(
        supervisor.subprocess,
        "run",
        lambda argv, **kw: calls.append(1) or type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    )
    states = [_state(name) for name in parts]
    # The loop runs every few seconds; the heartbeat must not follow it.
    supervisor.publish_supervisor_status(states, force=True)
    for _ in range(10):
        supervisor.publish_supervisor_status(states)
    assert len(calls) == 1


def test_the_publish_interval_is_slower_than_the_loop():
    assert supervisor.STATUS_PUBLISH_INTERVAL_SECONDS > supervisor.LOOP_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# The terminal row
# ---------------------------------------------------------------------------


def test_a_clean_shutdown_publishes_down(monkeypatch):
    captured = {}

    def fake_run(argv, **kwargs):
        captured["payload"] = json.loads(kwargs["input"])
        return type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(supervisor.subprocess, "run", fake_run)
    supervisor._publish_supervisor_final_status("service stop", self_reload=False)
    assert captured["payload"]["status"] == "down"
    assert captured["payload"]["phase"] == "shutdown"


def test_a_self_reload_publishes_maintenance_not_down(monkeypatch):
    # It is coming straight back; nobody should be paged for a deliberate
    # restart.
    captured = {}

    def fake_run(argv, **kwargs):
        captured["payload"] = json.loads(kwargs["input"])
        return type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(supervisor.subprocess, "run", fake_run)
    supervisor._publish_supervisor_final_status("pick up supervisor.py", self_reload=True)
    assert captured["payload"]["status"] == "maintenance"
    assert "self-reload" in captured["payload"]["reason"]


def test_a_final_publish_failure_never_blocks_shutdown(monkeypatch):
    monkeypatch.setattr(
        supervisor.subprocess,
        "run",
        lambda argv, **kw: (_ for _ in ()).throw(OSError("database gone")),
    )
    supervisor._publish_supervisor_final_status("service stop", self_reload=False)


# ---------------------------------------------------------------------------
# The published vocabulary must be one the shared validator accepts
# ---------------------------------------------------------------------------


def test_every_status_word_the_supervisor_can_publish_is_valid():
    from jwc_pylib.sysvar_flags import PHASE_VALUES, STATUS_VALUES

    for word in ("healthy", "degraded", "down", "maintenance"):
        assert word in STATUS_VALUES, word
    for word in ("poll", "shutdown"):
        assert word in PHASE_VALUES, word


def test_the_publisher_only_forwards_known_status_fields():
    sys.path.insert(0, str(_HERE))
    import publish_supervisor_status as publisher

    from jwc_pylib.sysvar_flags import STA_FIELDS

    assert publisher.ALLOWED_FIELDS <= set(STA_FIELDS)


def test_the_helper_script_exists_where_the_supervisor_looks_for_it():
    assert supervisor.SUPERVISOR_STATUS_PUBLISHER.exists()


def test_the_publisher_runs_as_a_script(tmp_path):
    # It is spawned by path, so it has to be runnable standalone -- an import
    # error would show up only as a warning line in the supervisor log.
    completed = subprocess.run(
        [sys.executable, str(supervisor.SUPERVISOR_STATUS_PUBLISHER)],
        input="",
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(_HERE),
    )
    # No payload on stdin is a clean rc=2, not a traceback.
    assert completed.returncode == 2
    assert "no payload on stdin" in completed.stderr
