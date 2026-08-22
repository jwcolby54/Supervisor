"""Offline tests for the supervisor's per-loop probe cache + batched crawler read.

Run: python -m pytest Supervisor/test_probe_batching.py -q   (from DataSourceQueue)

No live DB/queues: the batched crawler read and the individual probe_health are
monkeypatched, so these assert the batching/memoization/hold-on-inconclusive
behavior deterministically.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

import supervisor as sup  # noqa: E402


class FakeProc:
    def __init__(self, alive=True):
        self._alive = alive

    def poll(self):
        return None if self._alive else 0


def _state(name: str, alive=True) -> sup.PartState:
    spec = sup._build_spec_by_name()[name]
    st = sup.PartState(spec=spec)
    st.proc = FakeProc(alive=alive)
    st.started_at = sup.time.monotonic() - 1000  # long-running, past settle
    return st


# --------------------------------------------------------------------------- #
# _crawler_probe_spec parsing
# --------------------------------------------------------------------------- #


def test_crawler_probe_spec_parses_song_identity_submit():
    spec = sup._build_spec_by_name()["song_hydrator_identity_submit"]
    parsed = sup._crawler_probe_spec(spec)
    assert parsed == {
        "part_name": "MT_song_hydrator_identity_submit",
        "heartbeat_max_seconds": 180,
        "progress_max_seconds": 3600,
    }


def test_crawler_probe_spec_none_for_queue_and_http():
    specs = sup._build_spec_by_name()
    # queue API uses mbqueue.runtime_probe, not the crawler WORKER_STATUS_PROBE
    assert sup._crawler_probe_spec(specs["mbqueue_api"]) is None
    # web app uses health_url, no probe_argv
    assert sup._crawler_probe_spec(specs["graph_explorer_pg"]) is None


# --------------------------------------------------------------------------- #
# gather_probe_results: one batch call for all crawler parts, one probe each else
# --------------------------------------------------------------------------- #


def test_gather_batches_crawler_and_memoizes(monkeypatch):
    # A representative slice: 3 crawler workers, 1 queue API, 1 web app.
    names = [
        "song_hydrator_identity_submit",
        "artist_hydrator_submit",
        "album_hydrator_hydrate",
        "mbqueue_api",
        "graph_explorer_pg",
    ]
    states = [_state(n) for n in names]

    batch_calls = []

    def fake_batch(specs):
        batch_calls.append(specs)
        return {
            s["part_name"]: {"ok": True, "reason": "ok", "part_name": s["part_name"]}
            for s in specs
        }

    probe_calls = []

    def fake_probe_health(state):
        probe_calls.append(state.spec.name)
        return True, {"reason": "ok"}

    monkeypatch.setattr(sup, "_run_crawler_status_batch", fake_batch)
    monkeypatch.setattr(sup, "probe_health", fake_probe_health)

    results = sup.gather_probe_results(states)

    # Exactly ONE batch call covering all three crawler workers.
    assert len(batch_calls) == 1
    assert {s["part_name"] for s in batch_calls[0]} == {
        "MT_song_hydrator_identity_submit",
        "MT_artist_hydrator_submit",
        "MT_album_hydrator_hydrate",
    }
    # probe_health used only for the non-crawler probe parts, once each.
    assert sorted(probe_calls) == ["graph_explorer_pg", "mbqueue_api"]
    # Every alive probe part has a cached result.
    assert set(results) == set(names)
    assert all(ok for ok, _ in results.values())


def test_gather_skips_dead_parts(monkeypatch):
    states = [_state("song_hydrator_identity_submit", alive=False), _state("mbqueue_api", alive=False)]
    monkeypatch.setattr(sup, "_run_crawler_status_batch", lambda specs: {})
    monkeypatch.setattr(sup, "probe_health", lambda s: (True, {}))
    assert sup.gather_probe_results(states) == {}


def test_gather_batch_miss_leaves_crawler_uncached(monkeypatch):
    # Batch total failure returns {} -> crawler parts absent from cache (held),
    # and they are NOT re-probed live.
    states = [_state("song_hydrator_identity_submit")]
    monkeypatch.setattr(sup, "_run_crawler_status_batch", lambda specs: {})
    live = []
    monkeypatch.setattr(sup, "probe_health", lambda s: live.append(s.spec.name) or (True, {}))
    results = sup.gather_probe_results(states)
    assert results == {}
    assert live == []  # never re-probed live


# --------------------------------------------------------------------------- #
# _part_ready_for_dependency: cache reuse + hold-on-inconclusive
# --------------------------------------------------------------------------- #


def test_dependency_uses_cache_ready():
    st = _state("mbqueue_api")
    cache = {"mbqueue_api": (True, {"reason": "ok"})}
    ready, _ = sup._part_ready_for_dependency(st, cache)
    assert ready is True


def test_dependency_uses_cache_not_ready():
    st = _state("mbqueue_api")
    cache = {"mbqueue_api": (False, {"reason": "stale_heartbeat"})}
    ready, detail = sup._part_ready_for_dependency(st, cache)
    assert ready is False
    assert "stale_heartbeat" in detail


def test_dependency_holds_when_probe_missing():
    st = _state("mbqueue_api")
    ready, detail = sup._part_ready_for_dependency(st, {})  # cache present, entry missing
    assert ready is True
    assert "inconclusive" in detail


def test_dependency_not_running_is_down():
    st = _state("mbqueue_api", alive=False)
    ready, detail = sup._part_ready_for_dependency(st, {"mbqueue_api": (True, {})})
    assert ready is False
    assert detail == "not running"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
