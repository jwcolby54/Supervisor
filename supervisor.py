"""MusicApp Supervisor -- keeps the always-on fleet alive as separate OS processes.

Wiki source of truth:
    E:\\DevPython\\DataSourceQueue\\Supervisor\\SupervisorWiki\\wiki\\index.md

V1 scope: own the four queue processes (MBQueue API + worker, FMQueue API +
worker), restart any that die or wedge, and log every transition.

Hard rules this file honors:
  - Each part is its OWN OS process. The supervisor only ever holds a process
    handle (subprocess.Popen). It NEVER imports or calls a worker's code in its
    own interpreter -- doing so would recreate the single-GIL trap the whole
    architecture exists to avoid.
  - The supervisor holds NO credentials and no persistent DB/Vault connection.
    It must stay alive precisely when PostgreSQL or Vault are down. It performs
    only credential-free, hard-timeout-bounded readiness PROBES (is Vault
    unsealed? is PostgreSQL past startup?) and gates each part's start on the
    services it needs, so the fleet comes up in a clean, ordered way instead of
    crash-looping against infrastructure that is still booting.
  - Liveness is checked out of band and always with a hard timeout, so a hung
    child can never stall the supervisor's own loop.

Later parts (MyMusic Worker A/B, etc.) plug in by adding a row to PARTS. The
supervisor reads durable health state only through bounded helper probes: queue
workers publish `*.Worker.*` SysVars, and crawler workers publish
`CR_<Part>_Sta_*` SysVar flags through `probe_worker_status.py`.
"""

from __future__ import annotations

import logging
import os
import json
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from supervisor_shared_logging import (
    AppLogLoggingHandler,
    build_runtime_logger,
    set_expected_window,
)


# --------------------------------------------------------------------------- #
# Paths and constants
# --------------------------------------------------------------------------- #

SUPERVISOR_DIR = Path(__file__).resolve().parent
LOG_DIR = SUPERVISOR_DIR / "logs"
CONTROL_DIR = SUPERVISOR_DIR / "control"
DISABLED_DIR = CONTROL_DIR / "disabled"
RELOAD_DIR = CONTROL_DIR / "reload"
DATASOURCE_ROOT = SUPERVISOR_DIR.parent  # E:\DevPython\DataSourceQueue
SUPERVISOR_SYSVAR_READER = SUPERVISOR_DIR / "read_supervisor_sysvars.py"
SUPERVISOR_STATUS_PUBLISHER = SUPERVISOR_DIR / "publish_supervisor_status.py"
POWER_EVENT_BRIDGE = SUPERVISOR_DIR / "power_event_bridge.py"

PYTHON = sys.executable  # same interpreter the supervisor runs under

# Singleton guard: only one supervisor may run. Binding this loopback port is a
# cheap, DB-free mutex -- if the bind fails, another supervisor already owns it.
SINGLETON_HOST = "127.0.0.1"
SINGLETON_PORT = 18760

LOOP_INTERVAL_SECONDS = 5
HEALTH_TIMEOUT_SECONDS = 3
# The steady-state loop batches every crawler-worker status read into ONE probe
# subprocess (one short-lived DB connection for the whole fleet) instead of one
# subprocess per worker per loop. That single read is given more headroom than an
# individual probe, but is still hard-bounded so it can never hang the loop.
BATCH_PROBE_TIMEOUT_SECONDS = 10
# A part that has stayed up at least this long is considered stable, so its
# consecutive-failure counter (and thus its backoff) resets.
STABLE_AFTER_SECONDS = 60

# Sequential startup gating. Parts are brought up one at a time, in PARTS order,
# and each must report ready before the next is started. This mirrors the
# by-hand runbook and -- critically -- stops the API and worker of one project
# from refreshing the shared Vault-credential keyring cache at the same instant
# (the concurrent-start race that crosses a username from one lease with a
# password from another). It also means the API never begins accepting enqueues
# until its drainer worker is confirmed up.
READINESS_TIMEOUT_SECONDS = 45   # max wait for one part to report ready
# A worker has no /health endpoint, so "ready" means it survived bootstrap:
# still alive this many seconds after launch (a bad cred/DB/schema bootstrap
# exits well within this window).
WORKER_SETTLE_SECONDS = 8
# Restart backoff: delay = min(CAP, BASE * 2 ** (failures - 1)).
BACKOFF_BASE_SECONDS = 2
BACKOFF_CAP_SECONDS = 60
# How many consecutive failed health probes before an API part is judged wedged
# and force-restarted even though its process is technically still alive.
HEALTH_FAIL_LIMIT = 3
# Even restart-worthy health failures must persist for a while before the
# supervisor recycles the process.
HEALTH_RESTART_MIN_UNHEALTHY_SECONDS = 120
DIAGNOSTIC_ENABLED = True
DIAGNOSTIC_PERIODIC_SECONDS = 1800
DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS = 300
DIAGNOSTIC_MAX_RUNTIME_SECONDS = 900
DIAGNOSTIC_TIMEOUT_SECONDS = 30
DIAGNOSTIC_FIX_WAIT_SECONDS = 10
# Host sleep/resume handling. If the supervisor loop disappears far longer than
# expected, treat that as the machine having slept or resumed and temporarily
# suppress restart/health decisions while the fleet wakes back up.
RESUME_GAP_DETECTED_SECONDS = 30
RESUME_GRACE_SECONDS = 180
# Cold-boot grace for the ops "expected" flag only. On a reboot the supervisor
# starts fresh (no long loop gap, so resume-grace never triggers) while
# PostgreSQL may still be finishing startup, so the supervisor's own WARN lines
# during this opening window -- a worker that started and immediately died on a
# refused connection, "exited code=1" -- are understood restart noise. Rows
# logged in this window are stamped expected=True (foldable on the ops page),
# never dropped, and this window governs ONLY that flag, not restart/health
# decisions (those keep their existing timing).
STARTUP_EXPECTED_GRACE_SECONDS = 180


# --------------------------------------------------------------------------- #
# Infrastructure dependency probes
# --------------------------------------------------------------------------- #
# A part is not started until the services it needs are actually ready. Each
# probe is credential-free and hard-timeout-bounded, so it can never hang the
# supervisor. Add a new dependency (openwebui, celery, ...) by writing a probe
# and listing its key in a PartSpec.requires. Probes return (ready, detail).

VAULT_HEALTH_URL = "http://127.0.0.1:18200/v1/sys/health"
POSTGRES_PROBE_HOST = "127.0.0.1"
POSTGRES_PROBE_PORT = 5434
PROBE_TIMEOUT_SECONDS = 3
# How often to re-probe while waiting for a dependency at boot. Speed is not the
# concern here -- a clean, ordered start is -- so this is deliberately relaxed.
DEPENDENCY_POLL_SECONDS = 5

# Auto-remediation: when Vault is up but sealed, the supervisor spawns this
# helper (a separate, credential-scoped process) to unseal it, instead of waiting
# forever for a human. The helper reads the unseal key from this account's
# keyring; the supervisor itself never holds it. Cooldown avoids hammering.
UNSEAL_HELPER = SUPERVISOR_DIR / "unseal_vault.py"
UNSEAL_COOLDOWN_SECONDS = 15

# How often the supervisor publishes its own SUP_Sta_* result-flags. The loop
# runs every LOOP_INTERVAL_SECONDS (5s); a heartbeat that often would be pure
# noise and a subprocess spawn every 5 seconds besides. 60s is well inside any
# reasonable staleness threshold while costing one child process a minute.
STATUS_PUBLISH_INTERVAL_SECONDS = 60


def probe_vault() -> tuple[bool, str]:
    """Vault is ready only when reachable AND unsealed.

    A sealed Vault answers /sys/health with HTTP 503, so anything that needs it
    must wait rather than launch into failure.
    """
    try:
        with urllib.request.urlopen(VAULT_HEALTH_URL, timeout=PROBE_TIMEOUT_SECONDS) as resp:
            body = resp.read(512).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return False, f"not ready (HTTP {exc.code}, likely sealed)"
    except Exception as exc:  # noqa: BLE001
        return False, f"unreachable ({exc.__class__.__name__})"
    if '"sealed":true' in body.replace(" ", ""):
        return False, "sealed"
    return True, "unsealed"


def probe_postgres() -> tuple[bool, str]:
    """PostgreSQL is ready once it is past 'the database system is starting up'.

    Connects with a throwaway login so the supervisor needs no real credential:
    while PG initializes it answers 57P03 ('starting up'); once ready it rejects
    the bogus login (auth failure / role missing), which proves it is accepting
    real connections.
    """
    try:
        import psycopg2
    except Exception as exc:  # noqa: BLE001
        return False, f"probe unavailable ({exc.__class__.__name__})"
    try:
        conn = psycopg2.connect(
            host=POSTGRES_PROBE_HOST,
            port=POSTGRES_PROBE_PORT,
            dbname="postgres",
            user="supervisor_probe",
            password="supervisor_probe",
            connect_timeout=PROBE_TIMEOUT_SECONDS,
        )
        conn.close()
        return True, "accepting connections"
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if "starting up" in msg:
            return False, "starting up"
        if any(s in msg for s in ("authentication failed", "does not exist", "no pg_hba", "role")):
            return True, "up (probe login rejected, as expected)"
        first = msg.splitlines()[0][:50] if msg else "unknown"
        return False, f"not ready ({first})"


INFRA_PROBES = {
    "vault": probe_vault,
    "postgres": probe_postgres,
    # Future: "openwebui": probe_openwebui, "celery": probe_celery,
}

# Cap how often an infra dependency is actually probed. Many parts can declare
# the same dependency and the loop re-checks every pass, so a live probe per
# check fans out into a connection storm -- and for the postgres probe, a burst
# of rejected-login lines in the PG log. Cache each probe's result so the real
# check runs at most once per this interval no matter how many callers ask.
# Freshness costs at most this many seconds at boot, which the ordered-start
# design already accepts (see DEPENDENCY_POLL_SECONDS).
INFRA_PROBE_CACHE_SECONDS = 10

_infra_probe_cache: dict[str, tuple[float, tuple[bool, str]]] = {}


def probe_infra(dep: str) -> tuple[bool, str]:
    """Run the named infra probe, at most once per INFRA_PROBE_CACHE_SECONDS."""
    probe = INFRA_PROBES[dep]
    now = time.monotonic()
    cached = _infra_probe_cache.get(dep)
    if cached is not None and (now - cached[0]) < INFRA_PROBE_CACHE_SECONDS:
        return cached[1]
    result = probe()
    _infra_probe_cache[dep] = (now, result)
    return result


# --------------------------------------------------------------------------- #
# Part registry -- the fleet. Adding a worker later is adding a row here.
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class PartSpec:
    """Static declaration of one supervised process."""

    name: str
    cwd: Path
    argv: list[str]
    # Substring that uniquely identifies this part in a process command line,
    # used only for the startup orphan sweep.
    cmdline_match: str
    # If set, an HTTP GET here must return an "ok" health body; used as a
    # secondary wedged-detection probe for API parts. Workers have none.
    health_url: Optional[str] = None
    # Infra dependencies (keys in INFRA_PROBES) that must pass before this part
    # is started, and that gate its restart too. Each part declares what it needs.
    requires: tuple[str, ...] = ()
    # Runtime dependencies on other supervised parts by name. If an upstream
    # dependency is down or unhealthy, this part is held down too.
    requires_parts: tuple[str, ...] = ()
    # Optional bounded external probe command. It must print one JSON object
    # containing at least {"ok": bool, "reason": "..."}.
    probe_argv: Optional[list[str]] = None
    probe_cwd: Optional[Path] = None


MBQUEUE_DIR = DATASOURCE_ROOT / "MBQueue"
FMQUEUE_DIR = DATASOURCE_ROOT / "FMQueue"
YTQUEUE_DIR = DATASOURCE_ROOT / "YTQueue"
MYMUSIC_ROOT = Path(r"E:\DevPython\MyMusicCollection")
MYMUSIC_CRAWLER_DIR = MYMUSIC_ROOT / "ActiveCode" / "crawler"
MYMUSIC_TOOLS_DIR = MYMUSIC_ROOT / "ActiveCode" / "tools"
MYMUSIC_EXPLORER_DIR = MYMUSIC_ROOT / "ActiveCode" / "apps" / "music_explorer_pg"
RUNTIME_OPS_SCRIPT = MYMUSIC_ROOT / "ActiveCode" / "tools" / "runtime_ops.py"
WORKER_STATUS_PROBE = SUPERVISOR_DIR / "probe_worker_status.py"
# Public web presence: the two Explorer apps and the Cloudflare tunnel that
# fronts them. cloudflared runs under LocalSystem, so the config path must be
# absolute (its default ~/.cloudflared resolves to the system profile).
CLOUDFLARED_EXE = r"C:\Program Files (x86)\cloudflared\cloudflared.exe"
CLOUDFLARED_CONFIG = r"C:\Users\jwcol\.cloudflared\config.yml"

# Order matters: within each project the drainer worker is started and confirmed
# up BEFORE its API, so the API never accepts enqueues without a live drainer.
PARTS: list[PartSpec] = [
    PartSpec(
        name="mbqueue_worker",
        cwd=MBQUEUE_DIR,
        argv=[PYTHON, "-m", "mbqueue.worker_main"],
        cmdline_match="mbqueue.worker_main",
        health_url=None,
        requires=("vault", "postgres"),
        probe_argv=[
            PYTHON,
            "-m",
            "mbqueue.runtime_probe",
            "--component",
            "Worker",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        probe_cwd=MBQUEUE_DIR,
    ),
    PartSpec(
        name="mbqueue_api",
        cwd=MBQUEUE_DIR,
        argv=[PYTHON, "-m", "mbqueue.api_http"],
        cmdline_match="mbqueue.api_http",
        health_url=None,
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_worker",),
        probe_argv=[
            PYTHON,
            "-m",
            "mbqueue.runtime_probe",
            "--component",
            "Api",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        probe_cwd=MBQUEUE_DIR,
    ),
    PartSpec(
        name="fmqueue_worker",
        cwd=FMQUEUE_DIR,
        argv=[PYTHON, "-m", "fmqueue.worker_main"],
        cmdline_match="fmqueue.worker_main",
        health_url=None,
        requires=("vault", "postgres"),
        probe_argv=[
            PYTHON,
            "-m",
            "fmqueue.runtime_probe",
            "--component",
            "Worker",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        probe_cwd=FMQUEUE_DIR,
    ),
    PartSpec(
        name="fmqueue_api",
        cwd=FMQUEUE_DIR,
        argv=[PYTHON, "-m", "fmqueue.api_http"],
        cmdline_match="fmqueue.api_http",
        health_url=None,
        requires=("vault", "postgres"),
        requires_parts=("fmqueue_worker",),
        probe_argv=[
            PYTHON,
            "-m",
            "fmqueue.runtime_probe",
            "--component",
            "Api",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        probe_cwd=FMQUEUE_DIR,
    ),
    # YTQueue: keyless yt-dlp search transport for the YouTube-id backfill. It has
    # NO api_http (consumers write yt_out over direct DB), so only the drainer
    # worker is supervised. The reaper (a MyMusic tool) scores drained responses
    # into crawler.graph_node_youtube_cache; it is gated on the worker so the two
    # never refresh the shared Vault-credential cache at the same instant.
    PartSpec(
        name="ytqueue_worker",
        cwd=YTQUEUE_DIR,
        argv=[PYTHON, "-m", "ytqueue.worker_main"],
        cmdline_match="ytqueue.worker_main",
        health_url=None,
        requires=("vault", "postgres"),
        probe_argv=[
            PYTHON,
            "-m",
            "ytqueue.runtime_probe",
            "--component",
            "Worker",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
        probe_cwd=YTQUEUE_DIR,
    ),
    PartSpec(
        name="ytqueue_reaper",
        cwd=MYMUSIC_TOOLS_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_TOOLS_DIR / "yt_cache_reaper.py"),
            "--loop",
            "--sleep-seconds",
            "30",
        ],
        cmdline_match="yt_cache_reaper.py --loop",
        requires=("vault", "postgres"),
        requires_parts=("ytqueue_worker",),
    ),
    PartSpec(
        name="song_hydrator_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_songchart_harvester.py"),
            "--hydrate-submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_songchart_harvester.py --hydrate-submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api", "fmqueue_api"),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_song_hydrator_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="song_hydrator_collect",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_songchart_harvester.py"),
            "--hydrate-collect",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_songchart_harvester.py --hydrate-collect --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_song_hydrator_collect",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="song_lastfm_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_songchart_harvester.py"),
            "--lastfm-submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_songchart_harvester.py --lastfm-submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("fmqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_song_lastfm_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="song_lastfm_collect",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_songchart_harvester.py"),
            "--lastfm-collect",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_songchart_harvester.py --lastfm-collect --loop",
        requires=("vault", "postgres"),
        requires_parts=("fmqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_song_lastfm_collect",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="artist_hydrator_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator_ng.py"),
            "--hydrate-submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator_ng.py --hydrate-submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_artist_hydrator_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="artist_hydrator_collect",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator_ng.py"),
            "--hydrate-collect",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator_ng.py --hydrate-collect --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_artist_hydrator_collect",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="artist_lastfm_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator_ng.py"),
            "--lastfm-submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator_ng.py --lastfm-submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("fmqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_artist_lastfm_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="artist_lastfm_collect",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator_ng.py"),
            "--lastfm-collect",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator_ng.py --lastfm-collect --loop",
        requires=("vault", "postgres"),
        requires_parts=("fmqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_artist_lastfm_collect",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    # Album tracklist lane (MB-only). Three stages: submit enqueues release-group
    # lookups, collect drains ready MBQueue responses, hydrate chains the release
    # lookup and caches tracklists into crawler song works. Started submit ->
    # collect -> hydrate so each downstream stage has an upstream already live.
    PartSpec(
        name="album_hydrator_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator_ng.py"),
            "--submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator_ng.py --submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_album_hydrator_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="album_hydrator_collect",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator_ng.py"),
            "--collect",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator_ng.py --collect --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_album_hydrator_collect",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    PartSpec(
        name="album_hydrator_hydrate",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator_ng.py"),
            "--hydrate",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator_ng.py --hydrate --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_album_hydrator_hydrate",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "1800",
        ],
    ),
    # Song identity lane (MB identity + canonical Wikipedia), distinct from the
    # harvester's song_hydrator_submit/collect discovery lane above. Submit admits
    # the small charted set flagged by cr_works.wrk_song_hydrate_pending into the
    # dedicated song_identity queue kind; collect drains that kind, chains any MB
    # fallback hop, and finalizes the stored identity so the graph never shows a
    # Wikipedia title guess.
    PartSpec(
        name="song_hydrator_identity_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_song_hydrator_ng.py"),
            "--submit",
            "--loop",
            "--batch-limit",
            "1",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_song_hydrator_ng.py --submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
        probe_argv=[
            PYTHON,
            str(WORKER_STATUS_PROBE),
            "--part-name",
            "MT_song_hydrator_identity_submit",
            "--heartbeat-max-seconds",
            "180",
            "--progress-max-seconds",
            "3600",
        ],
    ),
    # Public web presence. Both apps self-bootstrap their sys.path and gate the
    # supervisor's startup on a real /health probe, so their DB init is fully
    # serialized (avoids the dynamic-cred cache race). cloudflared starts LAST so
    # the tunnel comes up over already-healthy backends.
    PartSpec(
        name="music_explorer_pg",
        cwd=MYMUSIC_EXPLORER_DIR,
        argv=[PYTHON, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8001"],
        cmdline_match="uvicorn main:app",
        health_url="http://127.0.0.1:8001/health",
        requires=("vault", "postgres"),
    ),
    PartSpec(
        name="graph_explorer_pg",
        cwd=MYMUSIC_EXPLORER_DIR,
        argv=[PYTHON, "-m", "uvicorn", "graph_prototype:app", "--host", "127.0.0.1", "--port", "8002"],
        cmdline_match="graph_prototype:app",
        health_url="http://127.0.0.1:8002/health",
        requires=("vault", "postgres"),
    ),
    PartSpec(
        name="cloudflared_tunnel",
        cwd=MYMUSIC_EXPLORER_DIR,  # cwd is irrelevant; the config path is absolute
        argv=[CLOUDFLARED_EXE, "--config", CLOUDFLARED_CONFIG, "tunnel", "run", "mymusic"],
        cmdline_match="tunnel run mymusic",
        health_url=None,
        requires=(),
        requires_parts=("music_explorer_pg", "graph_explorer_pg"),
    ),
    # External lane-advancement watchdog. Deliberately declares NO
    # `requires_parts`: every other health signal in this fleet is self-reported
    # by the worker being judged, and on 2026-08-22 two lanes forged theirs and
    # sat undetected for hours. This part judges lanes from outcome rows only,
    # so it must keep running when the rest of the fleet is sick -- gating it on
    # music_explorer_pg would silence it in exactly the case it exists for. It
    # degrades gracefully when the ops endpoint is unreachable.
    PartSpec(
        name="lane_watchdog",
        cwd=MYMUSIC_TOOLS_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_TOOLS_DIR / "lane_watchdog.py"),
            "--loop",
            "--interval-seconds",
            "60",
        ],
        cmdline_match="lane_watchdog.py --loop",
        requires=("vault", "postgres"),
    ),
]


# --------------------------------------------------------------------------- #
# Per-part mutable runtime state
# --------------------------------------------------------------------------- #

@dataclass
class PartState:
    """Live handle and bookkeeping for one supervised process."""

    spec: PartSpec
    proc: Optional[subprocess.Popen] = None
    started_at: float = 0.0
    consecutive_failures: int = 0
    next_start_allowed_at: float = 0.0
    health_fail_count: int = 0
    health_unhealthy_since: float = 0.0
    log_handle: object = field(default=None, repr=False)
    # Last "restart deferred, waiting on ..." reason logged, to avoid spamming.
    dep_wait_note: str = ""
    # Last health note logged, to avoid probe-noise spam while observing.
    health_note: str = ""
    # Last maintenance-disable note logged, to avoid loop spam while a part is
    # intentionally held down.
    maintenance_note: str = ""


@dataclass
class DiagnosticState:
    """Live handle and bookkeeping for the bounded runtime diagnostic helper."""

    proc: Optional[subprocess.Popen] = None
    log_handle: object = field(default=None, repr=False)
    launched_at: float = 0.0
    last_launch_at: float = 0.0
    last_reason: str = ""


@dataclass
class CompanionProcessState:
    """Live handle for a helper process owned by the supervisor itself."""

    name: str
    argv: list[str]
    cwd: Path
    proc: Optional[subprocess.Popen] = None
    log_handle: object = field(default=None, repr=False)


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

def build_logger() -> logging.Logger:
    """Return the supervisor logger writing to a rotating file plus stdout."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DISABLED_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("supervisor")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    file_handler = RotatingFileHandler(
        LOG_DIR / "supervisor.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)
    logger.addHandler(
        AppLogLoggingHandler(
            source="supervisor/supervisor.py",
            event_type="supervisor_runtime_log",
        )
    )

    return logger


LOG = build_logger()
APP_LOGGER = build_runtime_logger(source="supervisor/supervisor.py")
APP_LOGGER.install_unhandled_exception_hook()


def load_runtime_settings_from_sysvars() -> None:
    """Overlay supervisor tuning from crawler.sysvar if available."""
    global LOOP_INTERVAL_SECONDS
    global HEALTH_TIMEOUT_SECONDS
    global DEPENDENCY_POLL_SECONDS
    global READINESS_TIMEOUT_SECONDS
    global WORKER_SETTLE_SECONDS
    global BACKOFF_BASE_SECONDS
    global BACKOFF_CAP_SECONDS
    global HEALTH_FAIL_LIMIT
    global HEALTH_RESTART_MIN_UNHEALTHY_SECONDS
    global DIAGNOSTIC_ENABLED
    global DIAGNOSTIC_PERIODIC_SECONDS
    global DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS
    global DIAGNOSTIC_MAX_RUNTIME_SECONDS
    global DIAGNOSTIC_TIMEOUT_SECONDS
    global DIAGNOSTIC_FIX_WAIT_SECONDS
    global RESUME_GAP_DETECTED_SECONDS
    global RESUME_GRACE_SECONDS

    try:
        completed = subprocess.run(
            [PYTHON, str(SUPERVISOR_SYSVAR_READER)],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("supervisor sysvar config skipped: reader failed -> %r", exc)
        return
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        LOG.warning("supervisor sysvar config skipped: reader rc=%s -> %s", completed.returncode, detail)
        return
    try:
        payload = json.loads((completed.stdout or "").strip())
    except json.JSONDecodeError as exc:
        LOG.warning("supervisor sysvar config skipped: bad json -> %r", exc)
        return

    LOOP_INTERVAL_SECONDS = int(payload.get("loop_interval_seconds", LOOP_INTERVAL_SECONDS))
    HEALTH_TIMEOUT_SECONDS = int(payload.get("health_timeout_seconds", HEALTH_TIMEOUT_SECONDS))
    DEPENDENCY_POLL_SECONDS = int(payload.get("dependency_poll_seconds", DEPENDENCY_POLL_SECONDS))
    READINESS_TIMEOUT_SECONDS = int(payload.get("readiness_timeout_seconds", READINESS_TIMEOUT_SECONDS))
    WORKER_SETTLE_SECONDS = int(payload.get("worker_settle_seconds", WORKER_SETTLE_SECONDS))
    BACKOFF_BASE_SECONDS = int(payload.get("backoff_base_seconds", BACKOFF_BASE_SECONDS))
    BACKOFF_CAP_SECONDS = int(payload.get("backoff_cap_seconds", BACKOFF_CAP_SECONDS))
    HEALTH_FAIL_LIMIT = int(payload.get("health_fail_limit", HEALTH_FAIL_LIMIT))
    HEALTH_RESTART_MIN_UNHEALTHY_SECONDS = int(
        payload.get("health_restart_min_unhealthy_seconds", HEALTH_RESTART_MIN_UNHEALTHY_SECONDS)
    )
    DIAGNOSTIC_ENABLED = bool(int(payload.get("diagnostic_enabled", int(DIAGNOSTIC_ENABLED))))
    DIAGNOSTIC_PERIODIC_SECONDS = int(payload.get("diagnostic_periodic_seconds", DIAGNOSTIC_PERIODIC_SECONDS))
    DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS = int(
        payload.get("diagnostic_trigger_cooldown_seconds", DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS)
    )
    DIAGNOSTIC_MAX_RUNTIME_SECONDS = int(
        payload.get("diagnostic_max_runtime_seconds", DIAGNOSTIC_MAX_RUNTIME_SECONDS)
    )
    DIAGNOSTIC_TIMEOUT_SECONDS = int(payload.get("diagnostic_timeout_seconds", DIAGNOSTIC_TIMEOUT_SECONDS))
    DIAGNOSTIC_FIX_WAIT_SECONDS = int(payload.get("diagnostic_fix_wait_seconds", DIAGNOSTIC_FIX_WAIT_SECONDS))
    RESUME_GAP_DETECTED_SECONDS = int(payload.get("resume_gap_detected_seconds", RESUME_GAP_DETECTED_SECONDS))
    RESUME_GRACE_SECONDS = int(payload.get("resume_grace_seconds", RESUME_GRACE_SECONDS))
    LOG.info("loaded supervisor runtime config from crawler.sysvar")


# --------------------------------------------------------------------------- #
# Singleton guard
# --------------------------------------------------------------------------- #

def acquire_singleton() -> socket.socket:
    """Bind the loopback singleton port or exit if another supervisor holds it."""
    guard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        guard.bind((SINGLETON_HOST, SINGLETON_PORT))
        guard.listen(1)
    except OSError:
        LOG.error(
            "another supervisor already holds %s:%s -- exiting",
            SINGLETON_HOST,
            SINGLETON_PORT,
        )
        sys.exit(1)
    return guard


# --------------------------------------------------------------------------- #
# Startup orphan sweep -- guarantee clean ownership of the fleet
# --------------------------------------------------------------------------- #

def sweep_orphans() -> None:
    """Kill any pre-existing queue processes so the supervisor owns all parts.

    A prior interactive/agent shell can leave an API or worker running (the
    classic "healthy API, silently no worker" black hole). Before starting our
    own children we clear anything matching a managed part's module so there is
    exactly one owner: this supervisor.
    """
    matches = [p.cmdline_match for p in PARTS]
    try:
        # One PowerShell call returns "PID<TAB>CommandLine" for every process
        # whose command line mentions any managed module.
        pattern = "|".join(m.replace(".", "\\.") for m in matches)
        ps_cmd = (
            "Get-CimInstance Win32_Process | "
            f"Where-Object {{ $_.CommandLine -match '{pattern}' }} | "
            "ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }"
        )
        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as exc:  # noqa: BLE001 - startup sweep must never be fatal
        LOG.warning("orphan sweep skipped (process scan failed): %r", exc)
        return

    my_pid = os.getpid()
    for line in completed.stdout.splitlines():
        line = line.strip()
        if not line or "\t" not in line:
            continue
        pid_str, cmdline = line.split("\t", 1)
        try:
            pid = int(pid_str)
        except ValueError:
            continue
        if pid == my_pid:
            continue
        LOG.info("orphan sweep: killing pid=%s %s", pid, cmdline.strip())
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F", "/T"],
                capture_output=True,
                text=True,
                timeout=15,
            )
        except Exception as exc:  # noqa: BLE001
            LOG.warning("orphan sweep: failed to kill pid=%s: %r", pid, exc)


# --------------------------------------------------------------------------- #
# Process lifecycle
# --------------------------------------------------------------------------- #

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def start_part(state: PartState) -> None:
    """Spawn one part as its own OS process, redirecting its output to a file."""
    spec = state.spec
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Durable, stable per-part log (fixes queue output previously landing in a
    # transient agent scratchpad).
    log_path = LOG_DIR / f"{spec.name}.log"
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    log_file.write(f"\n===== {spec.name} start {_utc_stamp()} =====\n")
    log_file.flush()

    try:
        proc = subprocess.Popen(
            spec.argv,
            cwd=str(spec.cwd),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.error("failed to start %s: %r", spec.name, exc)
        log_file.close()
        state.consecutive_failures += 1
        state.next_start_allowed_at = time.monotonic() + _backoff(state)
        return

    state.proc = proc
    state.started_at = time.monotonic()
    _clear_health_observation(state)
    state.log_handle = log_file
    LOG.info("started %s pid=%s -> %s", spec.name, proc.pid, log_path.name)


def _backoff(state: PartState) -> float:
    failures = max(1, state.consecutive_failures)
    return min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (failures - 1)))


def probe_health(state: PartState) -> tuple[bool, dict | None]:
    """Return one bounded health result plus any structured probe payload."""
    url = state.spec.health_url
    if url:
        try:
            with urllib.request.urlopen(url, timeout=HEALTH_TIMEOUT_SECONDS) as resp:
                if resp.status != 200:
                    return False, {"reason": f"http_status_{resp.status}"}
                body = resp.read(2048).decode("utf-8", "replace")
                ok = '"status": "ok"' in body or '"status":"ok"' in body
                return ok, {"reason": "ok" if ok else "unexpected_health_body"}
        except Exception:  # noqa: BLE001 - any failure counts as an unhealthy probe
            return False, {"reason": "health_request_failed"}
    if state.spec.probe_argv:
        try:
            completed = subprocess.run(
                state.spec.probe_argv,
                capture_output=True,
                text=True,
                timeout=HEALTH_TIMEOUT_SECONDS,
                cwd=str(state.spec.probe_cwd) if state.spec.probe_cwd is not None else None,
            )
        except subprocess.TimeoutExpired:
            return False, {"reason": "probe_command_timeout"}
        except Exception:  # noqa: BLE001 - bounded probe failure counts unhealthy
            return False, {"reason": "probe_command_failed"}
        if completed.returncode != 0:
            return False, {"reason": f"probe_command_rc_{completed.returncode}"}
        try:
            payload = json.loads((completed.stdout or "").strip())
        except json.JSONDecodeError:
            return False, {"reason": "probe_command_bad_json"}
        return bool(payload.get("ok")), payload
    return True, {"reason": "no_probe_required"}


# --------------------------------------------------------------------------- #
# Per-loop probe cache + batched crawler-status read
# --------------------------------------------------------------------------- #
# Every supervision loop, each part's readiness is needed once for its own wedged
# check and once more for EACH dependent's dependency check. Probing live every
# time meant a shared upstream (e.g. mbqueue_api, an upstream of ~8 hydrators) was
# re-probed ~9x per loop, each a fresh subprocess + DB connection. The loop now
# probes every part at most once and reuses the result, and it reads the whole
# crawler-worker fleet's status in ONE batched subprocess/connection. A part with
# no result this loop (batch miss / transient failure) is treated as inconclusive
# and HELD, never torn down on a single blip.
#
# ProbeResult = tuple[bool, dict | None] (the same shape probe_health returns).


def _crawler_probe_spec(spec: PartSpec) -> dict | None:
    """If this part's health probe is the crawler SysVar-status probe, extract its
    (part_name, heartbeat_max_seconds, progress_max_seconds) for batching."""
    argv = spec.probe_argv
    if not argv or str(WORKER_STATUS_PROBE) not in argv:
        return None

    def _after(flag: str) -> str:
        return argv[argv.index(flag) + 1]

    try:
        return {
            "part_name": _after("--part-name"),
            "heartbeat_max_seconds": int(_after("--heartbeat-max-seconds")),
            "progress_max_seconds": int(_after("--progress-max-seconds")),
        }
    except (ValueError, IndexError):
        return None


def _run_crawler_status_batch(specs: list[dict]) -> dict[str, dict]:
    """Read every requested crawler worker's status in one subprocess/connection.

    Returns {part_name: payload}. Any whole-batch failure (timeout, non-zero exit,
    bad JSON, or the probe's own ``__batch_error__``) returns {} so callers treat
    those parts as inconclusive and hold, rather than acting on a transient blip.
    """
    argv = [PYTHON, str(WORKER_STATUS_PROBE), "--specs-json", json.dumps(specs)]
    try:
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=BATCH_PROBE_TIMEOUT_SECONDS,
            cwd=str(SUPERVISOR_DIR),
        )
    except Exception:  # noqa: BLE001 - a batch probe failure must never hang/kill the loop
        return {}
    if completed.returncode != 0:
        return {}
    try:
        data = json.loads((completed.stdout or "").strip())
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict) or "__batch_error__" in data:
        return {}
    return data


def gather_probe_results(states: list[PartState]) -> dict[str, tuple[bool, dict | None]]:
    """Probe every running part that needs probing, once, sharing one DB read for
    all crawler-status parts. Returns {part_name: (ok, payload)}.

    A crawler part missing from the batch result is intentionally left out of the
    cache (inconclusive -> held); non-crawler probe parts (queue runtime_probe,
    HTTP /health) are each probed exactly once.
    """
    results: dict[str, tuple[bool, dict | None]] = {}

    batch_states: list[PartState] = []
    batch_specs: list[dict] = []
    for state in states:
        proc = state.proc
        if proc is None or proc.poll() is not None:
            continue
        crawler_spec = _crawler_probe_spec(state.spec)
        if crawler_spec is not None:
            batch_states.append(state)
            batch_specs.append(crawler_spec)

    if batch_specs:
        batch = _run_crawler_status_batch(batch_specs)
        for state, crawler_spec in zip(batch_states, batch_specs):
            payload = batch.get(crawler_spec["part_name"])
            if payload is not None:
                results[state.spec.name] = (bool(payload.get("ok")), payload)
            # else: no entry -> leave uncached -> callers hold (inconclusive)

    for state in states:
        if state.spec.name in results:
            continue
        proc = state.proc
        if proc is None or proc.poll() is not None:
            continue
        if not (state.spec.health_url or state.spec.probe_argv):
            continue
        if _crawler_probe_spec(state.spec) is not None:
            continue  # a crawler part the batch could not resolve: hold, do not re-probe live
        results[state.spec.name] = probe_health(state)

    return results


def _part_ready_for_dependency(
    state: PartState,
    probe_cache: dict[str, tuple[bool, dict | None]] | None = None,
) -> tuple[bool, str]:
    """Return whether one supervised part is ready enough for dependents.

    In the steady-state loop the caller passes ``probe_cache`` so a shared upstream
    is probed once per loop, not once per dependent. Outside the loop (startup)
    ``probe_cache`` is None and the probe runs live.
    """
    proc = state.proc
    if proc is None or proc.poll() is not None:
        return False, "not running"
    if disable_reason(state.spec) is not None:
        return False, "maintenance-disabled"
    if state.spec.health_url or state.spec.probe_argv:
        if probe_cache is not None:
            cached = probe_cache.get(state.spec.name)
            if cached is None:
                # No probe result this loop (batch miss/transient): hold the
                # dependent rather than tear it down on inconclusive evidence.
                return True, "probe inconclusive (held)"
            ok, payload = cached
        else:
            ok, payload = probe_health(state)
        if ok or _probe_dependency_ready(payload):
            return True, _probe_summary(payload)
        return False, _probe_summary(payload)
    if (time.monotonic() - state.started_at) < WORKER_SETTLE_SECONDS:
        return False, f"starting (settle<{WORKER_SETTLE_SECONDS}s)"
    return True, "running"


def _probe_summary(payload: dict | None) -> str:
    """Return one compact human-readable probe summary for supervisor logs."""
    if not payload:
        return "no detail"
    fields = []
    for key in (
        "reason",
        "status",
        "runtime_reason",
        "suspected_cause",
        "open_work_count",
        "heartbeat_age_seconds",
        "progress_age_seconds",
    ):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, float):
            value = f"{value:.0f}"
        fields.append(f"{key}={value}")
    return ", ".join(fields) if fields else "no detail"


def _probe_dependency_ready(payload: dict | None) -> bool:
    if not payload:
        return False
    return bool(payload.get("dependency_ready", payload.get("ok", False)))


def _probe_restart_recommended(payload: dict | None) -> bool:
    if not payload:
        return True
    return bool(payload.get("restart_recommended", not payload.get("ok", True)))


def _clear_health_observation(state: PartState) -> None:
    state.health_fail_count = 0
    state.health_unhealthy_since = 0.0
    state.health_note = ""


def _resume_gap_threshold_seconds() -> float:
    """Return the loop-gap threshold that counts as host sleep/resume."""
    return max(float(RESUME_GAP_DETECTED_SECONDS), float(LOOP_INTERVAL_SECONDS * 4))


def _apply_resume_grace(states: list[PartState], gap_seconds: float, now: float) -> float:
    """Enter resume grace after a suspiciously long supervisor loop gap."""
    grace_until = now + RESUME_GRACE_SECONDS
    LOG.warning(
        "host sleep/resume suspected; supervisor loop gap %.1fs exceeded %.1fs. "
        "Suppressing restart/health actions for %ss while the laptop wakes.",
        gap_seconds,
        _resume_gap_threshold_seconds(),
        RESUME_GRACE_SECONDS,
    )
    for state in states:
        _clear_health_observation(state)
        state.dep_wait_note = ""
        state.health_note = ""
        state.next_start_allowed_at = max(state.next_start_allowed_at, grace_until)
    return grace_until


def _expected_window_state(
    now: float,
    resume_grace_until: float,
    startup_expected_until: float,
) -> tuple[bool, str | None]:
    """Return whether ops rows should be flagged expected right now, and why.

    Open during host resume grace (sleep/wake) or the cold-boot startup grace;
    closed otherwise. This governs only the ops-surface 'expected' flag on
    mirrored log rows -- never restart or health timing.
    """
    if now < resume_grace_until:
        return True, "host_resume_grace"
    if now < startup_expected_until:
        return True, "supervisor_startup"
    return False, None


def _reap_exited_during_resume_grace(state: PartState) -> None:
    """Release bookkeeping for a dead child without restarting during grace."""
    proc = state.proc
    if proc is None or proc.poll() is None:
        return
    LOG.warning(
        "%s exited code=%s during host resume grace; holding restart until grace ends",
        state.spec.name,
        proc.poll(),
    )
    if state.log_handle is not None:
        try:
            state.log_handle.close()
        except Exception:  # noqa: BLE001
            pass
        state.log_handle = None
    state.proc = None
    _clear_health_observation(state)


def _build_states_by_name(states: list[PartState]) -> dict[str, PartState]:
    """Return the canonical lookup for part state by part name."""
    return {state.spec.name: state for state in states}


def _build_spec_by_name() -> dict[str, PartSpec]:
    """Return the canonical lookup for part specs by part name."""
    return {spec.name: spec for spec in PARTS}


def _build_dependents_map() -> dict[str, tuple[str, ...]]:
    """Return the direct runtime dependents of each supervised part."""
    spec_by_name = _build_spec_by_name()
    dependents: dict[str, list[str]] = {name: [] for name in spec_by_name}
    for spec in PARTS:
        for dep_name in spec.requires_parts:
            if dep_name in dependents:
                dependents[dep_name].append(spec.name)
    return {name: tuple(children) for name, children in dependents.items()}


def _collect_downstream_names(root_name: str, dependents_map: dict[str, tuple[str, ...]]) -> set[str]:
    """Return `root_name` plus every recursive dependent beneath it."""
    wanted: set[str] = set()
    stack = [root_name]
    while stack:
        current = stack.pop()
        if current in wanted:
            continue
        wanted.add(current)
        stack.extend(dependents_map.get(current, ()))
    return wanted


def _topological_part_names(specs: list[PartSpec]) -> list[str]:
    """Return part names in dependency order, preserving PARTS order when tied."""
    spec_by_name = {spec.name: spec for spec in specs}
    order_index = {spec.name: index for index, spec in enumerate(specs)}
    adjacency: dict[str, list[str]] = {spec.name: [] for spec in specs}
    indegree: dict[str, int] = {spec.name: 0 for spec in specs}

    for spec in specs:
        for dep_name in spec.requires_parts:
            if dep_name not in spec_by_name:
                continue
            adjacency[dep_name].append(spec.name)
            indegree[spec.name] += 1

    ready = sorted(
        [name for name, degree in indegree.items() if degree == 0],
        key=order_index.__getitem__,
    )
    ordered: list[str] = []
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        children = sorted(adjacency[current], key=order_index.__getitem__)
        for child in children:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort(key=order_index.__getitem__)

    if len(ordered) != len(specs):
        unresolved = sorted(set(spec_by_name) - set(ordered), key=order_index.__getitem__)
        raise RuntimeError(f"Supervisor PARTS contains a runtime dependency cycle: {unresolved}")
    return ordered


def _shutdown_order_names(
    target_names: set[str],
    dependents_map: dict[str, tuple[str, ...]],
) -> list[str]:
    """Return reverse dependency order for a target closure."""
    ordered: list[str] = []
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited or name not in target_names:
            return
        visited.add(name)
        for child in dependents_map.get(name, ()):
            visit(child)
        ordered.append(name)

    part_order = [spec.name for spec in PARTS if spec.name in target_names]
    for name in part_order:
        visit(name)
    return ordered


def stop_part_closure(
    states_by_name: dict[str, PartState],
    dependents_map: dict[str, tuple[str, ...]],
    root_name: str,
    reason: str,
) -> list[PartState]:
    """Stop one part and every recursive dependent in reverse dependency order."""
    target_names = _collect_downstream_names(root_name, dependents_map)
    stopped: list[PartState] = []
    for name in _shutdown_order_names(target_names, dependents_map):
        state = states_by_name.get(name)
        if state is None:
            continue
        stop_part(state, reason)
        stopped.append(state)
    return stopped


def _reset_state_after_stop(state: PartState, *, reset_failures: bool) -> None:
    """Clear transient state after an intentional dependency-driven stop."""
    state.dep_wait_note = ""
    _clear_health_observation(state)
    state.next_start_allowed_at = 0.0
    if reset_failures:
        state.consecutive_failures = 0


def stop_part(state: PartState, reason: str) -> None:
    """Terminate a part's process and close its log handle."""
    proc = state.proc
    if proc is not None and proc.poll() is None:
        LOG.info("stopping %s pid=%s (%s)", state.spec.name, proc.pid, reason)
        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("error stopping %s: %r", state.spec.name, exc)
    if state.log_handle is not None:
        try:
            state.log_handle.close()
        except Exception:  # noqa: BLE001
            pass
        state.log_handle = None
    state.proc = None
    _clear_health_observation(state)


def _close_handle(handle: object | None) -> None:
    if handle is None:
        return
    try:
        handle.close()
    except Exception:  # noqa: BLE001
        pass


def launch_runtime_diagnostic(
    state: DiagnosticState,
    *,
    reason: str,
    respect_cooldown: bool = True,
) -> bool:
    """Spawn the bounded runtime sweep helper as a separate child process."""
    if not DIAGNOSTIC_ENABLED:
        return False
    if not RUNTIME_OPS_SCRIPT.exists():
        LOG.warning("runtime diagnostic launch skipped: missing %s", RUNTIME_OPS_SCRIPT)
        return False
    now = time.monotonic()
    if state.proc is not None and state.proc.poll() is None:
        return False
    if (
        respect_cooldown
        and state.last_launch_at
        and (now - state.last_launch_at) < DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS
    ):
        return False

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / "runtime_ops_supervisor.log"
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    log_file.write(f"\n===== runtime_ops start {_utc_stamp()} reason={reason} =====\n")
    log_file.flush()
    argv = [
        PYTHON,
        str(RUNTIME_OPS_SCRIPT),
        "--apply-safe-fixes",
        "--timeout-seconds",
        str(DIAGNOSTIC_TIMEOUT_SECONDS),
        "--fix-wait-seconds",
        str(DIAGNOSTIC_FIX_WAIT_SECONDS),
    ]
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(MYMUSIC_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
    except Exception as exc:  # noqa: BLE001
        _close_handle(log_file)
        LOG.warning("runtime diagnostic launch failed (%s): %r", reason, exc)
        return False

    state.proc = proc
    state.log_handle = log_file
    state.launched_at = now
    state.last_launch_at = now
    state.last_reason = reason
    LOG.info("launched runtime diagnostic pid=%s reason=%s", proc.pid, reason)
    return True


def _start_companion_process(state: CompanionProcessState) -> None:
    """Start one supervisor-owned helper that is not part of the managed fleet."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{state.name}.log"
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    log_file.write(f"\n===== {state.name} start {_utc_stamp()} =====\n")
    log_file.flush()
    try:
        proc = subprocess.Popen(
            state.argv,
            cwd=str(state.cwd),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
    except Exception as exc:  # noqa: BLE001
        _close_handle(log_file)
        LOG.warning("failed to start companion %s: %r", state.name, exc)
        return
    state.proc = proc
    state.log_handle = log_file
    LOG.info("started companion %s pid=%s -> %s", state.name, proc.pid, log_path.name)


def monitor_companion_process(state: CompanionProcessState) -> None:
    """Keep a supervisor-owned helper running without treating it as fleet work."""
    proc = state.proc
    if proc is None:
        _start_companion_process(state)
        return
    exit_code = proc.poll()
    if exit_code is None:
        return
    LOG.warning("companion %s exited code=%s; restarting", state.name, exit_code)
    _close_handle(state.log_handle)
    state.proc = None
    state.log_handle = None
    _start_companion_process(state)


def stop_companion_process(state: CompanionProcessState, reason: str) -> None:
    """Stop a supervisor-owned helper during shutdown/reload."""
    proc = state.proc
    if proc is not None and proc.poll() is None:
        LOG.info("stopping companion %s pid=%s (%s)", state.name, proc.pid, reason)
        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("error stopping companion %s: %r", state.name, exc)
    _close_handle(state.log_handle)
    state.proc = None
    state.log_handle = None


def monitor_runtime_diagnostic(state: DiagnosticState) -> None:
    """Reap or time-box the runtime diagnostic helper without blocking the loop."""
    proc = state.proc
    if proc is None:
        return
    if proc.poll() is None:
        runtime_seconds = time.monotonic() - state.launched_at
        if runtime_seconds < DIAGNOSTIC_MAX_RUNTIME_SECONDS:
            return
        LOG.warning(
            "runtime diagnostic exceeded %ss; terminating pid=%s reason=%s",
            DIAGNOSTIC_MAX_RUNTIME_SECONDS,
            proc.pid,
            state.last_reason,
        )
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("runtime diagnostic stop failed: %r", exc)

    exit_code = proc.poll()
    runtime_seconds = max(0.0, time.monotonic() - state.launched_at)
    LOG.info(
        "runtime diagnostic finished code=%s runtime=%.1fs reason=%s",
        exit_code,
        runtime_seconds,
        state.last_reason,
    )
    _close_handle(state.log_handle)
    state.proc = None
    state.log_handle = None
    state.launched_at = 0.0


def stop_runtime_diagnostic(state: DiagnosticState, reason: str) -> None:
    """Stop the diagnostic helper during supervisor shutdown/reload."""
    proc = state.proc
    if proc is None:
        _close_handle(state.log_handle)
        state.log_handle = None
        return
    if proc.poll() is None:
        LOG.info("stopping runtime diagnostic pid=%s (%s)", proc.pid, reason)
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("error stopping runtime diagnostic: %r", exc)
    _close_handle(state.log_handle)
    state.proc = None
    state.log_handle = None
    state.launched_at = 0.0


# --------------------------------------------------------------------------- #
# Maintenance / disable flags
# --------------------------------------------------------------------------- #

def _disabled_flag_paths(spec: PartSpec) -> list[Path]:
    return [
        DISABLED_DIR / "_all.disabled",
        DISABLED_DIR / f"{spec.name}.disabled",
    ]


def disable_reason(spec: PartSpec) -> str | None:
    """Return the disable reason text if a global or per-part flag exists."""
    for path in _disabled_flag_paths(spec):
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception:  # noqa: BLE001 - unreadable flag still counts as disabled
            text = ""
        if text:
            return f"{path.name}: {text}"
        return path.name
    return None


def is_disabled(spec: PartSpec) -> bool:
    return disable_reason(spec) is not None


# --------------------------------------------------------------------------- #
# Main supervision loop
# --------------------------------------------------------------------------- #

_SHUTDOWN = False
_SELF_RELOAD = False
_SHUTDOWN_REASON = "shutdown requested"


def request_supervisor_shutdown(reason: str = "shutdown requested", *, self_reload: bool = False) -> None:
    """Set the shared shutdown flags consumed by the main supervisor loop."""
    global _SHUTDOWN, _SELF_RELOAD, _SHUTDOWN_REASON
    _SHUTDOWN_REASON = (reason or "shutdown requested").strip() or "shutdown requested"
    _SELF_RELOAD = self_reload
    _SHUTDOWN = True


def _request_shutdown(signum, frame) -> None:  # noqa: ANN001 - signal handler
    del signum, frame
    request_supervisor_shutdown("service stop or signal request")


def _part_reload_flag(spec: PartSpec) -> Path:
    return RELOAD_DIR / f"{spec.name}.reload"


SUPERVISOR_RELOAD_FLAG = RELOAD_DIR / "_supervisor.reload"


def _read_flag_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:  # noqa: BLE001
        return ""


def consume_part_reload_reason(spec: PartSpec) -> str | None:
    path = _part_reload_flag(spec)
    if not path.exists():
        return None
    text = _read_flag_text(path)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    return text or "reload requested"


def consume_supervisor_reload_reason() -> str | None:
    if not SUPERVISOR_RELOAD_FLAG.exists():
        return None
    text = _read_flag_text(SUPERVISOR_RELOAD_FLAG)
    try:
        SUPERVISOR_RELOAD_FLAG.unlink()
    except FileNotFoundError:
        pass
    return text or "supervisor self-reload requested"


def shutdown_fleet(
    states: list[PartState],
    diagnostic_state: DiagnosticState,
    power_bridge_state: CompanionProcessState,
    *,
    reason: str,
    self_reload: bool,
) -> None:
    """Stop the supervised fleet and helpers in dependency-safe order."""
    states_by_name = _build_states_by_name(states)
    dependents_map = _build_dependents_map()

    if self_reload:
        LOG.info("self-reload requested; stopping fleet for supervisor restart -> %s", reason)
    else:
        LOG.info("shutdown requested; stopping fleet -> %s", reason)

    for name in _shutdown_order_names({state.spec.name for state in states}, dependents_map):
        state = states_by_name[name]
        stop_part(state, reason)
    stop_runtime_diagnostic(diagnostic_state, reason)
    stop_companion_process(power_bridge_state, reason)

    # Say goodbye on the way out. Without this the last published row stays
    # "healthy" forever and a reader has to infer death from heartbeat age --
    # which cannot distinguish a clean stop from a crash.
    _publish_supervisor_final_status(
        reason,
        self_reload=self_reload,
    )

    if self_reload:
        LOG.info("supervisor exiting for self-reload")
    else:
        LOG.info("supervisor stopped")


def supervise(
    states: list[PartState],
    diagnostic_state: DiagnosticState,
    power_bridge_state: CompanionProcessState,
) -> None:
    """Run the restart-and-heal loop until a shutdown signal arrives."""
    global _SHUTDOWN, _SELF_RELOAD
    states_by_name = _build_states_by_name(states)
    dependents_map = _build_dependents_map()
    last_loop_at = time.monotonic()
    resume_grace_until = 0.0
    # Cold-boot 'expected' window: open from process start so the opening burst
    # of restart noise is flagged foldable on the ops page (see the constant).
    startup_expected_until = time.monotonic() + STARTUP_EXPECTED_GRACE_SECONDS
    while not _SHUTDOWN:
        now = time.monotonic()
        loop_gap_seconds = now - last_loop_at
        last_loop_at = now
        if loop_gap_seconds >= _resume_gap_threshold_seconds():
            resume_grace_until = _apply_resume_grace(states, loop_gap_seconds, now)
        # Keep the ops 'expected' flag in sync with the boot/resume windows.
        window_active, window_reason = _expected_window_state(
            now, resume_grace_until, startup_expected_until
        )
        set_expected_window(window_active, window_reason)

        monitor_companion_process(power_bridge_state)
        monitor_runtime_diagnostic(diagnostic_state)
        # Rule 2 of the process_io_contract: the supervisor publishes its own
        # heartbeat/status, not just everyone else's. Rate-limited internally
        # and best-effort -- it can never hold up the loop below.
        publish_supervisor_status(states)
        if (
            DIAGNOSTIC_ENABLED
            and DIAGNOSTIC_PERIODIC_SECONDS > 0
            and (
                diagnostic_state.last_launch_at == 0.0
                or (now - diagnostic_state.last_launch_at) >= DIAGNOSTIC_PERIODIC_SECONDS
            )
        ):
            launch_runtime_diagnostic(
                diagnostic_state,
                reason="periodic_supervisor_sweep",
                respect_cooldown=False,
            )
        supervisor_reload_reason = consume_supervisor_reload_reason()
        if supervisor_reload_reason is not None:
            LOG.info("supervisor self-reload requested -> %s", supervisor_reload_reason)
            request_supervisor_shutdown(supervisor_reload_reason, self_reload=True)
            break

        # One probe pass for the whole fleet: batch every crawler-worker status
        # read into a single subprocess/connection, and probe each other part at
        # most once. Every readiness/wedged decision below reuses this cache.
        probe_cache = gather_probe_results(states)

        for state in states:
            spec = state.spec
            proc = state.proc
            maintenance_reason = disable_reason(spec)
            reload_reason = consume_part_reload_reason(spec)

            if reload_reason is not None:
                LOG.info("%s reload requested -> %s", spec.name, reload_reason)
                stopped_states = stop_part_closure(
                    states_by_name,
                    dependents_map,
                    spec.name,
                    f"reload requested: {reload_reason}",
                )
                for stopped_state in stopped_states:
                    _reset_state_after_stop(stopped_state, reset_failures=True)
                continue

            # Maintenance mode: if disabled, keep the part down and do not
            # count it as failed. If it is still running, stop it cleanly so a
            # human can work on it without the supervisor fighting back.
            if maintenance_reason is not None:
                if maintenance_reason != state.maintenance_note:
                    LOG.info("%s maintenance-disabled -> %s", spec.name, maintenance_reason)
                    state.maintenance_note = maintenance_reason
                if proc is not None and proc.poll() is None:
                    stopped_states = stop_part_closure(
                        states_by_name,
                        dependents_map,
                        spec.name,
                        f"maintenance-disabled: {maintenance_reason}",
                    )
                    for stopped_state in stopped_states:
                        _reset_state_after_stop(stopped_state, reset_failures=True)
                state.dep_wait_note = ""
                _clear_health_observation(state)
                state.next_start_allowed_at = 0.0
                continue
            if state.maintenance_note:
                LOG.info("%s maintenance re-enabled", spec.name)
                state.maintenance_note = ""

            if now < resume_grace_until:
                if proc is None or proc.poll() is not None:
                    _reap_exited_during_resume_grace(state)
                continue

            # Not running -> (re)start when backoff allows.
            if proc is None or proc.poll() is not None:
                if proc is not None:
                    code = proc.poll()
                    LOG.warning("%s exited code=%s", spec.name, code)
                    launch_runtime_diagnostic(
                        diagnostic_state,
                        reason=f"part_exited:{spec.name}:code={code}",
                    )
                    if state.log_handle is not None:
                        try:
                            state.log_handle.close()
                        except Exception:  # noqa: BLE001
                            pass
                    state.log_handle = None
                    state.proc = None
                    state.consecutive_failures += 1
                    state.next_start_allowed_at = now + _backoff(state)
                    for stopped_state in stop_part_closure(
                        states_by_name,
                        dependents_map,
                        spec.name,
                        f"upstream exited: {spec.name} code={code}",
                    ):
                        if stopped_state.spec.name == spec.name:
                            continue
                        _reset_state_after_stop(stopped_state, reset_failures=True)
                if now >= state.next_start_allowed_at:
                    # Never restart a part into infrastructure that is not ready
                    # (e.g. Vault resealed, PostgreSQL restarting) -- that just
                    # recreates crash churn. Wait and re-check instead.
                    ready, unmet = check_all_dependencies(state, states_by_name, probe_cache)
                    if not ready:
                        summary = "; ".join(unmet)
                        if summary != state.dep_wait_note:
                            LOG.info("%s restart deferred, waiting on -> %s", spec.name, summary)
                            state.dep_wait_note = summary
                        remediate_if_needed(unmet)
                        state.next_start_allowed_at = now + DEPENDENCY_POLL_SECONDS
                        continue
                    state.dep_wait_note = ""
                    start_part(state)
                continue

            # Running long enough to be considered stable -> reset failures.
            if state.consecutive_failures and (now - state.started_at) >= STABLE_AFTER_SECONDS:
                LOG.info("%s stable; clearing failure count", spec.name)
                state.consecutive_failures = 0

            deps_ready, unmet = check_all_dependencies(state, states_by_name, probe_cache)
            if not deps_ready:
                summary = "; ".join(unmet)
                if summary != state.dep_wait_note:
                    LOG.warning("%s dependency lost while running -> %s", spec.name, summary)
                    state.dep_wait_note = summary
                stopped_states = stop_part_closure(
                    states_by_name,
                    dependents_map,
                    spec.name,
                    f"dependency unavailable: {summary}",
                )
                for stopped_state in stopped_states:
                    _clear_health_observation(stopped_state)
                    stopped_state.next_start_allowed_at = now + DEPENDENCY_POLL_SECONDS
                continue
            if state.dep_wait_note:
                state.dep_wait_note = ""

            # Secondary wedged check via a bounded health probe. For APIs this is
            # /health; for crawler workers this is the durable SysVar
            # heartbeat/progress contract.
            if (spec.health_url or spec.probe_argv) and (
                now - state.started_at
            ) >= HEALTH_TIMEOUT_SECONDS:
                cached = probe_cache.get(spec.name)
                if cached is None:
                    # No probe result this loop (batch miss/transient): hold this
                    # part's wedged decision until the next loop rather than act on
                    # inconclusive evidence.
                    continue
                ok, payload = cached
                if ok:
                    _clear_health_observation(state)
                else:
                    summary = _probe_summary(payload)
                    if not _probe_restart_recommended(payload):
                        if summary != state.health_note:
                            LOG.info("%s health probe observe-only -> %s", spec.name, summary)
                            state.health_note = summary
                        state.health_fail_count = 0
                        state.health_unhealthy_since = 0.0
                        continue
                    if state.health_unhealthy_since == 0.0:
                        state.health_unhealthy_since = now
                    state.health_fail_count += 1
                    unhealthy_seconds = now - state.health_unhealthy_since
                    LOG.warning(
                        "%s health probe failed (%s/%s, %.0fs/%.0fs) -> %s",
                        spec.name,
                        state.health_fail_count,
                        HEALTH_FAIL_LIMIT,
                        unhealthy_seconds,
                        HEALTH_RESTART_MIN_UNHEALTHY_SECONDS,
                        summary,
                    )
                    state.health_note = summary
                    if (
                        state.health_fail_count >= HEALTH_FAIL_LIMIT
                        and unhealthy_seconds >= HEALTH_RESTART_MIN_UNHEALTHY_SECONDS
                    ):
                        LOG.error("%s judged unhealthy/wedged; restarting", spec.name)
                        launch_runtime_diagnostic(
                            diagnostic_state,
                            reason=f"health_probe_failed:{spec.name}",
                        )
                        stop_part(state, "wedged: bounded health probe")
                        state.consecutive_failures += 1
                        state.next_start_allowed_at = now + _backoff(state)

        time.sleep(LOOP_INTERVAL_SECONDS)

    shutdown_fleet(
        states,
        diagnostic_state,
        power_bridge_state,
        reason=_SHUTDOWN_REASON,
        self_reload=_SELF_RELOAD,
    )


def wait_ready(state: PartState) -> bool:
    """Block until a freshly-started part reports ready, or the timeout elapses.

    API parts (those with a health_url) are ready when /health reports ok.
    Worker parts have no health endpoint, so ready means the process survived
    its bootstrap window (still alive after WORKER_SETTLE_SECONDS). Either kind
    fails fast if the process exits. Returns True if ready, False otherwise;
    a False does not abort startup -- the monitor loop keeps retrying that part
    with backoff while the rest of the fleet still comes up.
    """
    spec = state.spec
    deadline = time.monotonic() + READINESS_TIMEOUT_SECONDS
    while time.monotonic() < deadline and not _SHUTDOWN:
        proc = state.proc
        if proc is None or proc.poll() is not None:
            LOG.warning("%s exited during startup (not ready)", spec.name)
            return False
        if spec.health_url or spec.probe_argv:
            ok, payload = probe_health(state)
            if ok:
                LOG.info("%s ready (%s)", spec.name, _probe_summary(payload))
                return True
        else:
            if (time.monotonic() - state.started_at) >= WORKER_SETTLE_SECONDS:
                LOG.info("%s ready (survived %ss settle)", spec.name, WORKER_SETTLE_SECONDS)
                return True
        time.sleep(1.5)
    LOG.warning("%s not ready within %ss; leaving to monitor loop",
                spec.name, READINESS_TIMEOUT_SECONDS)
    return False


def check_dependencies(spec: PartSpec) -> tuple[bool, list[str]]:
    """Probe a part's required infra once; return (all_ready, unmet_reasons)."""
    unmet: list[str] = []
    for dep in spec.requires:
        if dep not in INFRA_PROBES:
            LOG.warning("%s requires unknown dependency '%s' -- ignoring", spec.name, dep)
            continue
        ready, detail = probe_infra(dep)
        if not ready:
            unmet.append(f"{dep}: {detail}")
    return (not unmet), unmet


def check_part_dependencies(
    state: PartState,
    states_by_name: dict[str, PartState],
    probe_cache: dict[str, tuple[bool, dict | None]] | None = None,
) -> tuple[bool, list[str]]:
    """Check the supervised-part prerequisites for one part."""
    unmet: list[str] = []
    for dep_name in state.spec.requires_parts:
        dep_state = states_by_name.get(dep_name)
        if dep_state is None:
            LOG.warning("%s requires unknown part dependency '%s' -- ignoring", state.spec.name, dep_name)
            continue
        ready, detail = _part_ready_for_dependency(dep_state, probe_cache)
        if not ready:
            unmet.append(f"{dep_name}: {detail}")
    return (not unmet), unmet


def check_all_dependencies(
    state: PartState,
    states_by_name: dict[str, PartState],
    probe_cache: dict[str, tuple[bool, dict | None]] | None = None,
) -> tuple[bool, list[str]]:
    """Combine infra and runtime-part dependency checks for one part."""
    ready, unmet = check_dependencies(state.spec)
    parts_ready, part_unmet = check_part_dependencies(state, states_by_name, probe_cache)
    return (ready and parts_ready), [*unmet, *part_unmet]


_last_status_publish_at = 0.0


def _fleet_status_summary(states: list[PartState]) -> tuple[str, str, int, int]:
    """Summarize the fleet into (status, reason, running_count, down_count).

    `down` counts only parts that SHOULD be running: a part held down by a
    maintenance flag is doing exactly what it was told to do, and reporting the
    supervisor as degraded because of it would train everyone to ignore the
    status. That is the same reasoning the loop already applies when it refuses
    to count a maintenance-disabled part as failed.
    """
    running = 0
    down: list[str] = []
    maintenance = 0
    for state in states:
        if disable_reason(state.spec) is not None:
            maintenance += 1
            continue
        proc = state.proc
        if proc is not None and proc.poll() is None:
            running += 1
        else:
            down.append(state.spec.name)

    if down:
        return "degraded", f"down: {', '.join(sorted(down)[:5])}", running, len(down)
    if maintenance:
        return "healthy", f"{running} running, {maintenance} maintenance-disabled", running, 0
    return "healthy", f"all {running} parts running", running, 0


def _publish_supervisor_final_status(reason: str, *, self_reload: bool) -> None:
    """Publish a terminal SUP_Sta_* row so a stopped supervisor says so."""
    payload = {
        # `maintenance` for a self-reload: it is coming straight back, and a
        # reader should not page anyone over a deliberate restart.
        "status": "maintenance" if self_reload else "down",
        "phase": "shutdown",
        "reason": (f"self-reload: {reason}" if self_reload else reason)[:500],
        "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "host": socket.gethostname(),
    }
    try:
        subprocess.run(
            [PYTHON, str(SUPERVISOR_STATUS_PUBLISHER)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as exc:  # noqa: BLE001 - shutdown must complete regardless
        LOG.warning("supervisor final status publish skipped -> %r", exc)


def publish_supervisor_status(states: list[PartState], *, force: bool = False) -> None:
    """Publish this supervisor's own SUP_Sta_* result-flags. Best-effort.

    Runs in a child process so the supervisor's main loop keeps no DB or Vault
    state -- the whole reason it can still stop a runaway worker while
    PostgreSQL is down. Every failure path here is a warning and a return: a
    lost heartbeat sample must never become a stalled fleet.
    """
    global _last_status_publish_at
    now = time.monotonic()
    if not force and (now - _last_status_publish_at) < STATUS_PUBLISH_INTERVAL_SECONDS:
        return
    _last_status_publish_at = now

    status, reason, running, down = _fleet_status_summary(states)
    payload = {
        "status": status,
        "phase": "poll",
        "reason": reason[:500],
        "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
        "open_work_count": running,
        "error_count": down,
        "pid": os.getpid(),
        "host": socket.gethostname(),
    }
    try:
        completed = subprocess.run(
            [PYTHON, str(SUPERVISOR_STATUS_PUBLISHER)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("supervisor status publish skipped: publisher failed -> %r", exc)
        return
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        LOG.warning(
            "supervisor status publish skipped: publisher rc=%s -> %s",
            completed.returncode,
            detail,
        )


_last_unseal_attempt = 0.0


def maybe_unseal_vault() -> None:
    """Spawn the unseal helper if the cooldown has elapsed.

    Runs as a separate process so the supervisor itself never holds the unseal
    key; the helper reads it from this account's keyring and submits it to Vault.
    """
    global _last_unseal_attempt
    now = time.monotonic()
    if now - _last_unseal_attempt < UNSEAL_COOLDOWN_SECONDS:
        return
    _last_unseal_attempt = now
    try:
        result = subprocess.run(
            [PYTHON, str(UNSEAL_HELPER)],
            capture_output=True,
            text=True,
            timeout=25,
        )
        msg = (result.stdout or result.stderr or "").strip()
        LOG.info("vault unseal helper: %s", msg or f"rc={result.returncode}")
    except Exception as exc:  # noqa: BLE001
        LOG.warning("vault unseal helper failed: %r", exc)


def remediate_if_needed(unmet: list[str]) -> None:
    """Try to make an unmet dependency ready. Currently: unseal a sealed Vault."""
    for reason in unmet:
        if reason.startswith("vault") and ("seal" in reason or "503" in reason):
            maybe_unseal_vault()
            break


def wait_for_dependencies(state: PartState, states_by_name: dict[str, PartState]) -> None:
    """Block until all dependencies pass for one part (or shutdown).

    Patient by design: at machine boot Docker/PostgreSQL/Vault can take minutes.
    Starting a part before its dependencies are up is exactly the crash churn
    this gating removes. This includes both base infrastructure readiness and
    supervised upstream parts such as queue APIs/workers. Unmet reasons are
    logged only when they change, so a long wait stays visible without spamming.
    If Vault is up but sealed, the supervisor unseals it (via the helper)
    rather than waiting on a human.
    """
    if not state.spec.requires and not state.spec.requires_parts:
        return
    note = ""
    while not _SHUTDOWN:
        ready, unmet = check_all_dependencies(state, states_by_name)
        if ready:
            required = list(state.spec.requires) + list(state.spec.requires_parts)
            LOG.info("%s dependencies ready (%s)", state.spec.name, ", ".join(required))
            return
        summary = "; ".join(unmet)
        if summary != note:
            LOG.info("%s waiting on dependencies -> %s", state.spec.name, summary)
            note = summary
        remediate_if_needed(unmet)
        time.sleep(DEPENDENCY_POLL_SECONDS)


def startup_sequence(states: list[PartState]) -> None:
    """Start every part one at a time, each gated on its dependencies AND its own
    readiness before the next is started.

    Two gates apply per part:
      1. its declared infra (Vault unsealed, PostgreSQL past startup) must be
         ready -- the supervisor waits patiently rather than launching into a
         still-booting service;
      2. it must then report ready itself before the next part starts, which
         also keeps a project's API and worker from refreshing the shared
         Vault-credential cache at the same instant (the concurrent-start race).
    """
    states_by_name = _build_states_by_name(states)
    ordered_names = _topological_part_names(PARTS)
    for part_name in ordered_names:
        state = states_by_name[part_name]
        if _SHUTDOWN:
            return
        LOG.info("startup: bringing up %s", state.spec.name)
        maintenance_reason = disable_reason(state.spec)
        if maintenance_reason is not None:
            LOG.info("%s skipped at startup (maintenance-disabled -> %s)",
                     state.spec.name, maintenance_reason)
            state.maintenance_note = maintenance_reason
            continue
        wait_for_dependencies(state, states_by_name)
        if _SHUTDOWN:
            return
        start_part(state)
        wait_ready(state)


def main() -> int:
    guard = acquire_singleton()  # noqa: F841 - held for process lifetime
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    load_runtime_settings_from_sysvars()
    LOG.info("supervisor starting; python=%s", PYTHON)
    LOG.info("managing %s parts: %s", len(PARTS), ", ".join(p.name for p in PARTS))
    RELOAD_DIR.mkdir(parents=True, exist_ok=True)

    sweep_orphans()

    states = [PartState(spec=spec) for spec in PARTS]
    diagnostic_state = DiagnosticState()
    power_bridge_state = CompanionProcessState(
        name="power_event_bridge",
        argv=[PYTHON, str(POWER_EVENT_BRIDGE)],
        cwd=SUPERVISOR_DIR,
    )
    monitor_companion_process(power_bridge_state)
    startup_sequence(states)
    if not _SHUTDOWN:
        launch_runtime_diagnostic(
            diagnostic_state,
            reason="startup_complete",
            respect_cooldown=False,
        )

    supervise(states, diagnostic_state, power_bridge_state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
