"""MusicApp Supervisor -- keeps the always-on fleet alive as separate OS processes.

Design source:
    E:\\DevPython\\MyMusicCollection\\DesignFlows\\MusicDiscoveryArchitecture_Active.md
    (sections 24-25: runtime supervision layer).

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

PYTHON = sys.executable  # same interpreter the supervisor runs under

# Singleton guard: only one supervisor may run. Binding this loopback port is a
# cheap, DB-free mutex -- if the bind fails, another supervisor already owns it.
SINGLETON_HOST = "127.0.0.1"
SINGLETON_PORT = 18760

LOOP_INTERVAL_SECONDS = 5
HEALTH_TIMEOUT_SECONDS = 3
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
DIAGNOSTIC_ENABLED = True
DIAGNOSTIC_PERIODIC_SECONDS = 1800
DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS = 300
DIAGNOSTIC_MAX_RUNTIME_SECONDS = 900
DIAGNOSTIC_TIMEOUT_SECONDS = 30
DIAGNOSTIC_FIX_WAIT_SECONDS = 10


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
MYMUSIC_ROOT = Path(r"E:\DevPython\MyMusicCollection")
MYMUSIC_CRAWLER_DIR = MYMUSIC_ROOT / "ActiveCode" / "crawler"
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
    PartSpec(
        name="song_hydrator_submit",
        cwd=MYMUSIC_CRAWLER_DIR,
        argv=[
            PYTHON,
            str(MYMUSIC_CRAWLER_DIR / "MT_songchart_harvester.py"),
            "--hydrate-submit",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_songchart_harvester.py --hydrate-submit --loop",
        requires=("vault", "postgres"),
        requires_parts=("mbqueue_api",),
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
            "100",
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
            "100",
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
            "100",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator.py"),
            "--hydrate-submit",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator.py --hydrate-submit --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator.py"),
            "--hydrate-collect",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator.py --hydrate-collect --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator.py"),
            "--lastfm-submit",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator.py --lastfm-submit --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_artist_hydrator.py"),
            "--lastfm-collect",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_artist_hydrator.py --lastfm-collect --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator.py"),
            "--submit",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator.py --submit --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator.py"),
            "--collect",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator.py --collect --loop",
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
            str(MYMUSIC_CRAWLER_DIR / "MT_album_hydrator.py"),
            "--hydrate",
            "--loop",
            "--batch-limit",
            "100",
            "--sleep-seconds",
            "15",
        ],
        cmdline_match="MT_album_hydrator.py --hydrate --loop",
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
    log_handle: object = field(default=None, repr=False)
    # Last "restart deferred, waiting on ..." reason logged, to avoid spamming.
    dep_wait_note: str = ""
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

    return logger


LOG = build_logger()


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
    global DIAGNOSTIC_ENABLED
    global DIAGNOSTIC_PERIODIC_SECONDS
    global DIAGNOSTIC_TRIGGER_COOLDOWN_SECONDS
    global DIAGNOSTIC_MAX_RUNTIME_SECONDS
    global DIAGNOSTIC_TIMEOUT_SECONDS
    global DIAGNOSTIC_FIX_WAIT_SECONDS

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
    state.health_fail_count = 0
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


def _part_ready_for_dependency(state: PartState) -> tuple[bool, str]:
    """Return whether one supervised part is ready enough for dependents."""
    proc = state.proc
    if proc is None or proc.poll() is not None:
        return False, "not running"
    if disable_reason(state.spec) is not None:
        return False, "maintenance-disabled"
    if state.spec.health_url or state.spec.probe_argv:
        ok, payload = probe_health(state)
        if ok:
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


def _request_shutdown(signum, frame) -> None:  # noqa: ANN001 - signal handler
    global _SHUTDOWN
    _SHUTDOWN = True


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


def supervise(states: list[PartState], diagnostic_state: DiagnosticState) -> None:
    """Run the restart-and-heal loop until a shutdown signal arrives."""
    global _SHUTDOWN, _SELF_RELOAD
    states_by_name = {state.spec.name: state for state in states}
    while not _SHUTDOWN:
        monitor_runtime_diagnostic(diagnostic_state)
        if (
            DIAGNOSTIC_ENABLED
            and DIAGNOSTIC_PERIODIC_SECONDS > 0
            and (
                diagnostic_state.last_launch_at == 0.0
                or (time.monotonic() - diagnostic_state.last_launch_at) >= DIAGNOSTIC_PERIODIC_SECONDS
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
            _SELF_RELOAD = True
            _SHUTDOWN = True
            break
        now = time.monotonic()
        for state in states:
            spec = state.spec
            proc = state.proc
            maintenance_reason = disable_reason(spec)
            reload_reason = consume_part_reload_reason(spec)

            if reload_reason is not None:
                LOG.info("%s reload requested -> %s", spec.name, reload_reason)
                if proc is not None and proc.poll() is None:
                    stop_part(state, f"reload requested: {reload_reason}")
                state.dep_wait_note = ""
                state.health_fail_count = 0
                state.next_start_allowed_at = 0.0
                state.consecutive_failures = 0
                continue

            # Maintenance mode: if disabled, keep the part down and do not
            # count it as failed. If it is still running, stop it cleanly so a
            # human can work on it without the supervisor fighting back.
            if maintenance_reason is not None:
                if maintenance_reason != state.maintenance_note:
                    LOG.info("%s maintenance-disabled -> %s", spec.name, maintenance_reason)
                    state.maintenance_note = maintenance_reason
                if proc is not None and proc.poll() is None:
                    stop_part(state, f"maintenance-disabled: {maintenance_reason}")
                state.dep_wait_note = ""
                state.health_fail_count = 0
                state.next_start_allowed_at = 0.0
                continue
            if state.maintenance_note:
                LOG.info("%s maintenance re-enabled", spec.name)
                state.maintenance_note = ""

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
                if now >= state.next_start_allowed_at:
                    # Never restart a part into infrastructure that is not ready
                    # (e.g. Vault resealed, PostgreSQL restarting) -- that just
                    # recreates crash churn. Wait and re-check instead.
                    ready, unmet = check_all_dependencies(state, states_by_name)
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

            deps_ready, unmet = check_all_dependencies(state, states_by_name)
            if not deps_ready:
                summary = "; ".join(unmet)
                if summary != state.dep_wait_note:
                    LOG.warning("%s dependency lost while running -> %s", spec.name, summary)
                    state.dep_wait_note = summary
                stop_part(state, f"dependency unavailable: {summary}")
                state.health_fail_count = 0
                state.next_start_allowed_at = now + DEPENDENCY_POLL_SECONDS
                continue
            if state.dep_wait_note:
                state.dep_wait_note = ""

            # Secondary wedged check via a bounded health probe. For APIs this is
            # /health; for crawler workers this is the durable worker_status
            # heartbeat/progress contract.
            if (spec.health_url or spec.probe_argv) and (
                now - state.started_at
            ) >= HEALTH_TIMEOUT_SECONDS:
                ok, payload = probe_health(state)
                if ok:
                    state.health_fail_count = 0
                else:
                    state.health_fail_count += 1
                    LOG.warning(
                        "%s health probe failed (%s/%s) -> %s",
                        spec.name,
                        state.health_fail_count,
                        HEALTH_FAIL_LIMIT,
                        _probe_summary(payload),
                    )
                    if state.health_fail_count >= HEALTH_FAIL_LIMIT:
                        LOG.error("%s judged unhealthy/wedged; restarting", spec.name)
                        launch_runtime_diagnostic(
                            diagnostic_state,
                            reason=f"health_probe_failed:{spec.name}",
                        )
                        stop_part(state, "wedged: bounded health probe")
                        state.consecutive_failures += 1
                        state.next_start_allowed_at = now + _backoff(state)

        time.sleep(LOOP_INTERVAL_SECONDS)

    # Graceful shutdown: stop the fleet.
    if _SELF_RELOAD:
        LOG.info("self-reload exit requested; stopping fleet for supervisor restart")
    else:
        LOG.info("shutdown requested; stopping fleet")
    for state in states:
        stop_part(state, "supervisor shutdown")
    stop_runtime_diagnostic(diagnostic_state, "supervisor shutdown")
    if _SELF_RELOAD:
        LOG.info("supervisor exiting for self-reload")
    else:
        LOG.info("supervisor stopped")


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
        probe = INFRA_PROBES.get(dep)
        if probe is None:
            LOG.warning("%s requires unknown dependency '%s' -- ignoring", spec.name, dep)
            continue
            ready, detail = probe()
            if not ready:
                unmet.append(f"{dep}: {detail}")
    return (not unmet), unmet


def check_part_dependencies(state: PartState, states_by_name: dict[str, PartState]) -> tuple[bool, list[str]]:
    """Check the supervised-part prerequisites for one part."""
    unmet: list[str] = []
    for dep_name in state.spec.requires_parts:
        dep_state = states_by_name.get(dep_name)
        if dep_state is None:
            LOG.warning("%s requires unknown part dependency '%s' -- ignoring", state.spec.name, dep_name)
            continue
        ready, detail = _part_ready_for_dependency(dep_state)
        if not ready:
            unmet.append(f"{dep_name}: {detail}")
    return (not unmet), unmet


def check_all_dependencies(state: PartState, states_by_name: dict[str, PartState]) -> tuple[bool, list[str]]:
    """Combine infra and runtime-part dependency checks for one part."""
    ready, unmet = check_dependencies(state.spec)
    parts_ready, part_unmet = check_part_dependencies(state, states_by_name)
    return (ready and parts_ready), [*unmet, *part_unmet]


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
    states_by_name = {state.spec.name: state for state in states}
    for state in states:
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


def main() -> None:
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
    startup_sequence(states)
    if not _SHUTDOWN:
        launch_runtime_diagnostic(
            diagnostic_state,
            reason="startup_complete",
            respect_cooldown=False,
        )

    supervise(states, diagnostic_state)


if __name__ == "__main__":
    main()
