# Package Design
> Script-by-script map of the MusicApp Supervisor project and how the pieces fit together.

## Purpose

This page is the implementation map for `E:\DevPython\DataSourceQueue\Supervisor\`.
Open this before code diving.

## Current Files

| File | Responsibility |
|---|---|
| `supervisor.py` | Main supervisor loop, part registry, dependency probes, startup gating, restart logic |
| `set_maintenance.py` | Local control-plane helper for disable/enable/reload flags |
| `probe_worker_status.py` | Bounded probe of `CR_<Part>_Sta_*` SysVar status flags for MyMusic hydrator parts |
| `read_supervisor_sysvars.py` | Read supervisor tuning overrides from MyMusic sysvars |
| `publish_supervisor_status.py` | Publish the Supervisor's own `SUP_Sta_*` heartbeat/status into `crawler.sysvar`; `--read` prints them back |
| `verify_boot.py` | Post-boot chain verifier for service, Vault, queue APIs, and queue worker runtime probes |
| `unseal_vault.py` | Separate helper that unseals Vault using LocalSystem keyring material |
| `supervisor_shared_logging.py` | Shared app-log bridge for Supervisor scripts (DB first, emergency-file fallback) |
| `install_service.py` | Initial nssm service installer that writes the bootstrap service definition and temporary `VAULT_TOKEN` env var |
| `export_secrets.py` | Export the interactive user's Vault token + unseal key into a short-lived bridge file for the SYSTEM handoff |
| `system_keyring_populate.py` | Runs as LocalSystem to import bridge-file secrets into LocalSystem's keyring and verify readback |
| `system_keyring_selftest.py` | Runs as LocalSystem to prove keyring write/read/delete works before real secret population |
| `elevated_populate.ps1` | Elevated wrapper that runs the export -> SYSTEM-task populate flow |
| `elevated_activate.ps1` | Elevated wrapper that switches the service to the keyring-only env model |
| `elevated_start.ps1` | Elevated helper to start the service |
| `elevated_selftest.ps1` | Elevated helper for the SYSTEM keyring self-test |
| `disable_fast_startup.ps1` | One-time host setting helper used during cold-boot validation |
| `test_restart.py` | Restart/proof helper for service recovery testing |
| `power_event_bridge.py` | Companion helper that listens for Windows suspend/resume and toggles fleet-wide maintenance flags |

## AI Maintenance Hotspots

These files are the fastest route to the real control points:

- `supervisor.py`
 - part registry, startup gating, restart logic, child ownership
- `set_maintenance.py`
 - disable/enable/reload control plane
- `power_event_bridge.py`
 - suspend/resume companion that drives disable-all / enable-all
- `probe_worker_status.py`
 - worker heartbeat/progress wedge logic
- `read_supervisor_sysvars.py`
 - runtime tuning overrides read from MyMusic sysvars
- `publish_supervisor_status.py`
 - the Supervisor's own outward health signal
- `verify_boot.py`
 - end-to-end post-boot verifier
- `unseal_vault.py`
 - separate Vault unseal path used by the supervisor
- `supervisor_shared_logging.py`
 - shared `crawler.app_event_log` bridge for runtime/error logging
- `elevated_activate.ps1`
 - transition from bootstrap token state to keyring-only runtime

## `supervisor.py`

This is the real heart of the project.

It owns:

- the `PARTS` registry
- infrastructure probes for Vault and PostgreSQL
- sequential startup gating
- orphan sweep at startup
- bounded health checks
- restart backoff
- maintenance-disable handling
- child reload handling
- supervisor self-reload handling
- bounded launch/monitor of the deeper MyMusic runtime diagnostic helper
  (`E:\DevPython\MyMusicCollection\ActiveCode\tools\runtime_ops.py`)
- graceful shutdown of the fleet

Important design boundary:

- it never imports queue worker code into its own interpreter
- it only launches child OS processes
- deeper diagnostics remain a separate child process with cooldowns and a max
 runtime; the main loop stays lightweight and DB-free
- it also owns the `power_event_bridge.py` companion so suspend/resume can
  disable and later re-enable the fleet through the existing control flags

## `set_maintenance.py`

This is the local control plane.

It owns:

- per-part disable flags
- global disable flag
- per-part reload flags
- supervisor self-reload flag
- status reporting for those flags

The supervisor consumes these flags; this script only edits them.

`reload-hydrators` covers every crawler lane currently supervised:

- song MB submit/collect
- song FM submit/collect
- artist MB submit/collect
- artist FM submit/collect
- album MB submit/collect/hydrate
- song identity

## `probe_worker_status.py`

This helper exists because MyMusic crawler workers do not expose HTTP `/health`
endpoints.

It owns:

- one read of the crawler `CR_<Part>_Sta_*` SysVar status flags
- the wedge rule for heartbeat/progress (shared `_evaluate_status`)
- one JSON payload the supervisor can trust
- JSON degradation when the probe itself cannot open DB/Vault-backed status

Two modes (added 2026-08-20):

- SINGLE (`--part-name`) -- one worker, one JSON object. Used by the supervisor
  startup path (one part at a time).
- BATCH (`--specs-json '[{part_name,heartbeat_max_seconds,progress_max_seconds},...]'`)
  -- reads MANY workers over ONE DB connection and prints `{part_name: payload}`.
  The steady-state loop uses this so the entire crawler fleet's status costs a
  single subprocess + a single short-lived connection per loop. A whole-batch
  failure returns `{"__batch_error__": ...}` so the supervisor holds (treats those
  parts as inconclusive) rather than acting on a transient blip.

### Per-loop probe cache in `supervisor.py`

`supervise()` builds one `gather_probe_results(states)` cache at the top of every
loop: it batches all crawler-status parts into the single BATCH read above, and
probes each non-crawler part (queue `runtime_probe`, HTTP `/health`) at most once.
Every wedged-check and dependency-check that loop reuses the cache
(`_part_ready_for_dependency(state, probe_cache)`), so a shared upstream such as
`mbqueue_api` (an upstream of ~8 hydrators) is probed ONCE per loop, not once per
dependent. This replaced ~30 subprocess+DB-connection probes per loop with ~5
subprocesses and one DB connection. A part with no result this loop is HELD, never
torn down on inconclusive evidence. The supervisor still holds no persistent DB
connection -- the batch process opens one, reads everything, and exits.

## `read_supervisor_sysvars.py`

This is a bounded read helper, not a long-lived service.

It owns:

- loading MyMusic's DB client
- reading current supervisor tuning overrides from sysvars
- printing one JSON object for `supervisor.py` to consume
- logging failures through the shared app-log path before re-raising
- seeding missing Supervisor runtime-config sysvars before the first read

## `publish_supervisor_status.py`

The Supervisor's own outward health signal, added 2026-08-21. Before it, the one
process watching the fleet was itself observable only by tailing a log file:
`SysVarFlags("SUP").read_status()` returned `None`, so a hung Supervisor and a
healthy one looked identical to anything reading SysVars.

Like `read_supervisor_sysvars.py`, this is a **bounded child process, not a
long-lived service**, and for the same reason. The Supervisor holds no
persistent DB or Vault state in its main loop -- that is what lets it stop a
runaway worker while PostgreSQL is down.

The distinction that makes this safe: the DB-free rule constrains the
Supervisor's *inputs and lifecycle decisions*. Publishing status is an *output*,
and an output may be best-effort. Every failure path in
`supervisor.publish_supervisor_status` is a `LOG.warning` and a return. This is
**not** a reverse channel -- the Supervisor still takes no commands from
SysVars; its control plane stays file-flag based.

It owns:

- reading a JSON payload on stdin and filtering it to known status fields
- opening a Vault-issued DB client, publishing via the shared
  `SysVarFlags("SUP")`, and closing
- printing the live flags as JSON when called with `--read`

`supervisor.py` calls it once per loop pass, rate-limited to
`STATUS_PUBLISH_INTERVAL_SECONDS` (60s, against a 5s loop), and once more on
shutdown with a terminal row -- `down` for a real stop, `maintenance` for a
self-reload, so a deliberate restart does not read as a crash.

`_fleet_status_summary` reports `degraded` only when a part that *should* be
running is not. A maintenance-disabled part is not a fault; counting it as one
would train everyone to ignore the status.

```powershell
cd E:\DevPython\DataSourceQueue\Supervisor ; python publish_supervisor_status.py --read
```

## `verify_boot.py`

This is the operator's "did the whole chain really come up?" script.

It checks:

- Windows service state
- Vault reachability/seal state
- MBQueue API health
- FMQueue API health
- MBQueue worker runtime probe
- FMQueue worker runtime probe

It no longer queries advisory locks directly. Worker truth now comes from each
queue's shared runtime probe (`mbqueue.runtime_probe` / `fmqueue.runtime_probe`).

## `unseal_vault.py`

This helper intentionally runs as a separate process.

It owns:

- reading the unseal key from the current account's keyring
- calling Vault's unseal endpoint
- exiting with simple status codes
- shared error logging before returning a non-zero code on seal-status or unseal
  request failure

It exists so `supervisor.py` itself stays credential-free.

## `supervisor_shared_logging.py`

This helper centralizes Supervisor-side shared logging.

It owns:

- the `RuntimeAppLogger` wiring into `crawler.app_event_log`
- the stdlib `logging.Handler` bridge used by `supervisor.py`
- DB-first logging with emergency-file fallback when the DB is unavailable

## `install_service.py`

This is the bootstrap installer only.

It owns:

- stopping/removing any prior nssm service definition
- writing the `MusicAppSupervisor` service definition
- setting stdout/stderr log targets and restart policy
- injecting bootstrap `VAULT_TOKEN` + `VAULT_ADDR` into `AppEnvironmentExtra`
- starting the service after registration

Its output is intentionally transitional. The intended steady state is after
`elevated_populate.ps1` and `elevated_activate.ps1`, not after this script alone.

## `export_secrets.py`

This is the source-side half of the keyring bootstrap bridge.

It owns:

- reading `SA_SECRET_VAULT_DEV_MIGRATION_TOKEN`
- reading `SA_SECRET_VAULT_UNSEAL_KEY`
- writing those secrets plus the service name into one short-lived JSON bridge
  file for the SYSTEM task to consume

## `system_keyring_populate.py`

This is the destination-side half of the keyring bootstrap bridge.

It owns:

- running as LocalSystem
- reading the bridge JSON file
- writing each secret into LocalSystem's keyring
- reading each one back immediately to prove the write succeeded
- writing a local result log (`logs\system_keyring_populate.log`)

## `system_keyring_selftest.py`

This is the non-secret proof step for the SYSTEM keyring path.

It owns:

- running as LocalSystem
- writing one throwaway probe secret
- reading it back
- deleting it again
- writing the result log (`logs\system_keyring_selftest.log`)

## `elevated_populate.ps1`

This wrapper orchestrates the two-stage bootstrap bridge:

1. run `export_secrets.py` as the interactive elevated user
2. create a temporary scheduled task as `/RU SYSTEM`
3. run `system_keyring_populate.py` under SYSTEM
4. delete the scheduled task
5. remove the plaintext bridge file

## `elevated_selftest.ps1`

This wrapper creates a temporary `/RU SYSTEM` scheduled task that runs
`system_keyring_selftest.py`, then removes the task.

## `elevated_activate.ps1`

This wrapper moves the service to the keyring-only steady state.

It owns:

- replacing `AppEnvironmentExtra` so only `VAULT_ADDR` remains
- removing plaintext `VAULT_TOKEN` from the service environment
- restarting the service so the new model is live

## `elevated_start.ps1`

This is the simple recovery/start wrapper. It starts the Windows service, waits
briefly, then prints the nssm service status.

## `power_event_bridge.py`

This is a Supervisor-owned companion process, not a managed fleet part.

It owns:

- creating a hidden message-only window
- receiving `WM_POWERBROADCAST` suspend/resume events
- issuing `disable-all "host sleep pending"` on suspend
- issuing `enable-all` on resume
- staying outside the managed fleet so it can wake the fleet back up after
  `disable-all`

## `test_restart.py`

This is a one-shot proof helper for the nssm restart contract.

It owns:

- finding the current Supervisor pid
- force-killing it
- polling for a replacement pid
- writing the result to `logs\test_restart.log`

## `disable_fast_startup.ps1`

This is the historical cold-boot validation helper. It sets
`HiberbootEnabled=0`, writes the result to `logs\fast_startup_disable.log`, and
exists only to make Windows "Shut down" behave like a real cold boot during
validation.

## Install / Activation Helpers

These files together explain the full service lifecycle:

- `install_service.py`
 - initial nssm registration
 - still writes a bootstrap `VAULT_TOKEN` env var
- `export_secrets.py`
 - reads the interactive user's keyring
- `system_keyring_populate.py`
 - writes those secrets into LocalSystem's keyring
- `elevated_populate.ps1`
 - orchestrates the export -> SYSTEM populate bridge flow
- `elevated_activate.ps1`
 - removes the plaintext token from the service env and restarts the service

That staged flow matters. The current intended deployed state is after
activation, not just after installation.

## Primary Logs

- `logs\supervisor.log`
- `logs\service_stdout.log`
- `logs\service_stderr.log`
- `crawler.app_event_log` / `crawler.app_event_log_v` for shared runtime/error events
- one stable log per supervised part, for example:
 - `logs\mbqueue_worker.log`
 - `logs\fmqueue_api.log`
 - `logs\song_hydrator_collect.log`
 - `logs\artist_lastfm_collect.log`
 - `logs\graph_explorer_pg.log`
