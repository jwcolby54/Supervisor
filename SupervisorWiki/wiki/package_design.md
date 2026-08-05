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
| `probe_worker_status.py` | Bounded probe of `crawler.worker_status` for MyMusic hydrator parts |
| `read_supervisor_sysvars.py` | Read supervisor tuning overrides from MyMusic sysvars |
| `verify_boot.py` | Post-boot chain verifier for service, Vault, queue APIs, and advisory locks |
| `unseal_vault.py` | Separate helper that unseals Vault using LocalSystem keyring material |
| `install_service.py` | Initial nssm service installer |
| `export_secrets.py` | Export bootstrap secrets from the interactive user's keyring into a short-lived bridge file |
| `system_keyring_populate.py` | Runs as LocalSystem to import those secrets into LocalSystem's keyring |
| `system_keyring_selftest.py` | Verifies LocalSystem keyring round-trip |
| `elevated_populate.ps1` | Elevated wrapper that runs the export -> SYSTEM-task populate flow |
| `elevated_activate.ps1` | Elevated wrapper that switches the service to the keyring-only env model |
| `elevated_start.ps1` | Elevated helper to start the service |
| `elevated_selftest.ps1` | Elevated helper for the SYSTEM keyring self-test |
| `disable_fast_startup.ps1` | One-time host setting helper used during cold-boot validation |
| `test_restart.py` | Restart/proof helper for service recovery testing |

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
- graceful shutdown of the fleet

Important design boundary:

- it never imports queue worker code into its own interpreter
- it only launches child OS processes

## `set_maintenance.py`

This is the local control plane.

It owns:

- per-part disable flags
- global disable flag
- per-part reload flags
- supervisor self-reload flag
- status reporting for those flags

The supervisor consumes these flags; this script only edits them.

## `probe_worker_status.py`

This helper exists because MyMusic crawler workers do not expose HTTP `/health`
endpoints.

It owns:

- one read of `crawler.worker_status`
- the wedge rule for heartbeat/progress
- one JSON payload the supervisor can trust

## `read_supervisor_sysvars.py`

This is a bounded read helper, not a long-lived service.

It owns:

- loading MyMusic's DB client
- reading current supervisor tuning overrides from sysvars
- printing one JSON object for `supervisor.py` to consume

## `verify_boot.py`

This is the operator's "did the whole chain really come up?" script.

It checks:

- Windows service state
- Vault reachability/seal state
- MBQueue API health
- FMQueue API health
- advisory drain-lock ownership

## `unseal_vault.py`

This helper intentionally runs as a separate process.

It owns:

- reading the unseal key from the current account's keyring
- calling Vault's unseal endpoint
- exiting with simple status codes

It exists so `supervisor.py` itself stays credential-free.

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
- one stable log per supervised part, for example:
 - `logs\mbqueue_worker.log`
 - `logs\fmqueue_api.log`
 - `logs\song_hydrator_collect.log`
 - `logs\artist_lastfm_collect.log`
 - `logs\graph_explorer_pg.log`
