# Operations Runbook
> Operator-first checks and control flows for the MusicApp Supervisor project.

## Quick Checks

### Windows service

```powershell
Get-Service MusicAppSupervisor
```

Expected normal state:

- `Running`

### Full post-boot check

```powershell
python E:\DevPython\DataSourceQueue\Supervisor\verify_boot.py
```

This reports:

- service state
- Vault reachability and seal state
- MBQueue API health
- FMQueue API health
- MBQueue worker runtime probe
- FMQueue worker runtime probe

### Supervisor logs

Primary logs:

- `E:\DevPython\DataSourceQueue\Supervisor\logs\supervisor.log`
- `E:\DevPython\DataSourceQueue\Supervisor\logs\service_stdout.log`
- `E:\DevPython\DataSourceQueue\Supervisor\logs\service_stderr.log`

Per-part logs:

- `logs\mbqueue_worker.log`
- `logs\mbqueue_api.log`
- `logs\fmqueue_worker.log`
- `logs\fmqueue_api.log`
- `logs\song_hydrator_submit.log`
- `logs\song_hydrator_collect.log`
- `logs\song_lastfm_submit.log`
- `logs\song_lastfm_collect.log`
- `logs\artist_hydrator_submit.log`
- `logs\artist_hydrator_collect.log`
- `logs\artist_lastfm_submit.log`
- `logs\artist_lastfm_collect.log`
- `logs\album_hydrator_submit.log`
- `logs\album_hydrator_collect.log`
- `logs\album_hydrator_hydrate.log`
- `logs\song_hydrator_identity_submit.log`
- `logs\song_hydrator_identity_collect.log`
- `logs\music_explorer_pg.log`
- `logs\graph_explorer_pg.log`
- `logs\cloudflared_tunnel.log`

Diagnostic helper log:

- `logs\runtime_ops_supervisor.log`

Shared event/error log:

- `crawler.app_event_log`
- `crawler.app_event_log_v`

Bootstrap/keyring logs:

- `logs\install_service.log`
- `logs\system_keyring_populate.log`
- `logs\system_keyring_selftest.log`
- `logs\test_restart.log`
- `logs\fast_startup_disable.log`

That log is written by the Supervisor-launched deep runtime sweep helper
(`E:\DevPython\MyMusicCollection\ActiveCode\tools\runtime_ops.py`). The helper
also writes its own JSON/text artifacts under:

- `E:\DevPython\MyMusicCollection\output\runtime_ops_reports\`

## Maintenance And Reload Control

Control helper:

```powershell
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py status
```

Examples:

```powershell
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py disable mbqueue_worker "schema work"
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py enable mbqueue_worker
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py reload song_hydrator_collect "pick up code"
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py reload-hydrators "reload crawler workers"
python E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py reload-supervisor "pick up supervisor.py"
```

Flag directories:

- `control\disabled\`
- `control\reload\`

## Populate LocalSystem Keyring

Use this when the Vault token or unseal key has rotated and the service needs
fresh bootstrap material.

```powershell
powershell -ExecutionPolicy Bypass -File E:\DevPython\DataSourceQueue\Supervisor\elevated_populate.ps1
```

That flow:

1. exports the interactive user's secrets to a short-lived bridge file
2. runs a temporary SYSTEM task
3. imports those secrets into LocalSystem's keyring
4. deletes the bridge file

The underlying scripts are:

- `export_secrets.py` -- source-side keyring export into the short-lived bridge
  JSON
- `system_keyring_populate.py` -- SYSTEM-side import + readback verification

## Activate Keyring-Only Runtime

Use this after installation or reinstallation so the service no longer keeps a
plaintext `VAULT_TOKEN` in its registry-backed environment.

```powershell
powershell -ExecutionPolicy Bypass -File E:\DevPython\DataSourceQueue\Supervisor\elevated_activate.ps1
```

That step rewrites `AppEnvironmentExtra` to keep only `VAULT_ADDR`, then
restarts the service.

## Run The SYSTEM Keyring Self-Test

```powershell
powershell -ExecutionPolicy Bypass -File E:\DevPython\DataSourceQueue\Supervisor\elevated_selftest.ps1
```

This runs `system_keyring_selftest.py` under `/RU SYSTEM` and writes the result
to `logs\system_keyring_selftest.log`.

## Start The Service

```powershell
powershell -ExecutionPolicy Bypass -File E:\DevPython\DataSourceQueue\Supervisor\elevated_start.ps1
```

## Cold-Boot Validation Note

The project's prior validation found that Windows Fast Startup can make a
"Shut down" look like a cold boot when it is not. Real cold-boot testing
should use `Restart`, or a real power-loss scenario.

The historical helper used during that validation is:

- `E:\DevPython\DataSourceQueue\Supervisor\disable_fast_startup.ps1`

## Suspend / Resume Behavior

The Supervisor also owns a companion helper, `power_event_bridge.py`, that is
not part of the managed fleet itself.

It does this:

- on Windows suspend: calls `set_maintenance.py disable-all "host sleep pending"`
- on resume: calls `set_maintenance.py enable-all`

That keeps the fleet from fighting host sleep and lets normal dependency-gated
startup restore the parts in order after resume.
