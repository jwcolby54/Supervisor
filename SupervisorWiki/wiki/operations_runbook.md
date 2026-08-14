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
- advisory lock ownership

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
- `logs\music_explorer_pg.log`
- `logs\graph_explorer_pg.log`
- `logs\cloudflared_tunnel.log`

Diagnostic helper log:

- `logs\runtime_ops_supervisor.log`

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

## Activate Keyring-Only Runtime

Use this after installation or reinstallation so the service no longer keeps a
plaintext `VAULT_TOKEN` in its registry-backed environment.

```powershell
powershell -ExecutionPolicy Bypass -File E:\DevPython\DataSourceQueue\Supervisor\elevated_activate.ps1
```

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
