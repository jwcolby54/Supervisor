# Quick Start
> Scenario-based front door for the MusicApp Supervisor project.

## If You Need To...

### Understand what this project is

Read:

1. [[overview]]
2. [[package_design]]
3. [[module_inventory]]
4. [[status]]

### Restart or verify the always-on fleet

Read:

1. [[operations_runbook]]
2. [[package_design]]
3. [[module_inventory]]

### Find which script owns a specific behavior

Read:

1. [[package_design]]
2. [[module_inventory]]

### Debug restart, control-flag, or health-probe behavior fast

Read:

1. [[package_design]]
2. [[operations_runbook]]
3. [[module_inventory]]
4. [[overview]]

Check these files early instead of grepping blindly:

- `E:\DevPython\DataSourceQueue\Supervisor\supervisor.py`
- `E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py`
- `E:\DevPython\DataSourceQueue\Supervisor\probe_worker_status.py`
- `E:\DevPython\DataSourceQueue\Supervisor\read_supervisor_sysvars.py`
- `E:\DevPython\DataSourceQueue\Supervisor\verify_boot.py`
- `E:\DevPython\DataSourceQueue\Supervisor\unseal_vault.py`
- `E:\DevPython\DataSourceQueue\Supervisor\elevated_activate.ps1`

### Understand how MyMusic uses this project

Read:

1. [[overview]]
2. `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\supervisor_service_deployment.md`

## Default Rule

If the question is "what code do I change," open [[package_design]] before
grepping the repo.
