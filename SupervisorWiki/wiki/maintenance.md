# Maintenance
> Supervisor maintenance checklist: local commands, wiki refresh targets, and the operator/error-handling scripts to review before push.

## Purpose

This is the Supervisor local maintenance page.

Use it together with the cross-repo runbook in:

- `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\fleet_maintenance_runbook.md`
- `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\fleet_repo_inventory.md`

## Repo Identity

- local root: `E:\DevPython\DataSourceQueue\Supervisor`
- git repo: yes
- push target: `https://github.com/jwcolby54/Supervisor.git`
- wiki root: `E:\DevPython\DataSourceQueue\Supervisor\SupervisorWiki\wiki\`

## Standard Commands

```powershell
git status --short
git remote -v
python -m ruff check .
python -m pytest
python -m py_compile supervisor.py supervisor_shared_logging.py set_maintenance.py verify_boot.py unseal_vault.py probe_worker_status.py read_supervisor_sysvars.py publish_supervisor_status.py
```

Current test state:

- `python -m pytest` collects a real suite -- **27 passed** on 2026-08-21
  (`test_probe_batching.py`, `test_restart.py`, `test_supervisor_status.py`).
- The older "may collect 0 items" note was stale and has been corrected.
- `test_supervisor_status.py` replaces `supervisor.LOG` with a test logger, so
  its simulated publisher failures do not land in the live `supervisor.log`
  looking like a real incident. Keep that fixture if you add tests here.

## Wiki Refresh Targets

- `index.md`
- `overview.md`
- `package_design.md`
- `operations_runbook.md`
- `status.md`

## Error-Handling Review Targets

- `supervisor.py`
- `set_maintenance.py`
- `probe_worker_status.py`
- `read_supervisor_sysvars.py`
- `publish_supervisor_status.py`
- `verify_boot.py`
- `unseal_vault.py`

Review for:

- swallowed child-process failures
- subprocess return codes or stderr not surfaced to operators
- helper scripts that fail without leaving an understandable reason
- runtime parts declared in code but not reflected in the wiki

## Push Rule

If Supervisor changes during a fleet maintenance pass, it must get its own
commit and push. Because it is its own repo now, MyMusic wiki updates do not
cover Supervisor code changes.

## Related

- `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\fleet_maintenance_runbook.md`
- `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\fleet_repo_inventory.md`
