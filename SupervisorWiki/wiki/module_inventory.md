# Module Inventory
> Complete inventory of the current Supervisor Python code root.

## Purpose

This page is the completeness backstop for Supervisor's Python code.

If a Python file exists in the current Supervisor repo root, it must be named
here or on a more specific page.

## Scope

Included here:

- current Python files in `E:\DevPython\DataSourceQueue\Supervisor\`

## Repo Script Inventory

- `export_secrets.py` -- exports or reveals supervisor-relevant secret material
  for controlled operational use.
- `install_service.py` -- installs or refreshes the Windows service wrapper for
  the supervisor runtime.
- `probe_worker_status.py` -- targeted worker health/status probe helper used by
  operational flows.
- `read_supervisor_sysvars.py` -- direct SysVar inspection helper for the
  supervisor control plane.
- `set_maintenance.py` -- toggles or updates the maintenance control flag used
  by the supervised fleet.
- `supervisor.py` -- main supervisor runtime and orchestration entry point.
- `system_keyring_populate.py` -- populates machine-level keyring material for
  the service environment.
- `system_keyring_selftest.py` -- verifies the machine-level keyring path used
  by the service environment.
- `test_restart.py` -- restart-path test/helper script for supervisor-driven
  process control.
- `unseal_vault.py` -- Vault unseal/operator helper used during boot or repair.
- `verify_boot.py` -- verifies that the supervised fleet boot path succeeded.

## Related

- [[ai_wiki_contract]]
- [[wiki_coverage_gap_report]]
- [[quick_start]]
- [[package_design]]
- [[operations_runbook]]
