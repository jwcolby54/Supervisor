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
- `power_event_bridge.py` -- companion suspend/resume listener that toggles the
  fleet-wide maintenance flags outside the managed fleet itself.
- `probe_worker_status.py` -- targeted worker health/status probe helper used by
  operational flows.
- `publish_supervisor_status.py` -- bounded child helper that publishes the
  Supervisor's OWN `SUP_Sta_*` result-flags into `crawler.sysvar`, and reads
  them back with `--read`.
- `read_supervisor_sysvars.py` -- direct SysVar inspection helper for the
  supervisor control plane.
- `set_maintenance.py` -- toggles or updates the maintenance control flag used
  by the supervised fleet.
- `supervisor_shared_logging.py` -- shared app-log bridge used by Supervisor
  scripts for DB-first logging with emergency-file fallback.
- `supervisor.py` -- main supervisor runtime and orchestration entry point.
- `system_keyring_populate.py` -- populates machine-level keyring material for
  the service environment.
- `system_keyring_selftest.py` -- verifies the machine-level keyring path used
  by the service environment.
- `test_restart.py` -- restart-path test/helper script for supervisor-driven
  process control.
- `test_supervisor_status.py` -- offline tests for the `SUP_Sta_*` publication
  path: fleet summary, best-effort publishing, rate limiting, terminal row.
- `unseal_vault.py` -- Vault unseal/operator helper used during boot or repair.
- `verify_boot.py` -- verifies that the supervised fleet boot path succeeded.

## Related

- [[ai_wiki_contract]]
- [[wiki_coverage_gap_report]]
- [[quick_start]]
- [[package_design]]
- [[operations_runbook]]
