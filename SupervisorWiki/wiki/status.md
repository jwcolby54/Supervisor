# Status
> Current implementation state and known documentation/code nuances for the MusicApp Supervisor project.

## Build State

As of 2026-08-06, the Supervisor project contains:

- a working supervisor loop in `supervisor.py`
- local control-plane tooling in `set_maintenance.py`
- bounded MyMusic worker probes in `probe_worker_status.py`
- bounded sysvar tuning reads in `read_supervisor_sysvars.py`
- a post-boot verifier in `verify_boot.py`
- a separate Vault unseal helper in `unseal_vault.py`
- install/activation/bootstrap helpers for the Windows service model

## Current Runtime Shape

The supervised fleet is the `PARTS` registry in `supervisor.py`. As of
2026-08-06 it holds 18 parts:

- MBQueue worker + API
- FMQueue worker + API
- song hydrator submit/collect
- song Last.fm submit/collect
- artist hydrator submit/collect
- artist Last.fm submit/collect
- album hydrator submit/collect/hydrate (three-stage MB-only album tracklist
 lane, added 2026-08-06)
- `music_explorer_pg`
- `graph_explorer_pg`
- `cloudflared_tunnel`

Treat `supervisor.py` `PARTS`, not `/ops`, as the authority on what is
supervised. The live MyMusic `/ops` surface reports the queue, hydrator, and
web parts, but its own `OPS_PARTS` list does not currently expose the artist
Last.fm submit/collect pair even though they are supervised here.

## Known Nuance To Keep Straight

There are two different truths in the repo if you only read one file:

- `install_service.py` still represents the initial bootstrap install step and
 writes a bootstrap `VAULT_TOKEN` env var
- `elevated_activate.ps1` represents the current intended deployed state and
 removes that token, leaving only `VAULT_ADDR`

So documentation should describe the staged flow, not either file in isolation.

## Non-Gaps To Keep Straight

These are deliberate design choices, not missing work:

- one Python supervisor process owning many child OS processes
- no import-and-run of child worker code inside the supervisor interpreter
- bounded probes instead of persistent DB/Vault sessions inside the supervisor
- local file-based maintenance/reload flags instead of DB-owned control flags
- separate `unseal_vault.py` helper instead of storing the unseal key in the
 long-lived supervisor process
