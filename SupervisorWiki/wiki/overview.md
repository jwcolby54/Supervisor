# Overview
> What the MusicApp Supervisor is, why it exists, and how it keeps the always-on fleet alive.

## What This Is

The MusicApp Supervisor is a standalone Windows-service host for the always-on
parts of the MyMusic runtime.

It owns:

- process supervision for queue workers and APIs
- process supervision for the always-on MyMusic hydrator workers
- process supervision for the artist Last.fm submit/collect workers
- process supervision for the two local FastAPI apps
- process supervision for the Cloudflare tunnel
- dependency-gated startup
- bounded runtime health checks
- Vault auto-unseal
- maintenance-disable and reload control flags

It does not own:

- MusicBrainz transport logic itself
- Last.fm transport logic itself
- MyMusic crawler business rules
- PostgreSQL schema ownership for the supervised projects

Those belong to MBQueue, FMQueue, MyMusic, and SharedPyLib respectively.

## Why It Exists

This project exists because the runtime has moved beyond "start a few scripts
by hand in terminals."

The fleet now includes:

- MBQueue API + worker
- FMQueue API + worker
- song hydrator submit/collect workers
- song Last.fm submit/collect workers
- artist hydrator submit/collect workers
- artist Last.fm submit/collect workers
- `music_explorer_pg`
- `graph_explorer_pg`
- `cloudflared_tunnel`

One owned supervisor is simpler and safer than re-creating those start/restart
rules in multiple launchers or asking the operator to remember them.

This is the current deployed shape, not just source intent. The live MyMusic
ops surface reports these parts under the running `MusicAppSupervisor` service.

## The Shape Of The System

At runtime the supervisor is one long-lived Python process plus a small set of
bounded helper scripts:

1. `supervisor.py` holds the part registry, dependency probes, startup gating,
 restart loop, maintenance/reload handling, and child process ownership.
2. Helper probes report health without giving the supervisor its own long-lived
 DB or Vault session.
3. `set_maintenance.py` manipulates local control flags that the supervisor
 consumes.
4. `verify_boot.py` gives the operator one post-boot health report.
5. Bootstrap/activation helpers provision the LocalSystem keyring and switch
 the service from the initial install state to the current keyring-backed
 state.

The service identity is `LocalSystem`. Secrets are intended to live in
LocalSystem's keyring, not in the service registry config, after activation.

## Current Parts Owned

The current fleet is declared in `supervisor.py` as `PARTS`.

Owned parts include:

- `mbqueue_worker`
- `mbqueue_api`
- `fmqueue_worker`
- `fmqueue_api`
- `song_hydrator_submit`
- `song_hydrator_collect`
- `song_lastfm_submit`
- `song_lastfm_collect`
- `artist_hydrator_submit`
- `artist_hydrator_collect`
- `music_explorer_pg`
- `graph_explorer_pg`
- `cloudflared_tunnel`

The current live `/ops` surface reports this deployed set. The artist Last.fm
submit/collect pair is not currently exposed there as a live supervised part.

## Implementation Files

- `E:\DevPython\DataSourceQueue\Supervisor\supervisor.py`
- `E:\DevPython\DataSourceQueue\Supervisor\set_maintenance.py`
- `E:\DevPython\DataSourceQueue\Supervisor\probe_worker_status.py`
- `E:\DevPython\DataSourceQueue\Supervisor\read_supervisor_sysvars.py`
- `E:\DevPython\DataSourceQueue\Supervisor\verify_boot.py`
- `E:\DevPython\DataSourceQueue\Supervisor\unseal_vault.py`
- `E:\DevPython\DataSourceQueue\Supervisor\install_service.py`
- `E:\DevPython\DataSourceQueue\Supervisor\elevated_activate.ps1`
- `E:\DevPython\DataSourceQueue\Supervisor\export_secrets.py`
- `E:\DevPython\DataSourceQueue\Supervisor\system_keyring_populate.py`
- `E:\DevPython\DataSourceQueue\Supervisor\system_keyring_selftest.py`

## Important Current Nuance

The repository still contains the initial installer flow in
`install_service.py`, which writes a bootstrap `VAULT_TOKEN` into the service
environment. The current intended deployed state is reached only after running
`elevated_activate.ps1`, which removes that plaintext token and leaves
`VAULT_ADDR` only.

So the durable mental model is:

- install
- populate LocalSystem keyring
- activate keyring-only runtime

not "raw install_service.py output forever."
