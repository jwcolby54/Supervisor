# Supervisor Wiki -- Index
> Master index of all wiki pages for the MusicApp Supervisor project.

Repo entry point: `E:\DevPython\DataSourceQueue\Supervisor\`
If you are not sure where to begin, start at [[quick_start]].

## Pages

### Start here

- [[ai_wiki_contract]] -- local adoption of the shared AI wiki standard:
 wiki-first teaching, code-final verification, and complete module coverage
- [[quick_start]] -- scenario-based chooser for operators, app integrators, and
 people trying to understand the service shape quickly
- [[module_inventory]] -- complete inventory of the current Supervisor Python
 code root
- [[overview]] -- what the supervisor is, what it owns, and how it relates to
 MyMusic, MBQueue, FMQueue, Vault, and PostgreSQL
- [[maintenance]] -- local maintenance checklist for Supervisor plus the
 pointer back to the shared fleet runbook in the MyMusic wiki
- [[operations_runbook]] -- operator-first startup checks, restart flows, and
 maintenance/reload control
- [[status]] -- current implementation state and known doc/code nuances

### Concepts and reference

- [[package_design]] -- every script in `Supervisor\`, what it owns, and how the
 pieces fit together
- [[module_inventory]] -- completeness backstop so every current Supervisor
 Python file is named somewhere in the wiki

## AI Maintenance Hotspots

Open these first when the task is "fix supervised-runtime behavior" rather than
"read the whole project":

- [[quick_start]] -- scenario router
- [[overview]] -- ownership boundary and live fleet shape
- [[package_design]] -- exact file ownership
- [[operations_runbook]] -- operator control flows and log locations
- [[status]] -- current build/runtime nuance only

## Related Projects

- MyMusic integration/deployment view:
 `E:\DevPython\MyMusicCollection\MusicApp_Wiki\wiki\docs\supervisor_service_deployment.md`
- MBQueue wiki:
 `E:\DevPython\DataSourceQueue\MBQueue\MBQueueWiki\wiki\index.md`
- FMQueue wiki:
 `E:\DevPython\DataSourceQueue\FMQueue\FMQueueWiki\wiki\index.md`
- SharedPyLib wiki:
 `E:\DevPython\SharedPyLib\SharedPyLibWiki\wiki\index.md`
