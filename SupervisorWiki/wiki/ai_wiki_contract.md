# AI Wiki Contract
> Local adoption of the shared AI wiki contract for the MusicApp Supervisor project.

Supervisor adopts the shared documentation standard defined in:

- `E:\DevPython\SharedPyLib\SharedPyLibWiki\wiki\ai_wiki_contract.md`

For this project, that means:

- the wiki teaches and routes first
- the code verifies and implements
- if code and wiki disagree, the wiki must be repaired

The AI should be able to identify the owning Supervisor script before doing
repo-wide grep.

## Coverage Rule

Every current Python file in `E:\DevPython\DataSourceQueue\Supervisor\` must be
named somewhere in the wiki.

The completeness backstop for that rule is [[module_inventory]].

## Related

- [[quick_start]]
- [[package_design]]
- [[module_inventory]]
