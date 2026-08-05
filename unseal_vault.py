"""Unseal Vault using the unseal key from the running account's keyring.

Run by the supervisor (as LocalSystem) when its vault probe reports sealed. Kept
as a SEPARATE process so the supervisor itself stays credential-free -- this
helper is the only thing that touches the unseal key. It only reads the key and
talks to Vault's seal API; it never connects to a database.

Exit codes: 0 = Vault is unsealed (either already, or we unsealed it); non-zero
otherwise (unreachable, no key, rejected, or still sealed because the threshold
needs more than the one stored key).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

VAULT_ADDR = "http://127.0.0.1:18200"
OPERATOR_SERVICE = "shared-vault-operator-secrets"
UNSEAL_KEY_ID = "SA_SECRET_VAULT_UNSEAL_KEY"


def _seal_status() -> dict:
    with urllib.request.urlopen(VAULT_ADDR + "/v1/sys/seal-status", timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _submit_unseal(key: str) -> dict:
    req = urllib.request.Request(
        VAULT_ADDR + "/v1/sys/unseal",
        data=json.dumps({"key": key}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    try:
        status = _seal_status()
    except Exception as exc:  # noqa: BLE001
        print(f"vault unreachable: {exc!r}")
        return 2
    if not status.get("sealed"):
        print("already unsealed")
        return 0

    import keyring

    key = keyring.get_password(OPERATOR_SERVICE, UNSEAL_KEY_ID)
    if not key:
        print("no unseal key in this account's keyring")
        return 3

    try:
        result = _submit_unseal(key)
    except Exception as exc:  # noqa: BLE001
        print(f"unseal request failed: {exc!r}")
        return 4

    if not result.get("sealed"):
        print("unsealed")
        return 0
    print(f"still sealed (progress {result.get('progress')}/{result.get('t')}) "
          "-- threshold needs more than the one stored key")
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
