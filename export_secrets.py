"""Runs as the interactive user (jwcolby), elevated: export the two foundational
secrets from THIS user's keyring to a short-lived bridge file, so a SYSTEM task
can copy them into LocalSystem's keyring.

The bridge file holds plaintext secrets for a few seconds and is deleted by the
caller immediately after the SYSTEM task consumes it. One-time bootstrap only.
"""

from __future__ import annotations

import json
import sys

import keyring

OPERATOR_SERVICE = "shared-vault-operator-secrets"
KEYS = ("SA_SECRET_VAULT_DEV_MIGRATION_TOKEN", "SA_SECRET_VAULT_UNSEAL_KEY")


def main() -> int:
    out_path = sys.argv[1]
    data = {k: keyring.get_password(OPERATOR_SERVICE, k) for k in KEYS}
    missing = [k for k, v in data.items() if not v]
    if missing:
        print(f"ERROR: missing in source keyring: {missing}")
        return 1
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"service": OPERATOR_SERVICE, "secrets": data}, fh)
    print(f"exported {len(data)} secrets to bridge file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
