"""Runs AS LocalSystem: copy the foundational secrets from the bridge file into
LocalSystem's Credential Manager, so the LocalSystem service (and its queue
children) can read the Vault token and unseal key from their OWN keyring -- with
nothing sensitive left in the service's registry config.

Reads the bridge file written by export_secrets.py, stores each secret, reads it
back to verify, and logs the outcome. Does not delete the bridge file (the
elevated caller does that right after this task finishes).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

LOG = Path(r"E:\DevPython\DataSourceQueue\Supervisor\logs\system_keyring_populate.log")
_lines: list[str] = []


def log(msg: str) -> None:
    _lines.append(f"{time.strftime('%H:%M:%S')} {msg}")


def main() -> None:
    try:
        who = subprocess.run(["whoami"], capture_output=True, text=True).stdout.strip()
        log(f"identity (whoami): {who}")
        import keyring

        bridge = Path(sys.argv[1])
        payload = json.loads(bridge.read_text(encoding="utf-8"))
        service = payload["service"]
        secrets = payload["secrets"]

        all_ok = True
        for key, value in secrets.items():
            if not value:
                log(f"{key}: MISSING in bridge -- skipped")
                all_ok = False
                continue
            keyring.set_password(service, key, value)
            back = keyring.get_password(service, key)
            ok = back == value
            all_ok = all_ok and ok
            log(f"{key}: stored len={len(value)} readback_match={ok}")

        log("POPULATE OK" if all_ok else "POPULATE INCOMPLETE")
    except Exception as exc:  # noqa: BLE001
        log(f"POPULATE ERROR {exc!r}")
    finally:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        LOG.write_text("\n".join(_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
