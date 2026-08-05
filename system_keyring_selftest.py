"""Runs AS LocalSystem: prove the keyring round-trips under the SYSTEM account.

This verifies the linchpin assumption behind putting the Vault token + unseal key
in LocalSystem's Credential Manager: that a secret written under SYSTEM can be
read back under SYSTEM. Writes a throwaway probe, reads it, deletes it, and logs
the outcome. Populates nothing real.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

LOG = Path(r"E:\DevPython\DataSourceQueue\Supervisor\logs\system_keyring_selftest.log")
_lines: list[str] = []


def log(msg: str) -> None:
    _lines.append(f"{time.strftime('%H:%M:%S')} {msg}")


def main() -> None:
    try:
        who = subprocess.run(["whoami"], capture_output=True, text=True).stdout.strip()
        log(f"identity (whoami): {who}")
        log(f"python: {sys.executable}")
        import keyring
        log(f"keyring backend: {keyring.get_keyring().__class__.__name__}")

        svc, key = "mmsupervisor-selftest", "probe"
        val = f"probe-{int(time.time())}"
        keyring.set_password(svc, key, val)
        got = keyring.get_password(svc, key)
        log(f"wrote len={len(val)} read len={len(got) if got else 0} match={got == val}")
        try:
            keyring.delete_password(svc, key)
            log("probe cleaned up")
        except Exception as exc:  # noqa: BLE001
            log(f"probe cleanup skipped ({exc!r})")

        log("SELFTEST OK" if got == val else "SELFTEST FAIL (round-trip mismatch)")
    except Exception as exc:  # noqa: BLE001
        log(f"SELFTEST ERROR {exc!r}")
    finally:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        LOG.write_text("\n".join(_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
