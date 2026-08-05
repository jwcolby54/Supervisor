"""Post-reboot verification of the whole aliveness chain.

Reports, top to bottom:
  OS -> nssm service -> supervisor -> queues, plus the infra the queues need
  (Vault reachable + unsealed). Distinguishes "service/supervisor up" (nssm's
  job) from "fleet healthy" (needs Docker PG/Vault up and Vault unsealed), so a
  reboot result is easy to read even if infra is slow or Vault is sealed.
"""

from __future__ import annotations

import json
import re
import subprocess
import urllib.request


def http_json(url: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def main() -> None:
    print("=== MusicApp boot verification ===\n")

    # 1. Service
    r = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", "(Get-Service MusicAppSupervisor).Status"],
        capture_output=True, text=True,
    )
    print(f"[service ] MusicAppSupervisor: {(r.stdout or r.stderr).strip()}")

    # 2. Vault reachable + unsealed
    status, body = http_json("http://127.0.0.1:18200/v1/sys/health")
    if isinstance(body, dict):
        print(f"[vault   ] reachable, sealed={body.get('sealed')} initialized={body.get('initialized')}")
    else:
        print(f"[vault   ] NOT reachable ({body})")

    # 3. Queue APIs
    for name, url in (("MBQueue", "http://127.0.0.1:18765/health"),
                      ("FMQueue", "http://127.0.0.1:18766/health")):
        status, body = http_json(url)
        ok = isinstance(body, dict) and body.get("status") == "ok"
        print(f"[api     ] {name}: {'OK' if ok else 'DOWN'} ({body if not ok else body.get('run_id')})")

    # 4. Worker drain locks
    try:
        env = {}
        for line in open(r"E:\SharedInfra\shared-stack\.env", encoding="utf-8"):
            m = re.match(r"^([A-Z_]+)=(.*)$", line.strip())
            if m:
                env[m.group(1)] = m.group(2)
        import psycopg2
        c = psycopg2.connect(host="localhost", port=5434, dbname="postgres",
                             user=env["POSTGRES_USER"], password=env["POSTGRES_PASSWORD"])
        cur = c.cursor()
        cur.execute("select objid,pid from pg_locks where locktype='advisory' and objid in (90421001,90421002) order by objid")
        held = {o: p for o, p in cur.fetchall()}
        cur.close()
        c.close()
        print(f"[workers ] MB drain lock: {'HELD pid='+str(held[90421001]) if 90421001 in held else 'NOT held'}")
        print(f"[workers ] FM drain lock: {'HELD pid='+str(held[90421002]) if 90421002 in held else 'NOT held'}")
    except Exception as exc:  # noqa: BLE001
        print(f"[workers ] could not check locks ({exc})")

    print("\nReading: service Running + both APIs OK + both locks HELD == full chain up.")
    print("If service Running but APIs DOWN, infra (Docker/Vault) is not ready yet or Vault is sealed.")


if __name__ == "__main__":
    main()
