"""Set or clear supervisor maintenance and reload flags.

Usage examples:
    python set_maintenance.py status
    python set_maintenance.py disable mbqueue_worker "schema work"
    python set_maintenance.py enable mbqueue_worker
    python set_maintenance.py disable-all "planned maintenance"
    python set_maintenance.py enable-all
    python set_maintenance.py reload song_hydrator_collect "pick up code"
    python set_maintenance.py reload-hydrators "reload crawler hydrators"
    python set_maintenance.py reload-supervisor "pick up supervisor.py"
"""
from __future__ import annotations

import argparse
from pathlib import Path

from supervisor import DISABLED_DIR, PARTS, RELOAD_DIR, SUPERVISOR_RELOAD_FLAG


ALL_FLAG = DISABLED_DIR / "_all.disabled"
KNOWN_PARTS = {part.name for part in PARTS}
HYDRATOR_PARTS = [
    "song_hydrator_submit",
    "song_hydrator_collect",
    "song_lastfm_submit",
    "song_lastfm_collect",
    "artist_hydrator_submit",
    "artist_hydrator_collect",
    "artist_lastfm_submit",
    "artist_lastfm_collect",
    "album_hydrator_submit",
    "album_hydrator_collect",
    "album_hydrator_hydrate",
]


def _part_flag(name: str) -> Path:
    return DISABLED_DIR / f"{name}.disabled"


def _part_reload_flag(name: str) -> Path:
    return RELOAD_DIR / f"{name}.reload"


def _write_flag(path: Path, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = reason.strip() if reason else ""
    if text:
        path.write_text(text + "\n", encoding="utf-8")
    else:
        path.write_text("", encoding="utf-8")


def _remove_flag(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def cmd_status() -> int:
    DISABLED_DIR.mkdir(parents=True, exist_ok=True)
    RELOAD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"global: {'DISABLED' if ALL_FLAG.exists() else 'enabled'}")
    if ALL_FLAG.exists():
        print(f"  reason: {ALL_FLAG.read_text(encoding='utf-8').strip() or '(none)'}")
    if SUPERVISOR_RELOAD_FLAG.exists():
        print("supervisor reload: PENDING")
        print(f"  reason: {SUPERVISOR_RELOAD_FLAG.read_text(encoding='utf-8').strip() or '(none)'}")
    else:
        print("supervisor reload: none")
    for name in sorted(KNOWN_PARTS):
        disable_path = _part_flag(name)
        reload_path = _part_reload_flag(name)
        if disable_path.exists():
            print(f"{name}: DISABLED")
            print(f"  reason: {disable_path.read_text(encoding='utf-8').strip() or '(none)'}")
        else:
            print(f"{name}: enabled")
        if reload_path.exists():
            print("  reload: PENDING")
            print(f"  reload reason: {reload_path.read_text(encoding='utf-8').strip() or '(none)'}")
    return 0


def cmd_disable(name: str, reason: str) -> int:
    if name not in KNOWN_PARTS:
        raise SystemExit(f"Unknown part: {name}")
    _write_flag(_part_flag(name), reason)
    print(f"disabled {name}")
    return 0


def cmd_enable(name: str) -> int:
    if name not in KNOWN_PARTS:
        raise SystemExit(f"Unknown part: {name}")
    _remove_flag(_part_flag(name))
    print(f"enabled {name}")
    return 0


def cmd_disable_all(reason: str) -> int:
    _write_flag(ALL_FLAG, reason)
    print("disabled all parts")
    return 0


def cmd_enable_all() -> int:
    _remove_flag(ALL_FLAG)
    print("enabled all parts")
    return 0


def cmd_reload(name: str, reason: str) -> int:
    if name not in KNOWN_PARTS:
        raise SystemExit(f"Unknown part: {name}")
    _write_flag(_part_reload_flag(name), reason)
    print(f"reload requested for {name}")
    return 0


def cmd_reload_hydrators(reason: str) -> int:
    for name in HYDRATOR_PARTS:
        _write_flag(_part_reload_flag(name), reason)
    print("reload requested for hydrator parts")
    return 0


def cmd_reload_supervisor(reason: str) -> int:
    _write_flag(SUPERVISOR_RELOAD_FLAG, reason)
    print("reload requested for supervisor")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Set or clear supervisor maintenance-disable and reload flags.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status")

    p_disable = sub.add_parser("disable")
    p_disable.add_argument("part")
    p_disable.add_argument("reason", nargs="?", default="")

    p_enable = sub.add_parser("enable")
    p_enable.add_argument("part")

    p_disable_all = sub.add_parser("disable-all")
    p_disable_all.add_argument("reason", nargs="?", default="")

    sub.add_parser("enable-all")

    p_reload = sub.add_parser("reload")
    p_reload.add_argument("part")
    p_reload.add_argument("reason", nargs="?", default="")

    p_reload_hydrators = sub.add_parser("reload-hydrators")
    p_reload_hydrators.add_argument("reason", nargs="?", default="")

    p_reload_supervisor = sub.add_parser("reload-supervisor")
    p_reload_supervisor.add_argument("reason", nargs="?", default="")

    args = parser.parse_args()
    if args.command == "status":
        return cmd_status()
    if args.command == "disable":
        return cmd_disable(args.part, args.reason)
    if args.command == "enable":
        return cmd_enable(args.part)
    if args.command == "disable-all":
        return cmd_disable_all(args.reason)
    if args.command == "enable-all":
        return cmd_enable_all()
    if args.command == "reload":
        return cmd_reload(args.part, args.reason)
    if args.command == "reload-hydrators":
        return cmd_reload_hydrators(args.reason)
    if args.command == "reload-supervisor":
        return cmd_reload_supervisor(args.reason)
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
