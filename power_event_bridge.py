"""Listen for Windows suspend/resume and toggle supervisor maintenance flags.

This helper is intentionally tiny and dependency-free:
- no pywin32 requirement
- no direct imports from supervisor.py
- no DB/Vault access

It owns a hidden message-only window, receives WM_POWERBROADCAST events, and
drives the existing local control plane:
- on suspend: disable-all so Supervisor cleanly winds the fleet down
- on resume:  enable-all so Supervisor brings the fleet back in order

The helper is owned by Supervisor itself, not as a managed fleet part, so it
survives disable-all and can also perform the wake-up action.
"""

from __future__ import annotations

import ctypes
import subprocess
import sys
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path


SUPERVISOR_DIR = Path(__file__).resolve().parent
SET_MAINTENANCE = SUPERVISOR_DIR / "set_maintenance.py"
PYTHON = sys.executable

WM_DESTROY = 0x0002
WM_NCCREATE = 0x0081
WM_POWERBROADCAST = 0x0218
PBT_APMSUSPEND = 0x0004
PBT_APMRESUMEAUTOMATIC = 0x0012
PBT_APMRESUMESUSPEND = 0x0007
HWND_MESSAGE = -3

LRESULT = ctypes.c_ssize_t
WNDPROC = ctypes.WINFUNCTYPE(
    LRESULT,
    wintypes.HWND,
    wintypes.UINT,
    wintypes.WPARAM,
    wintypes.LPARAM,
)


class WNDCLASSW(ctypes.Structure):
    _fields_ = [
        ("style", wintypes.UINT),
        ("lpfnWndProc", WNDPROC),
        ("cbClsExtra", ctypes.c_int),
        ("cbWndExtra", ctypes.c_int),
        ("hInstance", wintypes.HINSTANCE),
        ("hIcon", ctypes.c_void_p),
        ("hCursor", ctypes.c_void_p),
        ("hbrBackground", ctypes.c_void_p),
        ("lpszMenuName", wintypes.LPCWSTR),
        ("lpszClassName", wintypes.LPCWSTR),
    ]


class MSG(ctypes.Structure):
    _fields_ = [
        ("hwnd", wintypes.HWND),
        ("message", wintypes.UINT),
        ("wParam", wintypes.WPARAM),
        ("lParam", wintypes.LPARAM),
        ("time", wintypes.DWORD),
        ("pt_x", ctypes.c_long),
        ("pt_y", ctypes.c_long),
    ]


user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

user32.DefWindowProcW.argtypes = [
    wintypes.HWND,
    wintypes.UINT,
    wintypes.WPARAM,
    wintypes.LPARAM,
]
user32.DefWindowProcW.restype = LRESULT
user32.RegisterClassW.argtypes = [ctypes.POINTER(WNDCLASSW)]
user32.RegisterClassW.restype = wintypes.ATOM
user32.CreateWindowExW.argtypes = [
    wintypes.DWORD,
    wintypes.LPCWSTR,
    wintypes.LPCWSTR,
    wintypes.DWORD,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    wintypes.HWND,
    wintypes.HMENU,
    wintypes.HINSTANCE,
    wintypes.LPVOID,
]
user32.CreateWindowExW.restype = wintypes.HWND
user32.GetMessageW.argtypes = [ctypes.POINTER(MSG), wintypes.HWND, wintypes.UINT, wintypes.UINT]
user32.GetMessageW.restype = wintypes.BOOL
user32.TranslateMessage.argtypes = [ctypes.POINTER(MSG)]
user32.TranslateMessage.restype = wintypes.BOOL
user32.DispatchMessageW.argtypes = [ctypes.POINTER(MSG)]
user32.DispatchMessageW.restype = LRESULT
user32.PostQuitMessage.argtypes = [ctypes.c_int]
user32.PostQuitMessage.restype = None


_sleep_mode_active = False


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(message: str) -> None:
    print(f"{_stamp()} {message}", flush=True)


def _run_control(*args: str) -> None:
    cmd = [PYTHON, str(SET_MAINTENANCE), *args]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=20,
            cwd=str(SUPERVISOR_DIR),
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"control failed {' '.join(args)} -> {exc!r}")
        return
    detail = (completed.stdout or completed.stderr or "").strip()
    if completed.returncode == 0:
        _log(f"control ok {' '.join(args)} -> {detail or '(no output)'}")
    else:
        _log(f"control rc={completed.returncode} {' '.join(args)} -> {detail or '(no output)'}")


def _handle_suspend() -> None:
    global _sleep_mode_active
    if _sleep_mode_active:
        _log("suspend event repeated; ignoring duplicate")
        return
    _sleep_mode_active = True
    _log("windows suspend detected; disabling supervised fleet")
    _run_control("disable-all", "host sleep pending")


def _handle_resume(reason: str) -> None:
    global _sleep_mode_active
    _sleep_mode_active = False
    _log(f"windows resume detected ({reason}); enabling supervised fleet")
    _run_control("enable-all")


@WNDPROC
def _window_proc(hwnd, msg, w_param, l_param):  # noqa: ANN001
    del hwnd, l_param
    if msg == WM_NCCREATE:
        return 1
    if msg == WM_POWERBROADCAST:
        if w_param == PBT_APMSUSPEND:
            _handle_suspend()
            return 1
        if w_param == PBT_APMRESUMEAUTOMATIC:
            _handle_resume("automatic")
            return 1
        if w_param == PBT_APMRESUMESUSPEND:
            _handle_resume("resume_suspend")
            return 1
        return 1
    if msg == WM_DESTROY:
        user32.PostQuitMessage(0)
        return 0
    return 0


def main() -> int:
    class_name = "SupervisorPowerEventBridge"
    instance = kernel32.GetModuleHandleW(None)
    if not instance:
        raise ctypes.WinError()

    wnd_class = WNDCLASSW()
    wnd_class.lpfnWndProc = _window_proc
    wnd_class.hInstance = instance
    wnd_class.lpszClassName = class_name

    atom = user32.RegisterClassW(ctypes.byref(wnd_class))
    if not atom:
        error = ctypes.get_last_error()
        if error != 1410:  # class already exists
            raise ctypes.WinError(error)

    hwnd = user32.CreateWindowExW(
        0,
        class_name,
        class_name,
        0,
        0,
        0,
        0,
        0,
        wintypes.HWND(HWND_MESSAGE),
        None,
        instance,
        None,
    )
    if not hwnd:
        raise ctypes.WinError()

    _log("power_event_bridge listening for Windows suspend/resume events")

    msg = MSG()
    while True:
        result = user32.GetMessageW(ctypes.byref(msg), None, 0, 0)
        if result == 0:
            break
        if result == -1:
            raise ctypes.WinError()
        user32.TranslateMessage(ctypes.byref(msg))
        user32.DispatchMessageW(ctypes.byref(msg))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
