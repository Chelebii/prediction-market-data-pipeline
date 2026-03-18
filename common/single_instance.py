import atexit
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _normalize_windows_path(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return str(Path(value).resolve()).lower()
    except Exception:
        return str(value).strip().lower() or None


def current_process_identity() -> dict:
    exe_path = None
    image_name = None
    try:
        exe_path = sys.executable
    except Exception:
        exe_path = None
    normalized_exe = _normalize_windows_path(exe_path)
    if normalized_exe:
        image_name = Path(normalized_exe).name.lower()
    elif os.name == "nt":
        image_name = "python.exe"
    return {
        "pid": os.getpid(),
        "image_name": image_name,
        "exe_path": normalized_exe,
    }


def read_lock_metadata(lock_path: str) -> dict | None:
    if not os.path.exists(lock_path):
        return None
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            raw = (f.read() or "").strip()
    except Exception:
        return None

    if not raw:
        return None

    if raw.isdigit():
        return {"pid": int(raw)}

    try:
        payload = json.loads(raw)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    normalized: dict = dict(payload)
    try:
        if normalized.get("pid") is not None:
            normalized["pid"] = int(normalized["pid"])
    except Exception:
        normalized.pop("pid", None)

    image_name = normalized.get("image_name")
    if image_name:
        normalized["image_name"] = str(image_name).strip().lower()

    exe_path = normalized.get("exe_path")
    if exe_path:
        normalized["exe_path"] = _normalize_windows_path(str(exe_path))

    return normalized


def _query_windows_process(pid: int) -> dict | None:
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        out = (r.stdout or "").strip()
        if not out or "No tasks are running" in out:
            return None
        first_line = out.splitlines()[0].strip().strip('"')
        cols = [c.strip().strip('"') for c in first_line.split('","')]
        if len(cols) < 2:
            return None
        image_name, pid_col = cols[0].lower(), cols[1]
        if pid_col != str(pid):
            return None

        proc = {"pid": int(pid_col), "image_name": image_name, "exe_path": None}
        try:
            wmi = subprocess.run(
                [
                    "wmic",
                    "process",
                    "where",
                    f"processid={pid}",
                    "get",
                    "ExecutablePath",
                    "/value",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in (wmi.stdout or "").splitlines():
                line = line.strip()
                if line.lower().startswith("executablepath="):
                    proc["exe_path"] = _normalize_windows_path(line.split("=", 1)[1].strip())
                    break
        except Exception:
            pass
        return proc
    except Exception:
        return None


def _is_pid_alive(pid: int, expected_image_name: str | None = None, expected_exe_path: str | None = None) -> bool:
    try:
        if os.name == "nt":
            proc = _query_windows_process(pid)
            if not proc:
                return False
            if expected_image_name and proc.get("image_name") != str(expected_image_name).strip().lower():
                return False
            normalized_expected_exe = _normalize_windows_path(expected_exe_path)
            if normalized_expected_exe and proc.get("exe_path"):
                return proc.get("exe_path") == normalized_expected_exe
            return True
        else:
            os.kill(pid, 0)
            return True
    except Exception:
        return False


def _kill_pid(pid: int, on_log=None):
    """Kill a process by PID. Returns True if killed or already dead."""
    if not _is_pid_alive(pid):
        return True
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=10)
        else:
            import signal
            os.kill(pid, signal.SIGTERM)
        import time
        time.sleep(1)
        if _is_pid_alive(pid):
            if on_log:
                on_log(f"PID {pid} did not exit after kill, force retry")
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, timeout=10)
            import time
            time.sleep(1)
        return not _is_pid_alive(pid)
    except Exception as e:
        if on_log:
            on_log(f"Kill error for PID {pid}: {e}")
        return False


def is_lock_process_alive(lock_path: str) -> tuple[bool, int | None, dict | None]:
    metadata = read_lock_metadata(lock_path)
    if not metadata:
        return False, None, None
    pid = metadata.get("pid")
    if not pid:
        return False, None, metadata
    running = _is_pid_alive(
        int(pid),
        expected_image_name=metadata.get("image_name"),
        expected_exe_path=metadata.get("exe_path"),
    )
    return running, int(pid), metadata


def acquire_single_instance_lock(lock_path: str, process_name: str, on_log=None, exit_on_running: bool = True, takeover: bool = False):
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            metadata = read_lock_metadata(lock_path) or {}
            old_pid = metadata.get("pid")
            if old_pid and _is_pid_alive(
                int(old_pid),
                expected_image_name=metadata.get("image_name"),
                expected_exe_path=metadata.get("exe_path"),
            ):
                if takeover:
                    if on_log:
                        on_log(f"Takeover: killing old instance (PID {old_pid})")
                    _kill_pid(old_pid, on_log=on_log)
                else:
                    if on_log:
                        on_log(f"Already running (PID {old_pid}). Exiting.")
                    if exit_on_running:
                        sys.exit(0)
                    return None

            try:
                os.remove(lock_path)
                if on_log:
                    on_log(f"Stale lock cleaned: {lock_path}")
            except Exception:
                if on_log:
                    on_log(f"Lock busy, cannot clean now: {lock_path}")
                sys.exit(1)

    identity = current_process_identity()
    payload = {
        "pid": identity["pid"],
        "name": process_name,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "image_name": identity["image_name"],
        "exe_path": identity["exe_path"],
    }
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    def _release():
        try:
            if os.path.exists(lock_path):
                data = read_lock_metadata(lock_path) or {}
                if int(data.get("pid", -1)) == os.getpid():
                    os.remove(lock_path)
        except Exception:
            pass

    atexit.register(_release)
    return _release
