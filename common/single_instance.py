import atexit
import json
import os
import subprocess
import sys
from datetime import datetime


def _is_pid_alive(pid: int) -> bool:
    try:
        if os.name == "nt":
            r = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            out = (r.stdout or "").strip()
            if not out or "No tasks are running" in out:
                return False
            # CSV row: "Image Name","PID",...
            first_line = out.splitlines()[0].strip().strip('"')
            cols = [c.strip().strip('"') for c in first_line.split('","')]
            if len(cols) < 2:
                return False
            image_name, pid_col = cols[0].lower(), cols[1]
            return pid_col == str(pid) and image_name == "python.exe"
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


def acquire_single_instance_lock(lock_path: str, process_name: str, on_log=None, exit_on_running: bool = True, takeover: bool = False):
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            old_pid = None
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                    old_pid = int(payload.get("pid", 0))
            except Exception:
                try:
                    with open(lock_path, "r", encoding="utf-8") as f:
                        old_pid = int((f.read() or "0").strip())
                except Exception:
                    old_pid = None

            if old_pid and _is_pid_alive(old_pid):
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

    payload = {
        "pid": os.getpid(),
        "name": process_name,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    def _release():
        try:
            if os.path.exists(lock_path):
                with open(lock_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if int(data.get("pid", -1)) == os.getpid():
                    os.remove(lock_path)
        except Exception:
            pass

    atexit.register(_release)
    return _release
