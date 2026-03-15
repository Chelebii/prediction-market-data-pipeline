import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

try:
    import psutil
except Exception:
    psutil = None

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_SCRIPT = ROOT / "control" / "dashboard" / "server.py"
HOST = "127.0.0.1"
PORT = 8765
WINDOWS_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def pythonw_path() -> str:
    exe = Path(sys.executable)
    pyw = exe.with_name("pythonw.exe")
    return str(pyw if pyw.exists() else exe)


def port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


def process_running(fragment: str) -> bool:
    if not psutil:
        return False
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            name = (proc.info.get("name") or "").lower()
            if "python" not in name:
                continue
            cmdline = " ".join(str(part) for part in (proc.info.get("cmdline") or []) if part)
            if fragment.lower() in cmdline.lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False


def start_hidden(script_path: Path, workdir: Path) -> None:
    subprocess.Popen(
        [pythonw_path(), str(script_path)],
        cwd=str(workdir),
        creationflags=WINDOWS_NO_WINDOW,
    )


def main() -> None:
    if not port_open(HOST, PORT):
        start_hidden(DASHBOARD_SCRIPT, DASHBOARD_SCRIPT.parent.parent.parent)
        time.sleep(1.5)

    webbrowser.open(f"http://{HOST}:{PORT}/")


if __name__ == "__main__":
    main()
