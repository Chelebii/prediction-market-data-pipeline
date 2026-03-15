"""5MIN Bot Manager -- auto-restart on crash."""
import json
import os, sys, time, subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv, dotenv_values

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from common.single_instance import acquire_single_instance_lock
from common.bot_notify import send_bot_event
from common.run_registry import get_active_run, get_status, set_active_run, set_status, touch_heartbeat, set_trading_mode

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNS_DIR = os.path.join(BASE_DIR, "runs")
BOT_SCRIPT = "polymarket_paper_bot.py"
LOCK_FILE = os.path.join(BASE_DIR, "manager.lock")
ENV_PATH = os.path.join(BASE_DIR, ".env")
STARTED_NOTICE_STATE_PATH = os.path.join(BASE_DIR, "monitoring", "started_notice_state.json")
load_dotenv(os.path.join(BASE_DIR, ".env"))
WINDOWS_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
WINDOWS_NEW_CONSOLE = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] [5MIN-MGR] {msg}", flush=True)

def is_pid_alive(pid):
    try:
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
                           capture_output=True, text=True, timeout=5)
        return str(pid) in r.stdout
    except: return False

def acquire_lock():
    acquire_single_instance_lock(LOCK_FILE, process_name="5MIN-manager", on_log=log, takeover=True)

def ensure_runs_dir():
    os.makedirs(RUNS_DIR, exist_ok=True)


def load_started_notice_state() -> dict:
    try:
        with open(STARTED_NOTICE_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_started_notice_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STARTED_NOTICE_STATE_PATH), exist_ok=True)
        with open(STARTED_NOTICE_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def should_send_started_notice(run_name: str, trading_mode: str) -> bool:
    state = load_started_notice_state()
    if state.get("run_name") == run_name and state.get("trading_mode") == trading_mode:
        return False
    save_started_notice_state(
        {
            "run_name": run_name,
            "trading_mode": trading_mode,
            "sent_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return True


def read_env_value(key: str, default: str) -> str:
    try:
        values = dotenv_values(ENV_PATH)
        raw = values.get(key)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip()
    except Exception:
        pass
    return str(os.getenv(key, default)).strip()


def run_start_balance() -> float:
    try:
        return float(read_env_value("INITIAL_BALANCE", "1000"))
    except Exception:
        return 1000.0


def get_latest_active_run():
    runs = [os.path.join(RUNS_DIR, d) for d in os.listdir(RUNS_DIR) if d.startswith("Run_") and os.path.isdir(os.path.join(RUNS_DIR, d))]
    runs = sorted(runs, key=lambda p: os.path.getmtime(p), reverse=True)
    for run_path in runs:
        if not os.path.exists(os.path.join(run_path, "GAME_OVER.txt")):
            return run_path
    return None


def create_new_run():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_name = f"Run_{timestamp}"
    run_path = os.path.join(RUNS_DIR, run_name)
    os.makedirs(run_path, exist_ok=True)
    with open(os.path.join(run_path, "CURRENT_BALANCE.txt"), "w", encoding="utf-8") as f:
        f.write(str(run_start_balance()))
    return run_path


def run_bot(run_path):
    run_name = os.path.basename(run_path)
    trading_mode = read_env_value("TRADING_MODE", "paper").lower()
    set_trading_mode("5min", trading_mode)
    # 5MIN icin Telegram'da STARTED / STOPPED spamini istemiyoruz.
    # Trade open/close ve anlamli hata bildirimleri ayrica gonderiliyor.
    should_send_started_notice(run_name, trading_mode)

    env = os.environ.copy()
    # Refresh child process config from disk so restart picks up the latest .env values.
    for key, value in dotenv_values(ENV_PATH).items():
        if value is not None:
            env[str(key)] = str(value)
    env["BOT_LABEL"] = "5MIN"
    st = get_status("5min")
    env["ALLOW_NEW_ENTRIES"] = "0" if st == "DRAINING" else "1"
    if st == "DRAINING":
        log(f"DRAINING mode active for {run_name}: new entries disabled")

    process = subprocess.Popen(
        [sys.executable, BOT_SCRIPT, "--run-dir", run_path],
        cwd=BASE_DIR,
        env=env,
        creationflags=WINDOWS_NEW_CONSOLE,
    )
    while True:
        rc = process.poll()
        if rc is not None:
            return rc, trading_mode
        touch_heartbeat("5min")
        time.sleep(15)


def main():
    acquire_lock()
    ensure_runs_dir()
    restart_count = 0
    bot_label = "5MIN"
    fatal_exit_map = {
        88: "BANKRUPT",
        89: "MAX_DRAWDOWN",
        90: "LIVE_BLOCKED",
    }

    while True:
        run_path = get_active_run("5min")
        if not run_path or (os.path.exists(os.path.join(run_path, "GAME_OVER.txt"))):
            run_path = get_latest_active_run() or create_new_run()
        st = get_status("5min")
        if st == "ARCHIVED":
            set_status("5min", "CREATED")
            st = "CREATED"
        target_status = st if st in {"ACTIVE", "DRAINING"} else "ACTIVE"
        set_active_run("5min", run_path, status=target_status)
        touch_heartbeat("5min")
        run_name = os.path.basename(run_path)

        log(f"Starting bot (restart #{restart_count}) | {run_name}")
        try:
            exit_code, trading_mode = run_bot(run_path)
        except Exception as e:
            log(f"Popen error: {e}")
            exit_code = 1
            trading_mode = read_env_value("TRADING_MODE", "paper").lower()

        if exit_code in fatal_exit_map:
            fatal_reason = fatal_exit_map[exit_code]
            log(f"{fatal_reason}! Manager stopping without restart.")
            send_bot_event(bot_label, "DEAD", f"code={exit_code}", level="ERROR")
            if trading_mode == "live" and exit_code in {88, 89}:
                try:
                    from common.safety import SafetyManager
                    SafetyManager(bot_label=bot_label).activate_kill_switch(f"{bot_label} {fatal_reason} (code={exit_code})")
                except Exception:
                    pass
            with open(os.path.join(run_path, "GAME_OVER.txt"), "w", encoding="utf-8") as f:
                f.write(f"Status: {fatal_reason}")
            return
        elif exit_code == 0:
            log("Clean exit. Restarting in 5s...")
            time.sleep(5)
        else:
            log(f"Crash (code {exit_code}). Restarting in 5s...")
            time.sleep(5)

        restart_count += 1

if __name__ == "__main__":
    main()
