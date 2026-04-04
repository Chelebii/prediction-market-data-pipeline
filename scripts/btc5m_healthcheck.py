"""Health checks for unattended BTC5M live data collection."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import resolve_db_path, resolve_repo_path
from common.btc5m_ops_status import latest_operational_audit_window, operational_audit_is_material_failure
from common.bot_notify import send_alert
from common.single_instance import is_lock_process_alive, read_lock_metadata

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

HEALTH_LOG_PATH = resolve_repo_path(
    os.getenv("BTC5M_HEALTH_LOG_PATH"),
    default_path=ROOT_DIR / "runtime" / "logs" / "btc5m_healthcheck.log",
)
STATUS_PATH = resolve_repo_path(
    os.getenv("BTC5M_HEALTH_STATUS_PATH"),
    default_path=ROOT_DIR / "runtime" / "monitoring" / "btc5m_collection_health.json",
)
SNAPSHOT_PATH = resolve_repo_path(
    os.getenv("BTC_5MIN_SNAPSHOT_PATH"),
    default_path=ROOT_DIR / "runtime" / "snapshots" / "btc_5min_clob_snapshot.json",
)
SCANNER_LOCK = resolve_repo_path(
    ROOT_DIR / "polymarket_scanner" / "btc_5min_clob_scanner.lock",
    default_path=ROOT_DIR / "polymarket_scanner" / "btc_5min_clob_scanner.lock",
)
REFERENCE_LOCK = resolve_repo_path(
    os.getenv("BTC5M_REFERENCE_LOCK_PATH"),
    default_path=ROOT_DIR / "runtime" / "locks" / "btc5m_reference_collector.lock",
)
RESOLUTION_LOCK = resolve_repo_path(
    os.getenv("BTC5M_RESOLUTION_LOCK_PATH"),
    default_path=ROOT_DIR / "runtime" / "locks" / "btc5m_resolution_collector.lock",
)
MAX_SNAPSHOT_AGE_SEC = max(5, int(os.getenv("BTC5M_HEALTH_MAX_SNAPSHOT_AGE_SEC", "45")))
MAX_REFERENCE_AGE_SEC = max(2, int(os.getenv("BTC5M_HEALTH_MAX_REFERENCE_AGE_SEC", "30")))
MAX_AUDIT_AGE_SEC = max(60, int(os.getenv("BTC5M_HEALTH_MAX_AUDIT_AGE_SEC", "1800")))
ALERT_DEDUPE_SEC = max(60, int(os.getenv("BTC5M_HEALTH_ALERT_DEDUPE_SEC", "1800")))
STARTUP_GRACE_SEC = max(0, int(os.getenv("BTC5M_HEALTH_STARTUP_GRACE_SEC", "900")))
OPERATIONAL_AUDIT_WINDOW_MARKETS = max(3, int(os.getenv("BTC5M_OPERATIONAL_AUDIT_WINDOW_MARKETS", "12")))
BOT_LABEL = "BTC5M-DATA"

LOGGER = logging.getLogger("btc5m_healthcheck")
LOGGER.setLevel(logging.INFO)
LOGGER.handlers.clear()
HEALTH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-HEALTH | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
LOGGER.addHandler(_console)
_file_handler = RotatingFileHandler(HEALTH_LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-HEALTH | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
LOGGER.addHandler(_file_handler)


def log(message: str) -> None:
    LOGGER.info(message)


def process_running(lock_path: Path) -> tuple[bool, int | None, dict | None]:
    return is_lock_process_alive(str(lock_path))


def find_running_process(
    *,
    command_fragment: str,
    expected_image_name: str | None = None,
    expected_exe_path: str | None = None,
) -> tuple[bool, int | None, dict | None]:
    if os.name != "nt":
        return False, None, None
    ignored_images = {"powershell.exe", "pwsh.exe", "cmd.exe", "wscript.exe", "cscript.exe"}

    def _ps_literal(value: str) -> str:
        return value.replace("'", "''")

    filter_parts = [
        "$_.CommandLine",
        f"$_.CommandLine.ToLowerInvariant().Contains('{_ps_literal(command_fragment.lower())}')",
    ]
    if expected_image_name:
        filter_parts.append(f"$_.Name.ToLowerInvariant() -eq '{_ps_literal(expected_image_name.lower())}'")
    if expected_exe_path:
        normalized_exe = str(Path(expected_exe_path).resolve()).lower()
        filter_parts.append(
            "(-not $_.ExecutablePath -or "
            f"[System.IO.Path]::GetFullPath($_.ExecutablePath).ToLowerInvariant() -eq '{_ps_literal(normalized_exe)}')"
        )

    ps_script = (
        "$proc = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
        f"Where-Object {{ {' -and '.join(filter_parts)} }} | "
        "Select-Object -First 1 ProcessId,Name,ExecutablePath; "
        "if ($proc) { $proc | ConvertTo-Json -Compress }"
    )

    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        raw = (result.stdout or "").strip()
        if not raw:
            return False, None, None
        payload = json.loads(raw)
        pid = payload.get("ProcessId")
        if pid is None:
            return False, None, None
        image_name = str(payload.get("Name") or "").strip().lower() or None
        if image_name in ignored_images:
            return False, None, None
        meta = {
            "pid": int(pid),
            "image_name": image_name,
            "exe_path": str(payload.get("ExecutablePath") or "").strip().lower() or None,
        }
        return True, int(pid), meta
    except Exception:
        return False, None, None


def collector_process_meta(lock_meta: Any) -> tuple[str | None, str | None]:
    if not isinstance(lock_meta, dict):
        return None, None
    image_name = lock_meta.get("image_name")
    exe_path = lock_meta.get("exe_path")
    return (
        str(image_name) if image_name else None,
        str(exe_path) if exe_path else None,
    )


def lock_started_ts(lock_meta: Any) -> int | None:
    if not isinstance(lock_meta, dict):
        return None
    started_at = lock_meta.get("started_at")
    if not started_at:
        return None
    try:
        return int(datetime.fromisoformat(str(started_at)).timestamp())
    except Exception:
        return None


def latest_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    if isinstance(row, sqlite3.Row):
        return row[0]
    return row[0]


def safe_age(now_ts: int, ts_value: Any) -> int | None:
    if ts_value is None:
        return None
    try:
        return max(0, int(now_ts) - int(ts_value))
    except Exception:
        return None


def latest_audit_summary(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT run_id, audit_ts, audit_status, notes, slot_coverage_ratio, max_gap_sec "
        "FROM quality_audits WHERE market_id IS NULL ORDER BY audit_ts DESC, audit_id DESC LIMIT 1"
    ).fetchone()


def latest_collector_started_ts(conn: sqlite3.Connection, collector_name: str) -> int | None:
    value = latest_scalar(
        conn,
        "SELECT started_ts FROM collector_runs WHERE collector_name=? ORDER BY started_ts DESC LIMIT 1",
        (collector_name,),
    )
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def first_run_started_ts(conn: sqlite3.Connection) -> int | None:
    value = latest_scalar(conn, "SELECT MIN(started_ts) FROM collector_runs")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def active_collector_started_ts(running: bool, lock_meta: Any, db_started_ts: int | None) -> int | None:
    if running:
        active_started_ts = lock_started_ts(lock_meta)
        if active_started_ts is not None:
            return active_started_ts
    return db_started_ts


def build_status() -> tuple[dict[str, Any], list[str]]:
    now_ts = int(time.time())
    db_path = resolve_db_path()
    issues: list[str] = []

    scanner_running, scanner_pid, scanner_lock_meta = process_running(SCANNER_LOCK)
    reference_running, reference_pid, reference_lock_meta = process_running(REFERENCE_LOCK)
    resolution_running, resolution_pid, resolution_lock_meta = process_running(RESOLUTION_LOCK)
    scanner_image_name, scanner_exe_path = collector_process_meta(
        scanner_lock_meta or read_lock_metadata(str(SCANNER_LOCK))
    )
    reference_image_name, reference_exe_path = collector_process_meta(
        reference_lock_meta or read_lock_metadata(str(REFERENCE_LOCK))
    )
    resolution_image_name, resolution_exe_path = collector_process_meta(
        resolution_lock_meta or read_lock_metadata(str(RESOLUTION_LOCK))
    )

    if not scanner_running:
        scanner_running, scanner_pid, scanner_fallback_meta = find_running_process(
            command_fragment="btc_5min_clob_scanner.py",
            expected_image_name=scanner_image_name,
            expected_exe_path=scanner_exe_path,
        )
        if scanner_running and scanner_fallback_meta:
            scanner_lock_meta = scanner_lock_meta or scanner_fallback_meta
            scanner_image_name = scanner_fallback_meta.get("image_name") or scanner_image_name
            scanner_exe_path = scanner_fallback_meta.get("exe_path") or scanner_exe_path

    if not reference_running:
        reference_running, reference_pid, reference_fallback_meta = find_running_process(
            command_fragment="btc5m_reference_collector.py",
            expected_image_name=reference_image_name,
            expected_exe_path=reference_exe_path,
        )
        if reference_running and reference_fallback_meta:
            reference_lock_meta = reference_lock_meta or reference_fallback_meta
            reference_image_name = reference_fallback_meta.get("image_name") or reference_image_name
            reference_exe_path = reference_fallback_meta.get("exe_path") or reference_exe_path

    if not resolution_running:
        resolution_running, resolution_pid, resolution_fallback_meta = find_running_process(
            command_fragment="btc5m_resolution_collector.py",
            expected_image_name=resolution_image_name,
            expected_exe_path=resolution_exe_path,
        )
        if resolution_running and resolution_fallback_meta:
            resolution_lock_meta = resolution_lock_meta or resolution_fallback_meta
            resolution_image_name = resolution_fallback_meta.get("image_name") or resolution_image_name
            resolution_exe_path = resolution_fallback_meta.get("exe_path") or resolution_exe_path

    snapshot_file_age = None
    if SNAPSHOT_PATH.exists():
        try:
            snapshot_file_age = max(0, int(now_ts - int(SNAPSHOT_PATH.stat().st_mtime)))
        except OSError:
            snapshot_file_age = None

    status: dict[str, Any] = {
        "checked_ts": now_ts,
        "db_path": str(db_path),
        "snapshot_path": str(SNAPSHOT_PATH),
        "scanner": {
            "running": scanner_running,
            "pid": scanner_pid,
            "lock_path": str(SCANNER_LOCK),
            "process_image_name": scanner_image_name,
            "process_exe_path": scanner_exe_path,
            "lock_meta": scanner_lock_meta or read_lock_metadata(str(SCANNER_LOCK)),
        },
        "reference": {
            "running": reference_running,
            "pid": reference_pid,
            "lock_path": str(REFERENCE_LOCK),
            "process_image_name": reference_image_name,
            "process_exe_path": reference_exe_path,
            "lock_meta": reference_lock_meta or read_lock_metadata(str(REFERENCE_LOCK)),
        },
        "resolution": {
            "running": resolution_running,
            "pid": resolution_pid,
            "lock_path": str(RESOLUTION_LOCK),
            "process_image_name": resolution_image_name,
            "process_exe_path": resolution_exe_path,
            "lock_meta": resolution_lock_meta or read_lock_metadata(str(RESOLUTION_LOCK)),
        },
        "snapshot_file_age_sec": snapshot_file_age,
        "latest_snapshot_age_sec": None,
        "latest_reference_age_sec": None,
        "latest_audit_age_sec": None,
        "latest_audit_status": None,
        "latest_audit_notes": None,
        "operational_audit": None,
        "issues": [],
    }

    if not scanner_running:
        issues.append("scanner_process_not_running")
    if not reference_running:
        issues.append("reference_process_not_running")
    if not resolution_running:
        issues.append("resolution_process_not_running")

    if not db_path.exists():
        issues.append("dataset_db_missing")
        status["issues"] = issues
        return status, issues

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        latest_snapshot_ts = latest_scalar(conn, "SELECT MAX(collected_ts) FROM btc5m_snapshots")
        latest_reference_ts = latest_scalar(conn, "SELECT MAX(ts_utc) FROM btc5m_reference_ticks")
        audit_row = latest_audit_summary(conn)
        first_started_ts = first_run_started_ts(conn)
        scanner_started_ts = latest_collector_started_ts(conn, "btc5m-clob-scanner")
        reference_started_ts = latest_collector_started_ts(conn, "btc5m-reference-collector")
        resolution_started_ts = latest_collector_started_ts(conn, "btc5m-resolution-collector")

        active_started_ts_candidates = [
            active_collector_started_ts(scanner_running, status["scanner"]["lock_meta"], scanner_started_ts),
            active_collector_started_ts(reference_running, status["reference"]["lock_meta"], reference_started_ts),
            active_collector_started_ts(resolution_running, status["resolution"]["lock_meta"], resolution_started_ts),
        ]
        active_started_ts_candidates = [value for value in active_started_ts_candidates if value is not None]
        active_started_ts = max(active_started_ts_candidates) if active_started_ts_candidates else None

        latest_snapshot_age = safe_age(now_ts, latest_snapshot_ts)
        latest_reference_age = safe_age(now_ts, latest_reference_ts)
        latest_audit_age = safe_age(now_ts, audit_row["audit_ts"] if audit_row else None)
        startup_age = safe_age(now_ts, active_started_ts)
        historical_first_run_age = safe_age(now_ts, first_started_ts)
        startup_grace_active = startup_age is not None and startup_age < STARTUP_GRACE_SEC

        status["latest_snapshot_age_sec"] = latest_snapshot_age
        status["latest_reference_age_sec"] = latest_reference_age
        status["latest_audit_age_sec"] = latest_audit_age
        status["startup_started_ts"] = active_started_ts
        status["startup_age_sec"] = startup_age
        status["historical_first_run_started_ts"] = first_started_ts
        status["historical_first_run_age_sec"] = historical_first_run_age
        status["startup_grace_active"] = startup_grace_active

        if audit_row:
            operational_cutoff_ts = max(
                value for value in (scanner_started_ts, reference_started_ts) if value is not None
            ) if any(value is not None for value in (scanner_started_ts, reference_started_ts)) else None
            status["latest_audit_status"] = audit_row["audit_status"]
            status["latest_audit_notes"] = audit_row["notes"]
            status["latest_audit_slot_coverage_ratio"] = audit_row["slot_coverage_ratio"]
            status["latest_audit_max_gap_sec"] = audit_row["max_gap_sec"]
            status["operational_audit_cutoff_ts"] = operational_cutoff_ts
            status["operational_audit"] = latest_operational_audit_window(
                conn,
                window_markets=OPERATIONAL_AUDIT_WINDOW_MARKETS,
                min_slot_start_ts=operational_cutoff_ts,
            )

        if latest_snapshot_age is None:
            issues.append("no_snapshot_rows_yet")
        elif latest_snapshot_age > MAX_SNAPSHOT_AGE_SEC:
            issues.append(f"snapshot_stale:{latest_snapshot_age}s")

        if latest_reference_age is None:
            issues.append("no_reference_rows_yet")
        elif latest_reference_age > MAX_REFERENCE_AGE_SEC:
            issues.append(f"reference_stale:{latest_reference_age}s")

        if latest_audit_age is None and not startup_grace_active:
            issues.append("no_audit_rows_yet")
        elif latest_audit_age is not None and latest_audit_age > MAX_AUDIT_AGE_SEC:
            issues.append(f"audit_stale:{latest_audit_age}s")
        status["warnings"] = []
        operational_status = None
        if status.get("operational_audit"):
            operational_status = str(status["operational_audit"].get("status") or "")
        if audit_row and str(audit_row["audit_status"] or "") == "FAIL" and operational_audit_is_material_failure(status.get("operational_audit")):
            status["warnings"].append("latest_audit_failed")
    finally:
        conn.close()

    status["issues"] = issues
    return status, issues


def main() -> None:
    status, issues = build_status()
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(status, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    if issues:
        message = " | ".join(issues)
        log(f"UNHEALTHY | {message}")
        send_alert(BOT_LABEL, f"BTC5M data collection unhealthy: {message}", level="WARN", dedupe_seconds=ALERT_DEDUPE_SEC)
        raise SystemExit(1)

    log("HEALTHY | scanner/reference/resolution running and dataset fresh")


if __name__ == "__main__":
    main()
