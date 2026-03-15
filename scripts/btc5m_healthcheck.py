"""Health checks for unattended BTC5M live data collection."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import resolve_db_path
from common.bot_notify import send_alert
from common.single_instance import _is_pid_alive

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

HEALTH_LOG_PATH = Path(os.getenv("BTC5M_HEALTH_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_healthcheck.log"))
STATUS_PATH = Path(os.getenv("BTC5M_HEALTH_STATUS_PATH", ROOT_DIR / "runtime" / "monitoring" / "btc5m_collection_health.json"))
SNAPSHOT_PATH = Path(
    os.getenv("BTC_5MIN_SNAPSHOT_PATH", ROOT_DIR / "runtime" / "snapshots" / "btc_5min_clob_snapshot.json")
)
SCANNER_LOCK = ROOT_DIR / "polymarket_scanner" / "btc_5min_clob_scanner.lock"
REFERENCE_LOCK = Path(os.getenv("BTC5M_REFERENCE_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_reference_collector.lock"))
RESOLUTION_LOCK = Path(os.getenv("BTC5M_RESOLUTION_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_resolution_collector.lock"))
MAX_SNAPSHOT_AGE_SEC = max(5, int(os.getenv("BTC5M_HEALTH_MAX_SNAPSHOT_AGE_SEC", "45")))
MAX_REFERENCE_AGE_SEC = max(2, int(os.getenv("BTC5M_HEALTH_MAX_REFERENCE_AGE_SEC", "10")))
MAX_AUDIT_AGE_SEC = max(60, int(os.getenv("BTC5M_HEALTH_MAX_AUDIT_AGE_SEC", "1800")))
ALERT_DEDUPE_SEC = max(60, int(os.getenv("BTC5M_HEALTH_ALERT_DEDUPE_SEC", "1800")))
STARTUP_GRACE_SEC = max(0, int(os.getenv("BTC5M_HEALTH_STARTUP_GRACE_SEC", "900")))
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


def read_lock_pid(lock_path: Path) -> int | None:
    if not lock_path.exists():
        return None
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        if raw.isdigit():
            return int(raw)
        payload = json.loads(raw)
        pid = payload.get("pid")
        return int(pid) if pid else None
    except Exception:
        return None


def process_running(lock_path: Path) -> tuple[bool, int | None]:
    pid = read_lock_pid(lock_path)
    if not pid:
        return False, None
    return bool(_is_pid_alive(int(pid))), int(pid)


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
        "SELECT audit_ts, audit_status, notes, slot_coverage_ratio, max_gap_sec "
        "FROM quality_audits WHERE market_id IS NULL ORDER BY audit_ts DESC, audit_id DESC LIMIT 1"
    ).fetchone()


def first_run_started_ts(conn: sqlite3.Connection) -> int | None:
    value = latest_scalar(conn, "SELECT MIN(started_ts) FROM collector_runs")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def build_status() -> tuple[dict[str, Any], list[str]]:
    now_ts = int(time.time())
    db_path = resolve_db_path()
    issues: list[str] = []

    scanner_running, scanner_pid = process_running(SCANNER_LOCK)
    reference_running, reference_pid = process_running(REFERENCE_LOCK)
    resolution_running, resolution_pid = process_running(RESOLUTION_LOCK)

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
        "scanner": {"running": scanner_running, "pid": scanner_pid, "lock_path": str(SCANNER_LOCK)},
        "reference": {"running": reference_running, "pid": reference_pid, "lock_path": str(REFERENCE_LOCK)},
        "resolution": {"running": resolution_running, "pid": resolution_pid, "lock_path": str(RESOLUTION_LOCK)},
        "snapshot_file_age_sec": snapshot_file_age,
        "latest_snapshot_age_sec": None,
        "latest_reference_age_sec": None,
        "latest_audit_age_sec": None,
        "latest_audit_status": None,
        "latest_audit_notes": None,
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

        latest_snapshot_age = safe_age(now_ts, latest_snapshot_ts)
        latest_reference_age = safe_age(now_ts, latest_reference_ts)
        latest_audit_age = safe_age(now_ts, audit_row["audit_ts"] if audit_row else None)
        startup_age = safe_age(now_ts, first_started_ts)
        startup_grace_active = startup_age is not None and startup_age < STARTUP_GRACE_SEC

        status["latest_snapshot_age_sec"] = latest_snapshot_age
        status["latest_reference_age_sec"] = latest_reference_age
        status["latest_audit_age_sec"] = latest_audit_age
        status["startup_age_sec"] = startup_age
        status["startup_grace_active"] = startup_grace_active

        if audit_row:
            status["latest_audit_status"] = audit_row["audit_status"]
            status["latest_audit_notes"] = audit_row["notes"]
            status["latest_audit_slot_coverage_ratio"] = audit_row["slot_coverage_ratio"]
            status["latest_audit_max_gap_sec"] = audit_row["max_gap_sec"]

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
        if audit_row and str(audit_row["audit_status"] or "") == "FAIL":
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
