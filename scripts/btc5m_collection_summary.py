"""Print an operational summary for BTC5M live data collection."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import resolve_db_path, resolve_repo_path
from common.btc5m_ops_status import (
    classify_uptime_ratio,
    collector_has_recent_error,
    latest_operational_audit_window,
)
from common.single_instance import _is_pid_alive

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

STATUS_PATH = resolve_repo_path(
    os.getenv("BTC5M_HEALTH_STATUS_PATH"),
    default_path=ROOT_DIR / "runtime" / "monitoring" / "btc5m_collection_health.json",
)
BACKUP_DIR = resolve_repo_path(
    os.getenv("BTC5M_BACKUP_DIR"),
    default_path=ROOT_DIR / "runtime" / "backups",
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
MAX_REFERENCE_AGE_SEC = max(2, int(os.getenv("BTC5M_HEALTH_MAX_REFERENCE_AGE_SEC", "10")))
MAX_AUDIT_AGE_SEC = max(60, int(os.getenv("BTC5M_HEALTH_MAX_AUDIT_AGE_SEC", "1800")))
BACKUP_INTERVAL_HOURS = max(1, int(os.getenv("BTC5M_BACKUP_INTERVAL_HOURS", "6")))
BACKUP_STALE_GRACE_SEC = max(0, int(os.getenv("BTC5M_BACKUP_STALE_GRACE_SEC", "3600")))
MAX_BACKUP_AGE_SEC = max(
    300,
    int(
        os.getenv(
            "BTC5M_SUMMARY_MAX_BACKUP_AGE_SEC",
            str((BACKUP_INTERVAL_HOURS * 3600) + BACKUP_STALE_GRACE_SEC),
        )
    ),
)
OPERATIONAL_AUDIT_WINDOW_MARKETS = max(3, int(os.getenv("BTC5M_OPERATIONAL_AUDIT_WINDOW_MARKETS", "12")))
RECENT_COLLECTOR_ERROR_WINDOW_SEC = max(60, int(os.getenv("BTC5M_SUMMARY_RECENT_COLLECTOR_ERROR_WINDOW_SEC", "900")))

COLLECTOR_CONFIG = {
    "scanner": {
        "collector_name": "btc5m-clob-scanner",
        "lock_path": SCANNER_LOCK,
    },
    "reference": {
        "collector_name": "btc5m-reference-collector",
        "lock_path": REFERENCE_LOCK,
    },
    "resolution": {
        "collector_name": "btc5m-resolution-collector",
        "lock_path": RESOLUTION_LOCK,
    },
    "audit": {
        "collector_name": "btc5m-dataset-audit",
        "lock_path": None,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show a BTC5M live collection summary.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    return parser.parse_args()


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


def process_running(lock_path: Optional[Path]) -> tuple[bool, int | None]:
    if lock_path is None:
        return False, None
    pid = read_lock_pid(lock_path)
    if not pid:
        return False, None
    return bool(_is_pid_alive(int(pid))), int(pid)


def safe_age(now_ts: int, ts_value: Any) -> int | None:
    if ts_value is None:
        return None
    try:
        return max(0, int(now_ts) - int(ts_value))
    except Exception:
        return None


def format_ts(ts_value: Any) -> str:
    if ts_value in (None, ""):
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts_value)))
    except Exception:
        return str(ts_value)


def format_age(age_sec: Any) -> str:
    if age_sec is None:
        return "-"
    total = int(age_sec)
    if total < 60:
        return f"{total}s"
    minutes, seconds = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def format_ratio(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value):.3f}"


def format_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value):.1f}%"


def latest_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    return row[0]


def latest_collector_run(conn: sqlite3.Connection, collector_name: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT run_id, status, started_ts, ended_ts, snapshot_count, market_count, "
        "reference_tick_count, error_count, meta_json "
        "FROM collector_runs WHERE collector_name=? ORDER BY started_ts DESC LIMIT 1",
        (collector_name,),
    ).fetchone()
    return dict(row) if row else None


def latest_audit_summary(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT run_id, audit_ts, audit_status, notes, slot_coverage_ratio, max_gap_sec, invalid_book_ratio, "
        "semantic_reject_ratio, duplicate_snapshot_ratio, missing_reference_ratio, "
        "missing_resolution_flag, reference_sync_gap_sec "
        "FROM quality_audits WHERE market_id IS NULL ORDER BY audit_ts DESC, audit_id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def latest_backup_info(now_ts: int) -> dict[str, Any]:
    backups = sorted(BACKUP_DIR.glob("btc5m_dataset_*.db"), key=lambda item: item.stat().st_mtime, reverse=True)
    if not backups:
        return {
            "exists": False,
            "path": None,
            "name": None,
            "size_bytes": None,
            "last_write_ts": None,
            "age_sec": None,
        }
    latest = backups[0]
    stat = latest.stat()
    last_write_ts = int(stat.st_mtime)
    return {
        "exists": True,
        "path": str(latest),
        "name": latest.name,
        "size_bytes": int(stat.st_size),
        "last_write_ts": last_write_ts,
        "age_sec": safe_age(now_ts, last_write_ts),
    }


def read_health_status(now_ts: int) -> dict[str, Any]:
    if not STATUS_PATH.exists():
        return {
            "exists": False,
            "path": str(STATUS_PATH),
            "checked_ts": None,
            "age_sec": None,
            "issues": [],
            "warnings": [],
        }
    try:
        payload = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "exists": True,
            "path": str(STATUS_PATH),
            "checked_ts": None,
            "age_sec": None,
            "issues": [f"health_status_parse_failed:{exc}"],
            "warnings": [],
        }
    checked_ts = payload.get("checked_ts")
    return {
        "exists": True,
        "path": str(STATUS_PATH),
        "checked_ts": checked_ts,
        "age_sec": safe_age(now_ts, checked_ts),
        "issues": list(payload.get("issues") or []),
        "warnings": list(payload.get("warnings") or []),
    }


def table_count(conn: sqlite3.Connection, table_name: str) -> int:
    value = latest_scalar(conn, f"SELECT COUNT(*) FROM {table_name}")
    return int(value or 0)


def build_summary() -> dict[str, Any]:
    now_ts = int(time.time())
    db_path = resolve_db_path()
    summary: dict[str, Any] = {
        "checked_ts": now_ts,
        "db": {
            "path": str(db_path),
            "exists": db_path.exists(),
            "size_bytes": int(db_path.stat().st_size) if db_path.exists() else None,
        },
        "collectors": {},
        "counts": {},
        "freshness": {
            "snapshot_age_sec": None,
            "reference_age_sec": None,
            "audit_age_sec": None,
            "snapshot_file_age_sec": None,
        },
        "audit": None,
        "operational_audit": None,
        "uptime": None,
        "backup": latest_backup_info(now_ts),
        "health": read_health_status(now_ts),
        "warnings": [],
    }

    if SNAPSHOT_PATH.exists():
        summary["freshness"]["snapshot_file_age_sec"] = safe_age(now_ts, int(SNAPSHOT_PATH.stat().st_mtime))

    for label, config in COLLECTOR_CONFIG.items():
        running, pid = process_running(config["lock_path"])
        summary["collectors"][label] = {
            "running": running,
            "pid": pid,
            "lock_path": str(config["lock_path"]) if config["lock_path"] else None,
            "latest_run": None,
        }

    if not db_path.exists():
        summary["warnings"].append("dataset_db_missing")
        return summary

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        summary["counts"] = {
            "markets": table_count(conn, "btc5m_markets"),
            "snapshots": table_count(conn, "btc5m_snapshots"),
            "reference_ticks": table_count(conn, "btc5m_reference_ticks"),
            "orderbook_depth": table_count(conn, "btc5m_orderbook_depth"),
            "lifecycle_events": table_count(conn, "btc5m_lifecycle_events"),
            "quality_audits": table_count(conn, "quality_audits"),
        }

        latest_snapshot_ts = latest_scalar(conn, "SELECT MAX(collected_ts) FROM btc5m_snapshots")
        latest_reference_ts = latest_scalar(conn, "SELECT MAX(ts_utc) FROM btc5m_reference_ticks")
        audit_summary = latest_audit_summary(conn)
        summary["freshness"]["snapshot_age_sec"] = safe_age(now_ts, latest_snapshot_ts)
        summary["freshness"]["reference_age_sec"] = safe_age(now_ts, latest_reference_ts)
        summary["freshness"]["audit_age_sec"] = safe_age(now_ts, audit_summary["audit_ts"] if audit_summary else None)
        summary["audit"] = audit_summary
        summary["operational_audit"] = latest_operational_audit_window(
            conn,
            window_markets=OPERATIONAL_AUDIT_WINDOW_MARKETS,
        )

        for label, config in COLLECTOR_CONFIG.items():
            run_info = latest_collector_run(conn, config["collector_name"])
            summary["collectors"][label]["latest_run"] = run_info
    finally:
        conn.close()

    aggregate_uptime = classify_uptime_ratio(
        (summary["audit"] or {}).get("slot_coverage_ratio")
    )
    recent_uptime = classify_uptime_ratio(
        (summary["operational_audit"] or {}).get("min_coverage_ratio")
    )
    summary["uptime"] = {
        "aggregate": aggregate_uptime,
        "recent": recent_uptime,
    }

    freshness = summary["freshness"]
    if summary["freshness"]["snapshot_age_sec"] is None:
        summary["warnings"].append("no_snapshot_rows_yet")
    elif summary["freshness"]["snapshot_age_sec"] > MAX_SNAPSHOT_AGE_SEC:
        summary["warnings"].append(f"snapshot_stale:{freshness['snapshot_age_sec']}s")

    if summary["freshness"]["reference_age_sec"] is None:
        summary["warnings"].append("no_reference_rows_yet")
    elif summary["freshness"]["reference_age_sec"] > MAX_REFERENCE_AGE_SEC:
        summary["warnings"].append(f"reference_stale:{freshness['reference_age_sec']}s")

    if summary["freshness"]["audit_age_sec"] is None:
        summary["warnings"].append("no_audit_rows_yet")
    elif summary["freshness"]["audit_age_sec"] > MAX_AUDIT_AGE_SEC:
        summary["warnings"].append(f"audit_stale:{freshness['audit_age_sec']}s")

    for label, collector in summary["collectors"].items():
        if label == "audit":
            continue
        if not collector["running"]:
            summary["warnings"].append(f"{label}_collector_not_running")
        run_info = collector.get("latest_run") or {}
        if collector_has_recent_error(
            run_info,
            now_ts=now_ts,
            recent_window_sec=RECENT_COLLECTOR_ERROR_WINDOW_SEC,
        ):
            summary["warnings"].append(f"{label}_collector_errors:{int(run_info['error_count'])}")

    backup = summary["backup"]
    if not backup["exists"]:
        summary["warnings"].append("backup_missing")
    elif backup["age_sec"] is not None and backup["age_sec"] > MAX_BACKUP_AGE_SEC:
        summary["warnings"].append(f"backup_stale:{backup['age_sec']}s")

    health = summary["health"]
    if health["age_sec"] is not None and health["age_sec"] > MAX_AUDIT_AGE_SEC:
        summary["warnings"].append(f"health_status_stale:{health['age_sec']}s")
    for issue in health["issues"]:
        summary["warnings"].append(f"health_issue:{issue}")
    for warning in health["warnings"]:
        summary["warnings"].append(f"health_warning:{warning}")

    operational_status = None
    if summary["operational_audit"]:
        operational_status = str(summary["operational_audit"].get("status") or "")
    if summary["audit"] and str(summary["audit"].get("audit_status") or "") == "FAIL" and operational_status != "PASS":
        summary["warnings"].append("latest_audit_failed")

    if "latest_audit_failed" in summary["warnings"]:
        summary["warnings"] = [
            item for item in summary["warnings"]
            if item != "health_warning:latest_audit_failed"
        ]
    summary["warnings"] = sorted(set(summary["warnings"]))
    return summary


def print_text_summary(summary: dict[str, Any]) -> None:
    counts = summary["counts"]
    audit = summary["audit"] or {}
    freshness = summary["freshness"]
    backup = summary["backup"]
    health = summary["health"]
    operational_audit = summary["operational_audit"] or {}
    uptime = summary["uptime"] or {}
    aggregate_uptime = uptime.get("aggregate") or {}
    recent_uptime = uptime.get("recent") or {}

    print("BTC5M Collection Summary")
    print(f"Checked: {format_ts(summary['checked_ts'])}")
    print(f"DB: {summary['db']['path']}")
    print(f"DB size: {summary['db']['size_bytes'] or 0} bytes")
    print("")

    print("Collectors")
    for label in ("scanner", "reference", "resolution", "audit"):
        collector = summary["collectors"].get(label) or {}
        run_info = collector.get("latest_run") or {}
        status = "RUNNING" if collector.get("running") else "STOPPED"
        if label == "audit" and run_info:
            status = str(run_info.get("status") or status)
        print(
            f"- {label}: status={status} pid={collector.get('pid') or '-'} "
            f"last_run={format_ts(run_info.get('started_ts'))} errors={run_info.get('error_count') or 0}"
        )

    print("")
    print("DB Counts")
    print(
        f"- markets={counts.get('markets', 0)} snapshots={counts.get('snapshots', 0)} "
        f"reference_ticks={counts.get('reference_ticks', 0)} orderbook_depth={counts.get('orderbook_depth', 0)} "
        f"lifecycle_events={counts.get('lifecycle_events', 0)}"
    )

    print("")
    print("Freshness")
    print(
        f"- snapshot_db={format_age(freshness.get('snapshot_age_sec'))} "
        f"reference_db={format_age(freshness.get('reference_age_sec'))} "
        f"audit={format_age(freshness.get('audit_age_sec'))} "
        f"snapshot_file={format_age(freshness.get('snapshot_file_age_sec'))}"
    )

    print("")
    print("Latest Audit")
    if audit:
        print(
            f"- status={audit.get('audit_status')} coverage={format_ratio(audit.get('slot_coverage_ratio'))} "
            f"max_gap={format_ratio(audit.get('max_gap_sec'))} invalid={format_ratio(audit.get('invalid_book_ratio'))} "
            f"semantic_reject={format_ratio(audit.get('semantic_reject_ratio'))} "
            f"missing_resolution={audit.get('missing_resolution_flag', '-')}"
        )
        print(f"- notes={audit.get('notes') or '-'}")
    else:
        print("- no audit summary yet")

    print("")
    print("Operational Audit")
    if operational_audit:
        print(
            f"- status={operational_audit.get('status')} window={operational_audit.get('window_count')}/"
            f"{operational_audit.get('window_markets')} min_coverage={format_ratio(operational_audit.get('min_coverage_ratio'))} "
            f"avg_coverage={format_ratio(operational_audit.get('avg_coverage_ratio'))} "
            f"max_gap={format_ratio(operational_audit.get('max_gap_sec'))}"
        )
    else:
        print("- no operational audit window yet")

    print("")
    print("Uptime")
    print(
        f"- recent={format_pct(recent_uptime.get('pct'))} band={recent_uptime.get('band') or '-'} "
        f"note={recent_uptime.get('message') or '-'}"
    )
    print(
        f"- aggregate={format_pct(aggregate_uptime.get('pct'))} band={aggregate_uptime.get('band') or '-'} "
        f"note={aggregate_uptime.get('message') or '-'}"
    )

    print("")
    print("Latest Backup")
    if backup["exists"]:
        print(
            f"- file={backup['name']} age={format_age(backup['age_sec'])} "
            f"size={backup['size_bytes']} bytes"
        )
    else:
        print("- no backup found")

    print("")
    print("Health Status")
    print(
        f"- status_file_age={format_age(health.get('age_sec'))} "
        f"issues={len(health.get('issues') or [])} warnings={len(health.get('warnings') or [])}"
    )

    print("")
    print("Warnings")
    warnings = summary.get("warnings") or []
    if not warnings:
        print("- none")
    else:
        for item in warnings:
            print(f"- {item}")


def main() -> None:
    args = parse_args()
    summary = build_summary()
    if args.json:
        print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
        return
    print_text_summary(summary)


if __name__ == "__main__":
    main()
