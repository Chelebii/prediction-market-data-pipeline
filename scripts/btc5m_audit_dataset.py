"""Audit BTC5M dataset quality metrics and write results to SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import statistics
import sys
import time
from bisect import bisect_left
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import (
    connect_db,
    finish_collector_run,
    insert_quality_audit,
    resolve_db_path,
    resolve_repo_path,
    start_collector_run,
    update_collector_run,
)
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-dataset-audit"
COLLECTOR_VERSION = "2026-03-15"
SCAN_INTERVAL_SEC = max(1, int(os.getenv("BTC_5MIN_SCAN_INTERVAL_SEC", "3")))
REFERENCE_TOLERANCE_SEC = max(0, int(os.getenv("BTC5M_AUDIT_REFERENCE_TOLERANCE_SEC", "1")))
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_AUDIT_LOOKBACK_HOURS", "48")))
PARTIAL_STARTUP_GRACE_SEC = max(5, int(os.getenv("BTC5M_AUDIT_PARTIAL_STARTUP_GRACE_SEC", "15")))
SETTLEMENT_GRACE_SEC = max(60, int(os.getenv("BTC5M_AUDIT_SETTLEMENT_GRACE_SEC", "900")))
LOG_PATH = resolve_repo_path(
    os.getenv("BTC5M_AUDIT_LOG_PATH"),
    default_path=ROOT_DIR / "runtime" / "logs" / "btc5m_audit_dataset.log",
)
LOCK_PATH = resolve_repo_path(
    os.getenv("BTC5M_AUDIT_LOCK_PATH"),
    default_path=ROOT_DIR / "runtime" / "locks" / "btc5m_audit_dataset.lock",
)

THRESHOLDS = {
    "slot_coverage_ratio": 0.90,
    "max_gap_sec": 10.0,
    "duplicate_snapshot_ratio": 0.01,
    "invalid_book_ratio": 0.20,
    "reference_sync_gap_sec": 1.0,
}

_logger = logging.getLogger("btc5m_audit_dataset")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-AUDIT | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-AUDIT | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit BTC5M dataset quality metrics.")
    parser.add_argument("--market-slug", type=str, default="", help="Audit a single market slug.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to inspect ended markets.")
    parser.add_argument("--max-markets", type=int, default=250, help="Maximum number of markets to audit.")
    parser.add_argument("--include-active", action="store_true", help="Also audit active markets.")
    return parser.parse_args()


def collector_config_hash(args: argparse.Namespace) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "db_path": str(resolve_db_path()),
        "lookback_hours": int(args.lookback_hours),
        "max_markets": int(args.max_markets),
        "market_slug": str(args.market_slug or ""),
        "include_active": bool(args.include_active),
        "scan_interval_sec": SCAN_INTERVAL_SEC,
        "reference_tolerance_sec": REFERENCE_TOLERANCE_SEC,
        "partial_startup_grace_sec": PARTIAL_STARTUP_GRACE_SEC,
        "settlement_grace_sec": SETTLEMENT_GRACE_SEC,
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_candidate_markets(
    conn: sqlite3.Connection,
    *,
    now_ts: int,
    lookback_hours: int,
    max_markets: int,
    market_slug: str,
    include_active: bool,
) -> list[sqlite3.Row]:
    lower_bound = now_ts - (max(1, lookback_hours) * 3600)
    clauses = ["slot_end_ts >= ?"]
    params: list[Any] = [lower_bound]

    if include_active:
        clauses.append("slot_end_ts <= ?")
        params.append(now_ts + 300)
    else:
        clauses.append("slot_end_ts <= ?")
        params.append(now_ts)

    if market_slug:
        clauses.append("market_slug = ?")
        params.append(market_slug)

    sql = (
        "SELECT market_id, market_slug, slot_start_ts, slot_end_ts, market_status, "
        "market_resolution_status, resolved_outcome, resolved_yes_price, resolved_no_price, resolved_ts, "
        "settled_ts, first_seen_ts, last_seen_ts "
        "FROM btc5m_markets "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY slot_end_ts ASC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def load_snapshot_rows(conn: sqlite3.Connection, market_id: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT collected_ts, book_valid, reject_reason, orderbook_exists_yes, orderbook_exists_no "
            "FROM btc5m_snapshots WHERE market_id=? ORDER BY collected_ts ASC",
            (market_id,),
        ).fetchall()
    )


def load_reference_ts(conn: sqlite3.Connection, slot_start_ts: int, slot_end_ts: int) -> list[int]:
    return [
        int(row["ts_utc"])
        for row in conn.execute(
            "SELECT ts_utc FROM btc5m_reference_ticks WHERE ts_utc BETWEEN ? AND ? ORDER BY ts_utc ASC",
            (slot_start_ts - REFERENCE_TOLERANCE_SEC, slot_end_ts + REFERENCE_TOLERANCE_SEC),
        ).fetchall()
    ]


def expected_snapshot_count(slot_start_ts: int, slot_end_ts: int) -> int:
    duration_sec = max(0, int(slot_end_ts) - int(slot_start_ts))
    return max(1, (duration_sec // SCAN_INTERVAL_SEC) + 1)


def max_gap_sec(slot_start_ts: int, slot_end_ts: int, collected_ts: list[int]) -> float:
    if not collected_ts:
        return float(max(0, slot_end_ts - slot_start_ts))
    points = [int(slot_start_ts)] + sorted(set(int(ts) for ts in collected_ts if slot_start_ts <= int(ts) <= slot_end_ts)) + [int(slot_end_ts)]
    gaps = [max(0, right - left) for left, right in zip(points, points[1:])]
    return float(max(gaps) if gaps else 0.0)


def reference_gap_seconds(snapshot_ts: int, reference_ts: list[int]) -> Optional[float]:
    if not reference_ts:
        return None
    idx = bisect_left(reference_ts, int(snapshot_ts))
    candidates: list[int] = []
    if idx < len(reference_ts):
        candidates.append(reference_ts[idx])
    if idx > 0:
        candidates.append(reference_ts[idx - 1])
    if not candidates:
        return None
    return float(min(abs(int(snapshot_ts) - candidate) for candidate in candidates))


def duplicate_snapshot_ratio(collected_ts: list[int]) -> float:
    total = len(collected_ts)
    if total == 0:
        return 0.0
    unique_count = len(set(int(ts) for ts in collected_ts))
    duplicates = max(0, total - unique_count)
    return duplicates / float(total)


def structural_book_valid(snapshot_row: sqlite3.Row) -> bool:
    return bool(int(snapshot_row["orderbook_exists_yes"] or 0) == 1 and int(snapshot_row["orderbook_exists_no"] or 0) == 1)


def semantic_reject_snapshot(snapshot_row: sqlite3.Row) -> bool:
    reject_reason = str(snapshot_row["reject_reason"] or "")
    return bool(reject_reason) and structural_book_valid(snapshot_row)


def classify_market_scope(
    market: dict[str, Any],
    *,
    collected_ts: list[int],
    now_ts: int,
    raw_missing_resolution_flag: int,
) -> dict[str, Any]:
    slot_start_ts = int(market["slot_start_ts"])
    slot_end_ts = int(market["slot_end_ts"])
    first_seen_ts = market.get("first_seen_ts")
    first_seen_int = int(first_seen_ts) if first_seen_ts is not None else None
    first_snapshot_ts = min(collected_ts) if collected_ts else None

    first_observed_candidates = [value for value in (first_seen_int, first_snapshot_ts) if value is not None]
    first_observed_ts = min(first_observed_candidates) if first_observed_candidates else None
    late_start_sec = max(0, int(first_observed_ts) - slot_start_ts) if first_observed_ts is not None else None

    if slot_end_ts > now_ts:
        return {
            "scope": "ACTIVE",
            "summary_included": False,
            "late_start_sec": late_start_sec,
            "grace_remaining_sec": max(0, slot_end_ts - now_ts),
            "effective_missing_resolution_flag": 0,
            "scope_note": f"active_market_excluded,remaining_sec={max(0, slot_end_ts - now_ts)}",
        }

    if late_start_sec is not None and late_start_sec > PARTIAL_STARTUP_GRACE_SEC:
        return {
            "scope": "PARTIAL_STARTUP",
            "summary_included": False,
            "late_start_sec": late_start_sec,
            "grace_remaining_sec": None,
            "effective_missing_resolution_flag": int(raw_missing_resolution_flag),
            "scope_note": f"partial_startup_excluded,late_start_sec={late_start_sec}",
        }

    grace_remaining_sec = max(0, (slot_end_ts + SETTLEMENT_GRACE_SEC) - now_ts)
    settlement_grace_active = bool(raw_missing_resolution_flag) and grace_remaining_sec > 0
    if settlement_grace_active:
        return {
            "scope": "SETTLEMENT_GRACE",
            "summary_included": True,
            "late_start_sec": late_start_sec,
            "grace_remaining_sec": grace_remaining_sec,
            "effective_missing_resolution_flag": 0,
            "scope_note": f"settlement_grace_applied,grace_remaining_sec={grace_remaining_sec}",
        }

    return {
        "scope": "FULL",
        "summary_included": True,
        "late_start_sec": late_start_sec,
        "grace_remaining_sec": None,
        "effective_missing_resolution_flag": int(raw_missing_resolution_flag),
        "scope_note": "fully_observed",
    }


def compute_market_audit(conn: sqlite3.Connection, market_row: sqlite3.Row, now_ts: int) -> dict[str, Any]:
    market = dict(market_row)
    slot_start_ts = int(market["slot_start_ts"])
    slot_end_ts = int(market["slot_end_ts"])
    snapshots = load_snapshot_rows(conn, str(market["market_id"]))
    collected_ts = [int(row["collected_ts"]) for row in snapshots]
    refs = load_reference_ts(conn, slot_start_ts, slot_end_ts)

    expected_count = expected_snapshot_count(slot_start_ts, slot_end_ts)
    actual_count = len(collected_ts)
    coverage_ratio = min(1.0, actual_count / float(expected_count)) if expected_count > 0 else None
    structural_invalid_count = sum(1 for row in snapshots if not structural_book_valid(row))
    semantic_reject_count = sum(1 for row in snapshots if semantic_reject_snapshot(row))
    invalid_ratio = (
        structural_invalid_count / float(actual_count)
        if actual_count > 0
        else None
    )
    semantic_reject_ratio = (
        semantic_reject_count / float(actual_count)
        if actual_count > 0
        else None
    )
    duplicate_ratio = duplicate_snapshot_ratio(collected_ts)
    max_gap = max_gap_sec(slot_start_ts, slot_end_ts, collected_ts)

    matched_gaps: list[float] = []
    missing_reference_count = 0
    for snapshot_ts in collected_ts:
        gap = reference_gap_seconds(snapshot_ts, refs)
        if gap is None or gap > REFERENCE_TOLERANCE_SEC:
            missing_reference_count += 1
            continue
        matched_gaps.append(gap)

    missing_reference_ratio = (
        missing_reference_count / float(actual_count)
        if actual_count > 0
        else None
    )
    reference_sync_gap_sec = statistics.median(matched_gaps) if matched_gaps else None

    resolution_status = str(market.get("market_resolution_status") or "ACTIVE")
    raw_missing_resolution_flag = int(
        slot_end_ts <= now_ts
        and (
            resolution_status not in {"RESOLVED", "CANCELLED"}
            or (
                resolution_status == "RESOLVED"
                and (
                    market.get("resolved_outcome") in (None, "")
                    or market.get("resolved_yes_price") is None
                    or market.get("resolved_no_price") is None
                    or market.get("resolved_ts") is None
                )
            )
        )
    )
    scope_info = classify_market_scope(
        market,
        collected_ts=collected_ts,
        now_ts=now_ts,
        raw_missing_resolution_flag=raw_missing_resolution_flag,
    )
    missing_resolution_flag = int(scope_info["effective_missing_resolution_flag"])

    status, notes = evaluate_audit_status(
        coverage_ratio=coverage_ratio,
        max_gap=max_gap,
        duplicate_ratio=duplicate_ratio,
        invalid_ratio=invalid_ratio,
        reference_sync_gap_sec=reference_sync_gap_sec,
        missing_resolution_flag=missing_resolution_flag,
        actual_count=actual_count,
    )
    if not bool(scope_info["summary_included"]):
        status = "INFO"
        notes = scope_info["scope_note"]
    elif notes == "all_thresholds_met":
        notes = scope_info["scope_note"]
    else:
        notes = f"{scope_info['scope_note']},{notes}"

    return {
        "market_id": market["market_id"],
        "market_slug": market["market_slug"],
        "expected_snapshot_count": expected_count,
        "actual_snapshot_count": actual_count,
        "slot_coverage_ratio": coverage_ratio,
        "max_gap_sec": max_gap,
        "invalid_book_ratio": invalid_ratio,
        "structural_invalid_ratio": invalid_ratio,
        "semantic_reject_ratio": semantic_reject_ratio,
        "duplicate_snapshot_ratio": duplicate_ratio,
        "missing_reference_ratio": missing_reference_ratio,
        "missing_resolution_flag": missing_resolution_flag,
        "raw_missing_resolution_flag": raw_missing_resolution_flag,
        "reference_sync_gap_sec": reference_sync_gap_sec,
        "audit_status": status,
        "notes": notes,
        "matched_reference_gaps": matched_gaps,
        "summary_included": bool(scope_info["summary_included"]),
        "audit_scope": str(scope_info["scope"]),
        "late_start_sec": scope_info["late_start_sec"],
        "grace_remaining_sec": scope_info["grace_remaining_sec"],
    }


def evaluate_audit_status(
    *,
    coverage_ratio: Optional[float],
    max_gap: Optional[float],
    duplicate_ratio: Optional[float],
    invalid_ratio: Optional[float],
    reference_sync_gap_sec: Optional[float],
    missing_resolution_flag: int,
    actual_count: int,
) -> tuple[str, str]:
    failures: list[str] = []
    if actual_count <= 0:
        failures.append("no_snapshots")
    if coverage_ratio is None or coverage_ratio < THRESHOLDS["slot_coverage_ratio"]:
        failures.append("coverage_below_threshold")
    if max_gap is None or max_gap > THRESHOLDS["max_gap_sec"]:
        failures.append("max_gap_above_threshold")
    if duplicate_ratio is None or duplicate_ratio >= THRESHOLDS["duplicate_snapshot_ratio"]:
        failures.append("duplicate_ratio_above_threshold")
    if invalid_ratio is None or invalid_ratio >= THRESHOLDS["invalid_book_ratio"]:
        failures.append("invalid_ratio_above_threshold")
    if reference_sync_gap_sec is None or reference_sync_gap_sec > THRESHOLDS["reference_sync_gap_sec"]:
        failures.append("reference_sync_gap_above_threshold")
    if missing_resolution_flag:
        failures.append("missing_official_resolution")
    if not failures:
        return "PASS", "all_thresholds_met"
    return "FAIL", ",".join(failures)


def audit_summary_row(
    market_results: Iterable[dict[str, Any]],
    *,
    run_id: str,
    audit_ts: int,
    audit_date: str,
) -> dict[str, Any]:
    results = list(market_results)
    included_results = [item for item in results if bool(item.get("summary_included", True))]
    total_expected = sum(int(item["expected_snapshot_count"] or 0) for item in included_results)
    total_actual = sum(int(item["actual_snapshot_count"] or 0) for item in included_results)
    total_invalid = sum(
        (float(item["invalid_book_ratio"]) * int(item["actual_snapshot_count"]))
        for item in included_results
        if item["invalid_book_ratio"] is not None
    )
    total_semantic_reject = sum(
        (float(item["semantic_reject_ratio"]) * int(item["actual_snapshot_count"]))
        for item in included_results
        if item["semantic_reject_ratio"] is not None
    )
    total_missing_ref = sum(
        (float(item["missing_reference_ratio"]) * int(item["actual_snapshot_count"]))
        for item in included_results
        if item["missing_reference_ratio"] is not None
    )
    total_duplicate = sum(
        (float(item["duplicate_snapshot_ratio"]) * int(item["actual_snapshot_count"]))
        for item in included_results
        if item["duplicate_snapshot_ratio"] is not None
    )
    ref_gaps = [float(gap) for item in included_results for gap in item.get("matched_reference_gaps", [])]
    pass_count = sum(1 for item in included_results if item["audit_status"] == "PASS")
    fail_count = sum(1 for item in included_results if item["audit_status"] == "FAIL")
    active_excluded_count = sum(1 for item in results if item.get("audit_scope") == "ACTIVE")
    partial_startup_excluded_count = sum(1 for item in results if item.get("audit_scope") == "PARTIAL_STARTUP")
    settlement_grace_count = sum(1 for item in results if item.get("audit_scope") == "SETTLEMENT_GRACE")

    coverage_ratio = min(1.0, total_actual / float(total_expected)) if total_expected > 0 else None
    invalid_ratio = (total_invalid / float(total_actual)) if total_actual > 0 else None
    semantic_reject_ratio = (total_semantic_reject / float(total_actual)) if total_actual > 0 else None
    missing_ref_ratio = (total_missing_ref / float(total_actual)) if total_actual > 0 else None
    duplicate_ratio = (total_duplicate / float(total_actual)) if total_actual > 0 else 0.0
    max_gap = max((float(item["max_gap_sec"] or 0.0) for item in included_results), default=0.0)
    missing_resolution_flag = int(any(int(item["missing_resolution_flag"]) for item in included_results))
    reference_sync_gap_sec = statistics.median(ref_gaps) if ref_gaps else None
    status, notes = evaluate_audit_status(
        coverage_ratio=coverage_ratio,
        max_gap=max_gap,
        duplicate_ratio=duplicate_ratio,
        invalid_ratio=invalid_ratio,
        reference_sync_gap_sec=reference_sync_gap_sec,
        missing_resolution_flag=missing_resolution_flag,
        actual_count=total_actual,
    )
    if not included_results:
        status = "INFO"
        notes = "no_summary_eligible_markets"
    summary_notes = (
        f"market_count={len(results)},included_market_count={len(included_results)},"
        f"pass_count={pass_count},fail_count={fail_count},"
        f"active_excluded_count={active_excluded_count},"
        f"partial_startup_excluded_count={partial_startup_excluded_count},"
        f"settlement_grace_count={settlement_grace_count},"
        f"semantic_reject_ratio={format_metric(semantic_reject_ratio)},{notes}"
    )
    return {
        "audit_ts": audit_ts,
        "audit_date": audit_date,
        "market_id": None,
        "run_id": run_id,
        "expected_snapshot_count": total_expected,
        "actual_snapshot_count": total_actual,
        "slot_coverage_ratio": coverage_ratio,
        "max_gap_sec": max_gap,
        "invalid_book_ratio": invalid_ratio,
        "structural_invalid_ratio": invalid_ratio,
        "semantic_reject_ratio": semantic_reject_ratio,
        "duplicate_snapshot_ratio": duplicate_ratio,
        "missing_reference_ratio": missing_ref_ratio,
        "missing_resolution_flag": missing_resolution_flag,
        "reference_sync_gap_sec": reference_sync_gap_sec,
        "audit_status": status,
        "notes": summary_notes,
    }


def print_market_result(result: dict[str, Any]) -> None:
    log(
        "AUDIT | slug=%s | scope=%s | status=%s | coverage=%s | max_gap=%s | invalid=%s | semantic_reject=%s | dup=%s | missing_ref=%s | missing_res=%s"
        % (
            result["market_slug"],
            result["audit_scope"],
            result["audit_status"],
            format_metric(result["slot_coverage_ratio"]),
            format_metric(result["max_gap_sec"]),
            format_metric(result["invalid_book_ratio"]),
            format_metric(result["semantic_reject_ratio"]),
            format_metric(result["duplicate_snapshot_ratio"]),
            format_metric(result["missing_reference_ratio"]),
            result["missing_resolution_flag"],
        )
    )


def format_metric(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{float(value):.3f}"


def main() -> None:
    args = parse_args()
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args),
        meta_json={
            "lookback_hours": int(args.lookback_hours),
            "max_markets": int(args.max_markets),
            "market_slug": str(args.market_slug or ""),
            "include_active": bool(args.include_active),
            "scan_interval_sec": SCAN_INTERVAL_SEC,
            "reference_tolerance_sec": REFERENCE_TOLERANCE_SEC,
            "partial_startup_grace_sec": PARTIAL_STARTUP_GRACE_SEC,
            "settlement_grace_sec": SETTLEMENT_GRACE_SEC,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    exit_status = "STOPPED"
    now_ts = int(time.time())
    audit_date = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    results: list[dict[str, Any]] = []
    error_count = 0

    try:
        candidates = load_candidate_markets(
            conn,
            now_ts=now_ts,
            lookback_hours=args.lookback_hours,
            max_markets=args.max_markets,
            market_slug=str(args.market_slug or "").strip(),
            include_active=bool(args.include_active),
        )
        log(
            "Audit started | markets=%s | lookback=%sh | include_active=%s | db=%s"
            % (len(candidates), args.lookback_hours, bool(args.include_active), resolve_db_path())
        )

        for market_row in candidates:
            try:
                result = compute_market_audit(conn, market_row, now_ts)
                results.append(result)
                insert_quality_audit(
                    conn,
                    {
                        "audit_ts": now_ts,
                        "audit_date": audit_date,
                        "market_id": result["market_id"],
                        "run_id": run_id,
                        "expected_snapshot_count": result["expected_snapshot_count"],
                        "actual_snapshot_count": result["actual_snapshot_count"],
                        "slot_coverage_ratio": result["slot_coverage_ratio"],
                        "max_gap_sec": result["max_gap_sec"],
                        "invalid_book_ratio": result["invalid_book_ratio"],
                        "structural_invalid_ratio": result["structural_invalid_ratio"],
                        "semantic_reject_ratio": result["semantic_reject_ratio"],
                        "duplicate_snapshot_ratio": result["duplicate_snapshot_ratio"],
                        "missing_reference_ratio": result["missing_reference_ratio"],
                        "missing_resolution_flag": result["missing_resolution_flag"],
                        "reference_sync_gap_sec": result["reference_sync_gap_sec"],
                        "audit_status": result["audit_status"],
                        "notes": result["notes"],
                    },
                )
                print_market_result(result)
                update_collector_run(
                    conn,
                    run_id,
                    {
                        "market_count": len(results),
                        "error_count": error_count,
                        "status": "RUNNING",
                    },
                )
            except Exception as exc:
                error_count += 1
                update_collector_run(
                    conn,
                    run_id,
                    {
                        "market_count": len(results),
                        "error_count": error_count,
                        "status": "RUNNING",
                    },
                )
                log(f"WARN audit_failed | slug={market_row['market_slug']} | reason={exc}")

        summary = audit_summary_row(results, run_id=run_id, audit_ts=now_ts, audit_date=audit_date)
        insert_quality_audit(conn, summary)
        log(
            "SUMMARY | status=%s | markets=%s | coverage=%s | max_gap=%s | invalid=%s | semantic_reject=%s | dup=%s | missing_ref=%s | missing_res=%s | ref_gap=%s"
            % (
                summary["audit_status"],
                len(results),
                format_metric(summary["slot_coverage_ratio"]),
                format_metric(summary["max_gap_sec"]),
                format_metric(summary["invalid_book_ratio"]),
                format_metric(summary["semantic_reject_ratio"]),
                format_metric(summary["duplicate_snapshot_ratio"]),
                format_metric(summary["missing_reference_ratio"]),
                summary["missing_resolution_flag"],
                format_metric(summary["reference_sync_gap_sec"]),
            )
        )
        exit_status = "COMPLETED"
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status if error_count == 0 else "COMPLETED_WITH_ERRORS",
            market_count=len(results),
            error_count=error_count,
            meta_json={
                "lookback_hours": int(args.lookback_hours),
                "max_markets": int(args.max_markets),
                "market_slug": str(args.market_slug or ""),
                "include_active": bool(args.include_active),
                "scan_interval_sec": SCAN_INTERVAL_SEC,
                "reference_tolerance_sec": REFERENCE_TOLERANCE_SEC,
                "partial_startup_grace_sec": PARTIAL_STARTUP_GRACE_SEC,
                "settlement_grace_sec": SETTLEMENT_GRACE_SEC,
                "market_count": len(results),
                "error_count": error_count,
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
