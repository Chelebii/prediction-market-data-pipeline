"""Helpers for operational BTC5M collector status and audit windows."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

EXCLUDED_AUDIT_NOTE_PREFIXES = (
    "active_market_excluded",
    "partial_startup_excluded",
)

UPTIME_STRONG_RATIO = 0.95
UPTIME_GOOD_RATIO = 0.90
UPTIME_CAUTION_RATIO = 0.80
MATERIAL_OPERATIONAL_FAIL_COUNT = 2
MATERIAL_OPERATIONAL_MIN_COVERAGE = 0.97
MATERIAL_OPERATIONAL_MAX_GAP_SEC = 9.0


def parse_meta_json(raw_value: Any) -> dict[str, Any]:
    if raw_value in (None, ""):
        return {}
    if isinstance(raw_value, dict):
        return dict(raw_value)
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def latest_operational_audit_window(
    conn: sqlite3.Connection,
    *,
    window_markets: int,
    min_slot_start_ts: Optional[int] = None,
) -> dict[str, Any] | None:
    summary_row = conn.execute(
        "SELECT run_id, audit_ts, audit_status, notes "
        "FROM quality_audits WHERE market_id IS NULL ORDER BY audit_ts DESC, audit_id DESC LIMIT 1"
    ).fetchone()
    if not summary_row or not summary_row["run_id"]:
        return None

    limit = max(1, int(window_markets))
    rows = conn.execute(
        """
        SELECT
            qa.market_id,
            qa.audit_status,
            qa.slot_coverage_ratio,
            qa.max_gap_sec,
            qa.notes,
            m.market_slug,
            m.slot_end_ts
        FROM quality_audits qa
        JOIN btc5m_markets m ON m.market_id = qa.market_id
        WHERE qa.run_id = ?
          AND (? IS NULL OR m.slot_start_ts >= ?)
          AND (
            qa.notes IS NULL
            OR (
              qa.notes NOT LIKE 'active_market_excluded%%'
              AND qa.notes NOT LIKE 'partial_startup_excluded%%'
            )
          )
        ORDER BY m.slot_end_ts DESC
        LIMIT ?
        """,
        (summary_row["run_id"], min_slot_start_ts, min_slot_start_ts, limit),
    ).fetchall()
    if not rows:
        return None

    pass_count = sum(1 for row in rows if str(row["audit_status"] or "") == "PASS")
    fail_rows = [row for row in rows if str(row["audit_status"] or "") != "PASS"]
    coverage_values = [float(row["slot_coverage_ratio"]) for row in rows if row["slot_coverage_ratio"] is not None]
    gap_values = [float(row["max_gap_sec"]) for row in rows if row["max_gap_sec"] is not None]

    status = "PASS" if len(fail_rows) == 0 and len(rows) >= limit else "FAIL"
    return {
        "run_id": str(summary_row["run_id"]),
        "audit_ts": int(summary_row["audit_ts"]),
        "window_markets": limit,
        "window_count": len(rows),
        "pass_count": pass_count,
        "fail_count": len(fail_rows),
        "status": status,
        "min_coverage_ratio": min(coverage_values) if coverage_values else None,
        "avg_coverage_ratio": (sum(coverage_values) / len(coverage_values)) if coverage_values else None,
        "max_gap_sec": max(gap_values) if gap_values else None,
        "fail_market_slugs": [str(row["market_slug"]) for row in fail_rows[:10]],
        "min_slot_start_ts": int(min_slot_start_ts) if min_slot_start_ts is not None else None,
    }


def classify_uptime_ratio(ratio: Any) -> dict[str, Any]:
    if ratio is None:
        return {
            "ratio": None,
            "pct": None,
            "band": "UNKNOWN",
            "message": "no uptime data",
        }

    value = max(0.0, min(1.0, float(ratio)))
    pct = value * 100.0
    if value >= UPTIME_STRONG_RATIO:
        band = "STRONG"
        message = "strong research quality"
    elif value >= UPTIME_GOOD_RATIO:
        band = "GOOD"
        message = "good for baseline research"
    elif value >= UPTIME_CAUTION_RATIO:
        band = "CAUTION"
        message = "use carefully"
    else:
        band = "RISKY"
        message = "strategy/model conclusions risky"

    return {
        "ratio": value,
        "pct": pct,
        "band": band,
        "message": message,
    }


def collector_has_recent_error(
    run_info: Optional[dict[str, Any]],
    *,
    now_ts: int,
    recent_window_sec: int,
) -> bool:
    if not run_info:
        return False
    error_count = int(run_info.get("error_count") or 0)
    if error_count <= 0:
        return False
    meta = parse_meta_json(run_info.get("meta_json"))
    last_error_ts = meta.get("last_error_ts")
    if last_error_ts is None:
        return False
    try:
        age_sec = max(0, int(now_ts) - int(last_error_ts))
    except Exception:
        return False
    return age_sec <= max(60, int(recent_window_sec))


def operational_audit_is_material_failure(window: Optional[dict[str, Any]]) -> bool:
    if not window:
        return False
    if str(window.get("status") or "") != "FAIL":
        return False

    fail_count = int(window.get("fail_count") or 0)
    if fail_count >= MATERIAL_OPERATIONAL_FAIL_COUNT:
        return True

    min_coverage = window.get("min_coverage_ratio")
    if min_coverage is not None and float(min_coverage) < MATERIAL_OPERATIONAL_MIN_COVERAGE:
        return True

    max_gap = window.get("max_gap_sec")
    if max_gap is not None and float(max_gap) > MATERIAL_OPERATIONAL_MAX_GAP_SEC:
        return True

    return False
