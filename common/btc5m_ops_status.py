"""Helpers for operational BTC5M collector status and audit windows."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

EXCLUDED_AUDIT_NOTE_PREFIXES = (
    "active_market_excluded",
    "partial_startup_excluded",
)


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
        (summary_row["run_id"], limit),
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
        "max_gap_sec": max(gap_values) if gap_values else None,
        "fail_market_slugs": [str(row["market_slug"]) for row in fail_rows[:10]],
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
