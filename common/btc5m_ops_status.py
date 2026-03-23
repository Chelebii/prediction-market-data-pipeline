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


def _normalize_scanner_reason_tag(raw_reason: Any) -> str | None:
    if raw_reason in (None, ""):
        return None
    text = str(raw_reason).strip()
    if not text:
        return None
    token = text.split()[0].strip().lower()
    if token == "ok":
        return None
    return token or None


def _scanner_reject_tags(reason: Any, meta_json: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    reject_detail = meta_json.get("reject_detail")
    if isinstance(reject_detail, dict):
        for side in ("yes_reason", "no_reason"):
            normalized = _normalize_scanner_reason_tag(reject_detail.get(side))
            if normalized:
                side_label = "yes" if side.startswith("yes") else "no"
                tags.append(f"{side_label}.{normalized}")
    elif isinstance(reject_detail, str):
        normalized = _normalize_scanner_reason_tag(reject_detail)
        if normalized:
            tags.append(f"cross.{normalized}")

    normalized_reason = _normalize_scanner_reason_tag(reason)
    if normalized_reason:
        tags.append(normalized_reason)

    deduped: list[str] = []
    for tag in tags:
        if tag not in deduped:
            deduped.append(tag)
    return deduped


def scanner_recent_activity_summary(
    conn: sqlite3.Connection,
    *,
    now_ts: int,
    recent_window_sec: int,
) -> dict[str, Any]:
    recent_window_sec = max(60, int(recent_window_sec))
    min_event_ts = max(0, int(now_ts) - recent_window_sec)
    rows = conn.execute(
        """
        SELECT event_ts, event_type, reason, meta_json
        FROM btc5m_lifecycle_events
        WHERE event_ts >= ?
          AND event_type IN ('REJECTED', 'WARMUP', 'PUBLISHED')
        ORDER BY event_ts DESC, event_id DESC
        """,
        (min_event_ts,),
    ).fetchall()

    event_counts = {
        "PUBLISHED": 0,
        "WARMUP": 0,
        "REJECTED": 0,
    }
    reject_reason_counts: dict[str, int] = {}
    last_event_ts = None
    last_event_type = None
    last_event_reason = None

    for row in rows:
        event_type = str(row["event_type"] or "").upper()
        if event_type not in event_counts:
            continue
        event_counts[event_type] += 1
        if last_event_ts is None:
            last_event_ts = int(row["event_ts"])
            last_event_type = event_type
            last_event_reason = str(row["reason"] or "")
        if event_type == "REJECTED":
            meta_json = parse_meta_json(row["meta_json"])
            for tag in _scanner_reject_tags(row["reason"], meta_json):
                reject_reason_counts[tag] = int(reject_reason_counts.get(tag, 0)) + 1

    total_events = sum(event_counts.values())
    reject_count = int(event_counts["REJECTED"])
    warmup_count = int(event_counts["WARMUP"])
    published_count = int(event_counts["PUBLISHED"])
    last_event_age_sec = None if last_event_ts is None else max(0, int(now_ts) - int(last_event_ts))

    top_reject_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(
            reject_reason_counts.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:8]
    ]

    return {
        "window_sec": recent_window_sec,
        "total_events": total_events,
        "published_count": published_count,
        "warmup_count": warmup_count,
        "rejected_count": reject_count,
        "published_ratio": (published_count / total_events) if total_events else None,
        "warmup_ratio": (warmup_count / total_events) if total_events else None,
        "reject_ratio": (reject_count / total_events) if total_events else None,
        "top_reject_reasons": top_reject_reasons,
        "last_event_ts": last_event_ts,
        "last_event_age_sec": last_event_age_sec,
        "last_event_type": last_event_type,
        "last_event_reason": last_event_reason,
    }


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
    return bool(
        collector_recent_error_state(
            run_info,
            now_ts=now_ts,
            recent_window_sec=recent_window_sec,
        )["active"]
    )


def collector_recent_error_state(
    run_info: Optional[dict[str, Any]],
    *,
    now_ts: int,
    recent_window_sec: int,
) -> dict[str, Any]:
    if not run_info:
        return {
            "active": False,
            "count": 0,
            "last_error_ts": None,
            "last_error_age_sec": None,
            "last_error_reason": None,
            "last_error_kind": None,
            "last_success_ts": None,
            "consecutive_error_count": 0,
        }
    error_count = int(run_info.get("error_count") or 0)
    meta = parse_meta_json(run_info.get("meta_json"))
    recent_window_sec = max(60, int(recent_window_sec))
    last_success_ts = meta.get("last_success_ts")
    last_error_ts = meta.get("last_error_ts")
    last_error_age_sec = None
    try:
        if last_success_ts is not None:
            last_success_ts = int(last_success_ts)
    except Exception:
        last_success_ts = None
    try:
        if last_error_ts is not None:
            last_error_ts = int(last_error_ts)
            last_error_age_sec = max(0, int(now_ts) - last_error_ts)
    except Exception:
        last_error_ts = None
        last_error_age_sec = None

    recent_errors: list[int] = []
    raw_recent_errors = meta.get("recent_error_timestamps")
    if isinstance(raw_recent_errors, list):
        for value in raw_recent_errors:
            try:
                ts_value = int(value)
            except Exception:
                continue
            if max(0, int(now_ts) - ts_value) <= recent_window_sec:
                recent_errors.append(ts_value)

    if last_error_ts is not None and last_error_age_sec is not None and last_error_age_sec <= recent_window_sec:
        recent_errors.append(last_error_ts)

    recent_errors = sorted(set(recent_errors))
    try:
        consecutive_error_count = int(meta.get("consecutive_error_count") or 0)
    except Exception:
        consecutive_error_count = 0

    return {
        "active": bool(error_count > 0 and recent_errors),
        "count": len(recent_errors),
        "last_error_ts": last_error_ts,
        "last_error_age_sec": last_error_age_sec,
        "last_error_reason": meta.get("last_error_reason"),
        "last_error_kind": meta.get("last_error_kind"),
        "last_success_ts": last_success_ts,
        "consecutive_error_count": consecutive_error_count,
    }


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
