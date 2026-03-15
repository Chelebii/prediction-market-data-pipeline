"""Collect official BTC5M market resolutions into the dataset DB."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import (
    connect_db,
    finish_collector_run,
    insert_lifecycle_event,
    resolve_db_path,
    start_collector_run,
    update_collector_run,
    update_market,
)
from common.btc5m_resolution_feed import (
    DEFAULT_SOURCE_NAME,
    DEFAULT_TIMEOUT_SEC,
    GAMMA_BASE_URL,
    ResolutionFeedError,
    build_resolution_session,
    derive_resolution_decision,
    fetch_gamma_market_by_slug,
)
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-resolution-collector"
COLLECTOR_VERSION = "2026-03-15"
SOURCE_NAME = str(os.getenv("BTC5M_RESOLUTION_SOURCE_NAME", DEFAULT_SOURCE_NAME)).strip() or DEFAULT_SOURCE_NAME
BASE_URL = str(os.getenv("BTC5M_RESOLUTION_BASE_URL", GAMMA_BASE_URL)).strip() or GAMMA_BASE_URL
INTERVAL_SEC = max(5, int(os.getenv("BTC5M_RESOLUTION_INTERVAL_SEC", "30")))
TIMEOUT_SEC = max(1, int(os.getenv("BTC5M_RESOLUTION_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))))
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_RESOLUTION_LOOKBACK_HOURS", "48")))
LOG_PATH = Path(os.getenv("BTC5M_RESOLUTION_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_resolution_collector.log"))
LOCK_PATH = Path(os.getenv("BTC5M_RESOLUTION_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_resolution_collector.lock"))

_logger = logging.getLogger("btc5m_resolution_collector")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-RES | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-RES | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect official BTC5M market resolutions into the dataset DB.")
    parser.add_argument("--once", action="store_true", help="Run a single resolution sweep and exit.")
    parser.add_argument("--max-markets", type=int, default=25, help="Maximum candidate markets to inspect per sweep.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to scan expired markets.")
    parser.add_argument("--market-slug", type=str, default="", help="Only inspect one market slug from the DB.")
    return parser.parse_args()


def collector_config_hash(lookback_hours: int) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "source_name": SOURCE_NAME,
        "base_url": BASE_URL,
        "interval_sec": INTERVAL_SEC,
        "timeout_sec": TIMEOUT_SEC,
        "lookback_hours": lookback_hours,
        "db_path": str(resolve_db_path()),
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
) -> list[sqlite3.Row]:
    lower_bound = now_ts - (max(1, lookback_hours) * 3600)
    clauses = [
        "slot_end_ts >= ?",
        "slot_end_ts <= ?",
        "market_resolution_status NOT IN ('RESOLVED', 'CANCELLED')",
    ]
    params: list[Any] = [lower_bound, now_ts]

    if market_slug:
        clauses.append("market_slug = ?")
        params.append(market_slug)

    sql = (
        "SELECT market_id, market_slug, slot_end_ts, market_status, market_resolution_status, "
        "resolved_outcome, resolved_yes_price, resolved_no_price, resolved_ts, settled_ts, label_quality_flag "
        "FROM btc5m_markets "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY slot_end_ts ASC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def lifecycle_event_exists(
    conn: sqlite3.Connection,
    *,
    market_id: str,
    event_type: str,
    event_ts: int,
    reason: Optional[str],
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM btc5m_lifecycle_events WHERE market_id=? AND event_type=? AND event_ts=? AND COALESCE(reason, '')=? LIMIT 1",
        (market_id, event_type, int(event_ts), str(reason or "")),
    ).fetchone()
    return row is not None


def insert_lifecycle_event_if_missing(
    conn: sqlite3.Connection,
    *,
    market_id: str,
    event_type: Optional[str],
    event_ts: Optional[int],
    reason: Optional[str],
    meta_json: dict[str, Any],
) -> int:
    if not event_type or event_ts is None:
        return 0
    if lifecycle_event_exists(conn, market_id=market_id, event_type=event_type, event_ts=event_ts, reason=reason):
        return 0
    return insert_lifecycle_event(
        conn,
        {
            "market_id": market_id,
            "event_ts": int(event_ts),
            "event_type": event_type,
            "reason": reason,
            "meta_json": meta_json,
        },
    )


def update_run_metrics(conn: sqlite3.Connection, run_id: str, stats: dict[str, int]) -> None:
    update_collector_run(
        conn,
        run_id,
        {
            "market_count": stats["processed_count"],
            "error_count": stats["error_count"],
            "status": "RUNNING",
            "meta_json": {
                "resolved_count": stats["resolved_count"],
                "pending_count": stats["pending_count"],
                "cancelled_count": stats["cancelled_count"],
                "mismatch_count": stats["mismatch_count"],
                "active_count": stats["active_count"],
            },
        },
    )


def process_market(
    conn: sqlite3.Connection,
    session,
    db_market: sqlite3.Row,
    *,
    now_ts: int,
) -> tuple[str, str]:
    slug = str(db_market["market_slug"])
    fetched = fetch_gamma_market_by_slug(
        session,
        market_slug=slug,
        base_url=BASE_URL,
        timeout_sec=TIMEOUT_SEC,
    )
    decision = derive_resolution_decision(
        db_market,
        fetched["market"],
        now_ts=now_ts,
        source_name=SOURCE_NAME,
        fetch_meta=fetched["fetch_meta"],
    )
    update_market(conn, str(db_market["market_id"]), decision.updates)
    insert_lifecycle_event_if_missing(
        conn,
        market_id=str(db_market["market_id"]),
        event_type=decision.event_type,
        event_ts=decision.event_ts,
        reason=decision.event_reason,
        meta_json=decision.event_meta,
    )

    log_parts = [
        f"slug={slug}",
        f"status={decision.status}",
        f"quality={decision.quality_flag}",
    ]
    if decision.outcome:
        log_parts.append(f"outcome={decision.outcome}")
    if decision.updates.get("resolved_yes_price") is not None or decision.updates.get("resolved_no_price") is not None:
        log_parts.append(
            "prices=%s/%s"
            % (
                decision.updates.get("resolved_yes_price"),
                decision.updates.get("resolved_no_price"),
            )
        )
    log("RESOLUTION | " + " | ".join(log_parts))
    return decision.status, decision.quality_flag


def main() -> None:
    args = parse_args()
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    session = build_resolution_session()
    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args.lookback_hours),
        meta_json={
            "source_name": SOURCE_NAME,
            "base_url": BASE_URL,
            "lookback_hours": args.lookback_hours,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
            "market_slug": str(args.market_slug or ""),
        },
    )

    stats = {
        "processed_count": 0,
        "resolved_count": 0,
        "pending_count": 0,
        "cancelled_count": 0,
        "mismatch_count": 0,
        "active_count": 0,
        "error_count": 0,
    }
    exit_status = "STOPPED"

    log(
        "Resolution collector started | source=%s | interval=%ss | lookback=%sh | db=%s"
        % (SOURCE_NAME, INTERVAL_SEC, args.lookback_hours, resolve_db_path())
    )

    try:
        while True:
            loop_started_at = time.perf_counter()
            now_ts = int(time.time())
            candidates = load_candidate_markets(
                conn,
                now_ts=now_ts,
                lookback_hours=args.lookback_hours,
                max_markets=args.max_markets,
                market_slug=str(args.market_slug or "").strip(),
            )

            if not candidates:
                log("No due markets found for resolution sweep.")
            for market_row in candidates:
                try:
                    status, quality_flag = process_market(conn, session, market_row, now_ts=now_ts)
                    stats["processed_count"] += 1
                    if status == "RESOLVED":
                        stats["resolved_count"] += 1
                    elif status == "PENDING_SETTLEMENT":
                        stats["pending_count"] += 1
                    elif status == "CANCELLED":
                        stats["cancelled_count"] += 1
                    else:
                        stats["active_count"] += 1
                    if quality_flag == "MARKET_ID_MISMATCH":
                        stats["mismatch_count"] += 1
                    update_run_metrics(conn, run_id, stats)
                except KeyboardInterrupt:
                    exit_status = "STOPPED"
                    raise SystemExit(0)
                except ResolutionFeedError as exc:
                    stats["error_count"] += 1
                    update_run_metrics(conn, run_id, stats)
                    log(f"WARN resolution_fetch_failed | slug={market_row['market_slug']} | reason={exc}")
                    if args.once:
                        exit_status = "FAILED"
                        raise SystemExit(1)
                except Exception as exc:
                    stats["error_count"] += 1
                    update_run_metrics(conn, run_id, stats)
                    log(f"Runtime Error | slug={market_row['market_slug']} | reason={exc}")
                    if args.once:
                        exit_status = "FAILED"
                        raise

            if args.once:
                exit_status = "COMPLETED"
                break

            sleep_sec = max(0.0, INTERVAL_SEC - (time.perf_counter() - loop_started_at))
            time.sleep(sleep_sec)
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status,
            market_count=stats["processed_count"],
            error_count=stats["error_count"],
            meta_json={
                "source_name": SOURCE_NAME,
                "base_url": BASE_URL,
                "lookback_hours": args.lookback_hours,
                "resolved_count": stats["resolved_count"],
                "pending_count": stats["pending_count"],
                "cancelled_count": stats["cancelled_count"],
                "mismatch_count": stats["mismatch_count"],
                "active_count": stats["active_count"],
                "market_slug": str(args.market_slug or ""),
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
