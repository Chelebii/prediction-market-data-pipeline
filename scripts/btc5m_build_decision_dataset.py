"""Build the final BTC5M decision dataset from features and labels."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from bisect import bisect_right
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import connect_db, finish_collector_run, resolve_db_path, start_collector_run, update_collector_run
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-build-decision-dataset"
COLLECTOR_VERSION = "2026-03-15"
DEFAULT_FEATURE_VERSION = str(os.getenv("BTC5M_FEATURE_VERSION", "v1")).strip() or "v1"
DEFAULT_LABEL_VERSION = str(os.getenv("BTC5M_LABEL_VERSION", "v1")).strip() or "v1"
DEFAULT_DATASET_VERSION = str(os.getenv("BTC5M_DATASET_VERSION", "")).strip()
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_DATASET_LOOKBACK_HOURS", "168")))
MAX_REFERENCE_LAG_SEC = max(1, int(os.getenv("BTC5M_FEATURE_MAX_REFERENCE_LAG_SEC", "5")))
MAX_SNAPSHOT_AGE_MS = max(1, int(os.getenv("BTC5M_TRAINABLE_MAX_SNAPSHOT_AGE_MS", "5000")))
MAX_COLLECTOR_LATENCY_MS = max(1, int(os.getenv("BTC5M_TRAINABLE_MAX_COLLECTOR_LATENCY_MS", "5000")))
MIN_DECISION_HORIZON_SEC = max(0, int(os.getenv("BTC5M_LABEL_MIN_DECISION_HORIZON_SEC", "5")))
LOG_PATH = Path(os.getenv("BTC5M_DATASET_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_build_decision_dataset.log"))
LOCK_PATH = Path(os.getenv("BTC5M_DATASET_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_build_decision_dataset.lock"))

_logger = logging.getLogger("btc5m_build_decision_dataset")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-DATASET | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-DATASET | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the final BTC5M decision dataset.")
    parser.add_argument("--feature-version", type=str, default=DEFAULT_FEATURE_VERSION, help="Feature version to join.")
    parser.add_argument("--label-version", type=str, default=DEFAULT_LABEL_VERSION, help="Label version to join.")
    parser.add_argument("--dataset-version", type=str, default=DEFAULT_DATASET_VERSION, help="Dataset version label.")
    parser.add_argument("--market-slug", type=str, default="", help="Only rebuild one market slug.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to rebuild dataset rows.")
    parser.add_argument("--max-markets", type=int, default=250, help="Maximum number of markets to rebuild.")
    return parser.parse_args()


def normalize_dataset_version(args: argparse.Namespace) -> str:
    raw = str(args.dataset_version or "").strip()
    if raw:
        return raw
    return f"{str(args.feature_version).strip()}__{str(args.label_version).strip()}"


def collector_config_hash(args: argparse.Namespace, dataset_version: str) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "feature_version": str(args.feature_version),
        "label_version": str(args.label_version),
        "dataset_version": dataset_version,
        "db_path": str(resolve_db_path()),
        "lookback_hours": int(args.lookback_hours),
        "max_markets": int(args.max_markets),
        "market_slug": str(args.market_slug or ""),
        "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
        "max_snapshot_age_ms": MAX_SNAPSHOT_AGE_MS,
        "max_collector_latency_ms": MAX_COLLECTOR_LATENCY_MS,
        "min_decision_horizon_sec": MIN_DECISION_HORIZON_SEC,
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
    feature_version: str,
    label_version: str,
) -> list[sqlite3.Row]:
    lower_bound = now_ts - (max(1, lookback_hours) * 3600)
    clauses = [
        "m.slot_end_ts >= ?",
        "EXISTS (SELECT 1 FROM btc5m_features f WHERE f.market_id = m.market_id AND f.feature_version = ?)",
        "EXISTS (SELECT 1 FROM btc5m_labels l WHERE l.market_id = m.market_id AND l.label_version = ?)",
    ]
    params: list[Any] = [lower_bound, feature_version, label_version]
    if market_slug:
        clauses.append("m.market_slug = ?")
        params.append(market_slug)
    sql = (
        "SELECT m.market_id, m.market_slug, m.slot_start_ts, m.slot_end_ts "
        "FROM btc5m_markets m "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY m.slot_start_ts ASC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def assign_split_buckets(market_rows: list[sqlite3.Row]) -> dict[str, str]:
    total = len(market_rows)
    if total == 0:
        return {}
    mapping: dict[str, str] = {}
    for index, market_row in enumerate(market_rows):
        if total == 1:
            bucket = "train"
        else:
            ratio = index / float(total)
            if ratio < 0.70:
                bucket = "train"
            elif ratio < 0.85:
                bucket = "validation"
            else:
                bucket = "test"
        mapping[str(market_row["market_id"])] = bucket
    return mapping


def load_join_rows(conn: sqlite3.Connection, market_id: str, feature_version: str, label_version: str) -> list[sqlite3.Row]:
    sql = """
        SELECT
            f.market_id,
            f.ts_utc AS decision_ts,
            f.seconds_to_resolution,
            f.quote_stability_score,
            s.market_slug,
            s.mid_yes,
            s.mid_no,
            s.spread_yes,
            s.spread_no,
            s.book_valid,
            s.orderbook_exists_yes,
            s.orderbook_exists_no,
            s.snapshot_age_ms,
            s.collector_latency_ms,
            s.market_status,
            l.terminal_outcome,
            l.mtm_return_if_buy_yes_hold_to_resolution,
            l.mtm_return_if_buy_no_hold_to_resolution,
            l.label_quality_flag,
            m.market_resolution_status
        FROM btc5m_features f
        JOIN btc5m_labels l
          ON l.market_id = f.market_id
         AND l.decision_ts = f.ts_utc
         AND l.label_version = ?
        JOIN btc5m_snapshots s
          ON s.market_id = f.market_id
         AND s.collected_ts = f.ts_utc
        JOIN btc5m_markets m
          ON m.market_id = f.market_id
        WHERE f.market_id = ?
          AND f.feature_version = ?
        ORDER BY f.ts_utc ASC
    """
    return list(conn.execute(sql, (label_version, market_id, feature_version)).fetchall())


def load_reference_rows(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> list[sqlite3.Row]:
    sql = """
        SELECT ts_utc, btc_price
        FROM btc5m_reference_ticks
        WHERE ts_utc BETWEEN ? AND ?
        ORDER BY ts_utc ASC
    """
    return list(conn.execute(sql, (start_ts, end_ts)).fetchall())


def reference_price_at_or_before(reference_rows: list[sqlite3.Row], reference_ts: list[int], target_ts: int) -> Optional[float]:
    idx = bisect_right(reference_ts, int(target_ts)) - 1
    if idx < 0:
        return None
    if int(target_ts) - int(reference_ts[idx]) > MAX_REFERENCE_LAG_SEC:
        return None
    try:
        return float(reference_rows[idx]["btc_price"])
    except Exception:
        return None


def delete_dataset_rows(conn: sqlite3.Connection, market_id: str, dataset_version: str) -> None:
    conn.execute(
        "DELETE FROM btc5m_decision_dataset WHERE market_id=? AND dataset_version=?",
        (market_id, dataset_version),
    )
    conn.commit()


def insert_dataset_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO btc5m_decision_dataset ({', '.join(columns)}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(row[column] for column in columns) for row in rows])
    conn.commit()
    return len(rows)


def trainability_reason(row: dict[str, Any], btc_price: Optional[float]) -> Optional[str]:
    if str(row.get("market_resolution_status") or "") != "RESOLVED":
        return "MARKET_NOT_RESOLVED"
    if str(row.get("label_quality_flag") or "") != "OFFICIAL_RESOLVED":
        return str(row.get("label_quality_flag") or "LABEL_NOT_OFFICIAL")
    if int(row.get("book_valid") or 0) != 1:
        return "BOOK_INVALID"
    if int(row.get("seconds_to_resolution") or 0) < MIN_DECISION_HORIZON_SEC:
        return "DECISION_TOO_LATE"
    if int(row.get("orderbook_exists_yes") or 0) != 1 or int(row.get("orderbook_exists_no") or 0) != 1:
        return "ORDERBOOK_MISSING"
    if btc_price is None:
        return "REFERENCE_MISSING"
    if row.get("mtm_return_if_buy_yes_hold_to_resolution") is None or row.get("mtm_return_if_buy_no_hold_to_resolution") is None:
        return "ENTRY_PRICE_MISSING"
    snapshot_age_ms = row.get("snapshot_age_ms")
    if snapshot_age_ms is not None and int(snapshot_age_ms) > MAX_SNAPSHOT_AGE_MS:
        return "SNAPSHOT_STALE"
    collector_latency_ms = row.get("collector_latency_ms")
    if collector_latency_ms is not None and int(collector_latency_ms) > MAX_COLLECTOR_LATENCY_MS:
        return "COLLECTOR_SLOW"
    if row.get("mid_yes") is None or row.get("mid_no") is None:
        return "MID_MISSING"
    if str(row.get("market_status") or "") == "CANCELLED":
        return "CANCELLED"
    return None


def merge_quality_flag(base_flag: Any, reason: Optional[str]) -> str:
    flag = str(base_flag or "")
    if not reason:
        return flag or "OFFICIAL_RESOLVED"
    if not flag:
        return reason
    if reason in flag.split("|"):
        return flag
    return f"{flag}|{reason}"


def build_dataset_rows_for_market(
    conn: sqlite3.Connection,
    market_row: sqlite3.Row,
    *,
    feature_version: str,
    label_version: str,
    dataset_version: str,
    split_bucket: str,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in load_join_rows(conn, str(market_row["market_id"]), feature_version, label_version)]
    if not rows:
        return []

    first_ts = int(rows[0]["decision_ts"])
    last_ts = int(rows[-1]["decision_ts"])
    reference_rows = load_reference_rows(conn, first_ts - MAX_REFERENCE_LAG_SEC, last_ts)
    reference_ts = [int(row["ts_utc"]) for row in reference_rows]

    dataset_rows: list[dict[str, Any]] = []
    for row in rows:
        btc_price = reference_price_at_or_before(reference_rows, reference_ts, int(row["decision_ts"]))
        reason = trainability_reason(row, btc_price)
        dataset_rows.append(
            {
                "market_id": row["market_id"],
                "decision_ts": int(row["decision_ts"]),
                "seconds_to_resolution": int(row["seconds_to_resolution"]),
                "market_slug": row["market_slug"],
                "mid_yes": row["mid_yes"],
                "mid_no": row["mid_no"],
                "spread_yes": row["spread_yes"],
                "spread_no": row["spread_no"],
                "btc_price": btc_price,
                "quote_stability_score": row["quote_stability_score"],
                "terminal_outcome": row["terminal_outcome"],
                "target_yes_hold": row["mtm_return_if_buy_yes_hold_to_resolution"],
                "target_no_hold": row["mtm_return_if_buy_no_hold_to_resolution"],
                "label_quality_flag": merge_quality_flag(row.get("label_quality_flag"), reason),
                "is_trainable": int(reason is None),
                "split_bucket": split_bucket,
                "dataset_version": dataset_version,
            }
        )
    return dataset_rows


def process_market(
    conn: sqlite3.Connection,
    market_row: sqlite3.Row,
    *,
    feature_version: str,
    label_version: str,
    dataset_version: str,
    split_bucket: str,
) -> int:
    dataset_rows = build_dataset_rows_for_market(
        conn,
        market_row,
        feature_version=feature_version,
        label_version=label_version,
        dataset_version=dataset_version,
        split_bucket=split_bucket,
    )
    delete_dataset_rows(conn, str(market_row["market_id"]), dataset_version)
    inserted = insert_dataset_rows(conn, dataset_rows)
    log(
        "DATASET | slug=%s | dataset_version=%s | split=%s | rows=%s"
        % (market_row["market_slug"], dataset_version, split_bucket, inserted)
    )
    return inserted


def main() -> None:
    args = parse_args()
    feature_version = str(args.feature_version or DEFAULT_FEATURE_VERSION).strip() or DEFAULT_FEATURE_VERSION
    label_version = str(args.label_version or DEFAULT_LABEL_VERSION).strip() or DEFAULT_LABEL_VERSION
    dataset_version = normalize_dataset_version(args)
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args, dataset_version),
        meta_json={
            "feature_version": feature_version,
            "label_version": label_version,
            "dataset_version": dataset_version,
            "lookback_hours": int(args.lookback_hours),
            "max_markets": int(args.max_markets),
            "market_slug": str(args.market_slug or ""),
            "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
            "max_snapshot_age_ms": MAX_SNAPSHOT_AGE_MS,
            "max_collector_latency_ms": MAX_COLLECTOR_LATENCY_MS,
            "min_decision_horizon_sec": MIN_DECISION_HORIZON_SEC,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    exit_status = "STOPPED"
    market_count = 0
    dataset_row_count = 0
    error_count = 0

    try:
        now_ts = int(time.time())
        market_rows = load_candidate_markets(
            conn,
            now_ts=now_ts,
            lookback_hours=args.lookback_hours,
            max_markets=args.max_markets,
            market_slug=str(args.market_slug or "").strip(),
            feature_version=feature_version,
            label_version=label_version,
        )
        split_map = assign_split_buckets(market_rows)
        log(
            "Decision dataset build started | markets=%s | dataset_version=%s | db=%s"
            % (len(market_rows), dataset_version, resolve_db_path())
        )

        for market_row in market_rows:
            try:
                dataset_row_count += process_market(
                    conn,
                    market_row,
                    feature_version=feature_version,
                    label_version=label_version,
                    dataset_version=dataset_version,
                    split_bucket=split_map.get(str(market_row["market_id"]), "train"),
                )
                market_count += 1
                update_collector_run(
                    conn,
                    run_id,
                    {
                        "market_count": market_count,
                        "snapshot_count": dataset_row_count,
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
                        "market_count": market_count,
                        "snapshot_count": dataset_row_count,
                        "error_count": error_count,
                        "status": "RUNNING",
                    },
                )
                log(f"WARN dataset_build_failed | slug={market_row['market_slug']} | reason={exc}")

        log(
            "SUMMARY | status=%s | markets=%s | dataset_rows=%s | dataset_version=%s"
            % ("COMPLETED" if error_count == 0 else "COMPLETED_WITH_ERRORS", market_count, dataset_row_count, dataset_version)
        )
        exit_status = "COMPLETED"
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status if error_count == 0 else "COMPLETED_WITH_ERRORS",
            market_count=market_count,
            snapshot_count=dataset_row_count,
            error_count=error_count,
            meta_json={
                "feature_version": feature_version,
                "label_version": label_version,
                "dataset_version": dataset_version,
                "lookback_hours": int(args.lookback_hours),
                "max_markets": int(args.max_markets),
                "market_slug": str(args.market_slug or ""),
                "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
                "max_snapshot_age_ms": MAX_SNAPSHOT_AGE_MS,
                "max_collector_latency_ms": MAX_COLLECTOR_LATENCY_MS,
                "min_decision_horizon_sec": MIN_DECISION_HORIZON_SEC,
                "market_count": market_count,
                "dataset_row_count": dataset_row_count,
                "error_count": error_count,
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
