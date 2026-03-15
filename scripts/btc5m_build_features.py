"""Build versioned BTC5M features from raw snapshots and reference ticks."""

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
from bisect import bisect_left, bisect_right
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
    resolve_db_path,
    start_collector_run,
    update_collector_run,
)
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-build-features"
COLLECTOR_VERSION = "2026-03-15"
DEFAULT_FEATURE_VERSION = str(os.getenv("BTC5M_FEATURE_VERSION", "v1")).strip() or "v1"
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_FEATURE_LOOKBACK_HOURS", "48")))
MAX_REFERENCE_LAG_SEC = max(1, int(os.getenv("BTC5M_FEATURE_MAX_REFERENCE_LAG_SEC", "5")))
MIN_STABLE_PASSES = max(1, int(os.getenv("BTC_5MIN_MIN_STABLE_PASSES", "2")))
LOG_PATH = Path(os.getenv("BTC5M_FEATURE_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_build_features.log"))
LOCK_PATH = Path(os.getenv("BTC5M_FEATURE_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_build_features.lock"))

_logger = logging.getLogger("btc5m_build_features")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-FEAT | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-FEAT | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build BTC5M derived features from raw tables.")
    parser.add_argument("--feature-version", type=str, default=DEFAULT_FEATURE_VERSION, help="Feature version label.")
    parser.add_argument("--market-slug", type=str, default="", help="Only rebuild one market slug.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to rebuild features.")
    parser.add_argument("--max-markets", type=int, default=250, help="Maximum number of markets to rebuild.")
    return parser.parse_args()


def collector_config_hash(args: argparse.Namespace) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "feature_version": str(args.feature_version),
        "db_path": str(resolve_db_path()),
        "lookback_hours": int(args.lookback_hours),
        "max_markets": int(args.max_markets),
        "market_slug": str(args.market_slug or ""),
        "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
        "min_stable_passes": MIN_STABLE_PASSES,
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
        "m.slot_end_ts >= ?",
        "EXISTS (SELECT 1 FROM btc5m_snapshots s WHERE s.market_id = m.market_id)",
    ]
    params: list[Any] = [lower_bound]
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


def load_market_snapshots(conn: sqlite3.Connection, market_id: str) -> list[sqlite3.Row]:
    sql = """
        SELECT
            s.market_id,
            s.market_slug,
            s.collected_ts AS ts_utc,
            s.seconds_to_resolution,
            s.best_bid_yes,
            s.best_ask_yes,
            s.best_bid_no,
            s.best_ask_no,
            s.mid_yes,
            s.mid_no,
            s.spread_yes,
            s.spread_no,
            s.best_bid_size_yes,
            s.best_ask_size_yes,
            s.best_bid_size_no,
            s.best_ask_size_no,
            s.complement_gap_mid,
            s.complement_gap_cross,
            s.quote_stable_pass_count,
            d.yes_bid_depth_5,
            d.yes_ask_depth_5,
            d.no_bid_depth_5,
            d.no_ask_depth_5
        FROM btc5m_snapshots s
        LEFT JOIN btc5m_orderbook_depth d
            ON d.market_id = s.market_id
           AND d.collected_ts = s.collected_ts
        WHERE s.market_id = ?
        ORDER BY s.collected_ts ASC
    """
    return list(conn.execute(sql, (market_id,)).fetchall())


def load_reference_rows(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> list[sqlite3.Row]:
    sql = """
        SELECT ts_utc, btc_price
        FROM btc5m_reference_ticks
        WHERE ts_utc BETWEEN ? AND ?
        ORDER BY ts_utc ASC
    """
    return list(conn.execute(sql, (start_ts, end_ts)).fetchall())


def safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator) / float(denominator)


def imbalance(bid_size: Any, ask_size: Any) -> Optional[float]:
    bid = safe_float(bid_size)
    ask = safe_float(ask_size)
    if bid is None or ask is None:
        return None
    total = bid + ask
    if total <= 0:
        return None
    return (bid - ask) / total


def depth_ratio(bid_depth: Any, ask_depth: Any) -> Optional[float]:
    bid = safe_float(bid_depth)
    ask = safe_float(ask_depth)
    if bid is None or ask is None:
        return None
    total = bid + ask
    if total <= 0:
        return None
    return (bid - ask) / total


def microprice(bid: Any, ask: Any, bid_size: Any, ask_size: Any) -> Optional[float]:
    bid_value = safe_float(bid)
    ask_value = safe_float(ask)
    bid_size_value = safe_float(bid_size)
    ask_size_value = safe_float(ask_size)
    if None in {bid_value, ask_value, bid_size_value, ask_size_value}:
        return None
    total_size = float(bid_size_value) + float(ask_size_value)
    if total_size <= 0:
        return None
    return ((float(ask_value) * float(bid_size_value)) + (float(bid_value) * float(ask_size_value))) / total_size


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def quote_stability_score(stable_pass_count: Any) -> float:
    stable_count = max(0, int(stable_pass_count or 0))
    return min(1.0, stable_count / float(MIN_STABLE_PASSES))


def bisect_reference_index(reference_ts: list[int], target_ts: int) -> int:
    return bisect_right(reference_ts, int(target_ts)) - 1


def reference_price_at_or_before(
    reference_rows: list[sqlite3.Row],
    reference_ts: list[int],
    target_ts: int,
) -> Optional[float]:
    idx = bisect_reference_index(reference_ts, target_ts)
    if idx < 0:
        return None
    if int(target_ts) - int(reference_ts[idx]) > MAX_REFERENCE_LAG_SEC:
        return None
    return safe_float(reference_rows[idx]["btc_price"])


def past_return(
    reference_rows: list[sqlite3.Row],
    reference_ts: list[int],
    current_ts: int,
    horizon_sec: int,
) -> Optional[float]:
    current_price = reference_price_at_or_before(reference_rows, reference_ts, current_ts)
    past_price = reference_price_at_or_before(reference_rows, reference_ts, current_ts - horizon_sec)
    if current_price is None or past_price in (None, 0):
        return None
    return (current_price / past_price) - 1.0


def historical_volatility(
    reference_rows: list[sqlite3.Row],
    reference_ts: list[int],
    current_ts: int,
    window_sec: int,
) -> Optional[float]:
    end_idx = bisect_reference_index(reference_ts, current_ts)
    if end_idx < 1:
        return None
    start_target = current_ts - window_sec
    start_idx = bisect_left(reference_ts, int(start_target))
    prices = [
        safe_float(reference_rows[idx]["btc_price"])
        for idx in range(start_idx, end_idx + 1)
        if safe_float(reference_rows[idx]["btc_price"]) is not None
    ]
    if len(prices) < 3:
        return None
    step_returns = [
        (prices[idx] / prices[idx - 1]) - 1.0
        for idx in range(1, len(prices))
        if prices[idx - 1] not in (None, 0)
    ]
    if len(step_returns) < 2:
        return None
    return float(statistics.pstdev(step_returns))


def feature_row_from_snapshot(
    snapshot_row: sqlite3.Row,
    reference_rows: list[sqlite3.Row],
    reference_ts: list[int],
    feature_version: str,
) -> dict[str, Any]:
    snapshot = dict(snapshot_row)
    ts_utc = int(snapshot["ts_utc"])
    spread_yes = safe_float(snapshot.get("spread_yes"))
    spread_no = safe_float(snapshot.get("spread_no"))
    complement_gap = safe_float(snapshot.get("complement_gap_mid"))
    if complement_gap is None:
        complement_gap = safe_float(snapshot.get("complement_gap_cross"))

    return {
        "market_id": snapshot["market_id"],
        "ts_utc": ts_utc,
        "seconds_to_resolution": int(snapshot["seconds_to_resolution"]),
        "return_15s": past_return(reference_rows, reference_ts, ts_utc, 15),
        "return_30s": past_return(reference_rows, reference_ts, ts_utc, 30),
        "return_60s": past_return(reference_rows, reference_ts, ts_utc, 60),
        "return_120s": past_return(reference_rows, reference_ts, ts_utc, 120),
        "volatility_30s": historical_volatility(reference_rows, reference_ts, ts_utc, 30),
        "volatility_60s": historical_volatility(reference_rows, reference_ts, ts_utc, 60),
        "volatility_180s": historical_volatility(reference_rows, reference_ts, ts_utc, 180),
        "microprice_yes": microprice(
            snapshot.get("best_bid_yes"),
            snapshot.get("best_ask_yes"),
            snapshot.get("best_bid_size_yes"),
            snapshot.get("best_ask_size_yes"),
        ),
        "microprice_no": microprice(
            snapshot.get("best_bid_no"),
            snapshot.get("best_ask_no"),
            snapshot.get("best_bid_size_no"),
            snapshot.get("best_ask_size_no"),
        ),
        "order_imbalance_yes": imbalance(snapshot.get("best_bid_size_yes"), snapshot.get("best_ask_size_yes")),
        "order_imbalance_no": imbalance(snapshot.get("best_bid_size_no"), snapshot.get("best_ask_size_no")),
        "complement_gap": complement_gap,
        "spread_sum": (spread_yes + spread_no) if spread_yes is not None and spread_no is not None else None,
        "depth_ratio_yes": depth_ratio(snapshot.get("yes_bid_depth_5"), snapshot.get("yes_ask_depth_5")),
        "depth_ratio_no": depth_ratio(snapshot.get("no_bid_depth_5"), snapshot.get("no_ask_depth_5")),
        "quote_stability_score": quote_stability_score(snapshot.get("quote_stable_pass_count")),
        "feature_version": feature_version,
    }


def delete_feature_rows(conn: sqlite3.Connection, market_id: str, feature_version: str) -> None:
    conn.execute(
        "DELETE FROM btc5m_features WHERE market_id=? AND feature_version=?",
        (market_id, feature_version),
    )
    conn.commit()


def insert_feature_rows(conn: sqlite3.Connection, feature_rows: list[dict[str, Any]]) -> int:
    if not feature_rows:
        return 0
    columns = list(feature_rows[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO btc5m_features ({', '.join(columns)}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(row[column] for column in columns) for row in feature_rows])
    conn.commit()
    return len(feature_rows)


def process_market(
    conn: sqlite3.Connection,
    market_row: sqlite3.Row,
    *,
    feature_version: str,
) -> int:
    market = dict(market_row)
    snapshots = load_market_snapshots(conn, str(market["market_id"]))
    if not snapshots:
        delete_feature_rows(conn, str(market["market_id"]), feature_version)
        return 0

    first_ts = int(snapshots[0]["ts_utc"])
    last_ts = int(snapshots[-1]["ts_utc"])
    reference_rows = load_reference_rows(
        conn,
        start_ts=first_ts - 180 - MAX_REFERENCE_LAG_SEC,
        end_ts=last_ts,
    )
    reference_ts = [int(row["ts_utc"]) for row in reference_rows]

    feature_rows = [
        feature_row_from_snapshot(snapshot_row, reference_rows, reference_ts, feature_version)
        for snapshot_row in snapshots
    ]
    delete_feature_rows(conn, str(market["market_id"]), feature_version)
    inserted = insert_feature_rows(conn, feature_rows)
    log(
        "FEATURES | slug=%s | feature_version=%s | rows=%s"
        % (market["market_slug"], feature_version, inserted)
    )
    return inserted


def main() -> None:
    args = parse_args()
    feature_version = str(args.feature_version or DEFAULT_FEATURE_VERSION).strip() or DEFAULT_FEATURE_VERSION
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args),
        meta_json={
            "feature_version": feature_version,
            "lookback_hours": int(args.lookback_hours),
            "max_markets": int(args.max_markets),
            "market_slug": str(args.market_slug or ""),
            "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
            "min_stable_passes": MIN_STABLE_PASSES,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    exit_status = "STOPPED"
    market_count = 0
    feature_count = 0
    error_count = 0

    try:
        now_ts = int(time.time())
        candidates = load_candidate_markets(
            conn,
            now_ts=now_ts,
            lookback_hours=args.lookback_hours,
            max_markets=args.max_markets,
            market_slug=str(args.market_slug or "").strip(),
        )
        log(
            "Feature build started | markets=%s | version=%s | db=%s"
            % (len(candidates), feature_version, resolve_db_path())
        )

        for market_row in candidates:
            try:
                feature_count += process_market(conn, market_row, feature_version=feature_version)
                market_count += 1
                update_collector_run(
                    conn,
                    run_id,
                    {
                        "market_count": market_count,
                        "snapshot_count": feature_count,
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
                        "snapshot_count": feature_count,
                        "error_count": error_count,
                        "status": "RUNNING",
                    },
                )
                log(f"WARN feature_build_failed | slug={market_row['market_slug']} | reason={exc}")

        log(
            "SUMMARY | status=%s | markets=%s | feature_rows=%s | version=%s"
            % ("COMPLETED" if error_count == 0 else "COMPLETED_WITH_ERRORS", market_count, feature_count, feature_version)
        )
        exit_status = "COMPLETED"
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status if error_count == 0 else "COMPLETED_WITH_ERRORS",
            market_count=market_count,
            snapshot_count=feature_count,
            error_count=error_count,
            meta_json={
                "feature_version": feature_version,
                "lookback_hours": int(args.lookback_hours),
                "max_markets": int(args.max_markets),
                "market_slug": str(args.market_slug or ""),
                "max_reference_lag_sec": MAX_REFERENCE_LAG_SEC,
                "min_stable_passes": MIN_STABLE_PASSES,
                "market_count": market_count,
                "feature_row_count": feature_count,
                "error_count": error_count,
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
