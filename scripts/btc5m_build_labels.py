"""Build official-resolution-based BTC5M labels from raw snapshots."""

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

from common.btc5m_dataset_db import connect_db, finish_collector_run, resolve_db_path, start_collector_run, update_collector_run
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-build-labels"
COLLECTOR_VERSION = "2026-03-15"
DEFAULT_LABEL_VERSION = str(os.getenv("BTC5M_LABEL_VERSION", "v1")).strip() or "v1"
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_LABEL_LOOKBACK_HOURS", "168")))
MIN_DECISION_HORIZON_SEC = max(0, int(os.getenv("BTC5M_LABEL_MIN_DECISION_HORIZON_SEC", "5")))
LOG_PATH = Path(os.getenv("BTC5M_LABEL_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_build_labels.log"))
LOCK_PATH = Path(os.getenv("BTC5M_LABEL_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_build_labels.lock"))

_logger = logging.getLogger("btc5m_build_labels")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-LABEL | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-LABEL | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build BTC5M labels from official resolution data.")
    parser.add_argument("--label-version", type=str, default=DEFAULT_LABEL_VERSION, help="Label version label.")
    parser.add_argument("--market-slug", type=str, default="", help="Only rebuild one market slug.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to rebuild labels.")
    parser.add_argument("--max-markets", type=int, default=250, help="Maximum number of markets to rebuild.")
    return parser.parse_args()


def collector_config_hash(args: argparse.Namespace) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "label_version": str(args.label_version),
        "db_path": str(resolve_db_path()),
        "lookback_hours": int(args.lookback_hours),
        "max_markets": int(args.max_markets),
        "market_slug": str(args.market_slug or ""),
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
) -> list[sqlite3.Row]:
    lower_bound = now_ts - (max(1, lookback_hours) * 3600)
    clauses = [
        "slot_end_ts >= ?",
        "market_resolution_status = 'RESOLVED'",
        "EXISTS (SELECT 1 FROM btc5m_snapshots s WHERE s.market_id = btc5m_markets.market_id)",
    ]
    params: list[Any] = [lower_bound]
    if market_slug:
        clauses.append("market_slug = ?")
        params.append(market_slug)
    sql = (
        "SELECT market_id, market_slug, slot_start_ts, slot_end_ts, market_resolution_status, "
        "resolved_outcome, resolved_yes_price, resolved_no_price, resolved_ts, label_quality_flag "
        "FROM btc5m_markets "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY slot_start_ts ASC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def load_market_snapshots(conn: sqlite3.Connection, market_id: str) -> list[sqlite3.Row]:
    sql = """
        SELECT
            collected_ts,
            seconds_to_resolution,
            best_ask_yes,
            best_ask_no,
            best_bid_yes,
            best_bid_no,
            book_valid,
            orderbook_exists_yes,
            orderbook_exists_no,
            market_status
        FROM btc5m_snapshots
        WHERE market_id = ?
        ORDER BY collected_ts ASC
    """
    return list(conn.execute(sql, (market_id,)).fetchall())


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def delete_label_rows(conn: sqlite3.Connection, market_id: str, label_version: str) -> None:
    conn.execute(
        "DELETE FROM btc5m_labels WHERE market_id=? AND label_version=?",
        (market_id, label_version),
    )
    conn.commit()


def insert_label_rows(conn: sqlite3.Connection, label_rows: list[dict[str, Any]]) -> int:
    if not label_rows:
        return 0
    columns = list(label_rows[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO btc5m_labels ({', '.join(columns)}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(row[column] for column in columns) for row in label_rows])
    conn.commit()
    return len(label_rows)


def official_resolution_complete(market: dict[str, Any]) -> bool:
    return (
        str(market.get("market_resolution_status") or "") == "RESOLVED"
        and market.get("resolved_outcome") not in (None, "")
        and market.get("resolved_yes_price") is not None
        and market.get("resolved_no_price") is not None
        and market.get("resolved_ts") is not None
    )


def future_valid_rows(snapshots: list[dict[str, Any]], current_ts: int, slot_end_ts: int) -> list[dict[str, Any]]:
    return [
        row
        for row in snapshots
        if int(row["collected_ts"]) >= current_ts
        and int(row["collected_ts"]) <= slot_end_ts
        and int(row.get("book_valid") or 0) == 1
    ]


def best_exit_and_time(rows: list[dict[str, Any]], price_key: str, decision_ts: int) -> tuple[Optional[float], Optional[float]]:
    valid = [
        (safe_float(row.get(price_key)), int(row["collected_ts"]))
        for row in rows
        if safe_float(row.get(price_key)) is not None
    ]
    if not valid:
        return None, None
    best_price = max(price for price, _ in valid)
    best_ts = next(ts for price, ts in valid if price == best_price)
    return best_price, float(best_ts - decision_ts)


def winning_side(market: dict[str, Any]) -> Optional[str]:
    yes_price = safe_float(market.get("resolved_yes_price"))
    no_price = safe_float(market.get("resolved_no_price"))
    if yes_price is None or no_price is None:
        return None
    if yes_price > no_price:
        return "YES"
    if no_price > yes_price:
        return "NO"
    return None


def tp_sl_flags(
    side: Optional[str],
    entry_yes: Optional[float],
    entry_no: Optional[float],
    future_rows: list[dict[str, Any]],
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    if side == "YES":
        entry_price = entry_yes
        exit_prices = [safe_float(row.get("best_bid_yes")) for row in future_rows]
    elif side == "NO":
        entry_price = entry_no
        exit_prices = [safe_float(row.get("best_bid_no")) for row in future_rows]
    else:
        return None, None, None, None

    exit_prices = [price for price in exit_prices if price is not None]
    if entry_price is None or not exit_prices:
        return None, None, None, None

    def hit_tp(threshold: float) -> int:
        return int(any(price >= entry_price + threshold for price in exit_prices))

    def hit_sl(threshold: float) -> int:
        return int(any(price <= entry_price - threshold for price in exit_prices))

    return hit_tp(0.05), hit_tp(0.10), hit_sl(0.05), hit_sl(0.10)


def label_quality_flag(
    market: dict[str, Any],
    entry_yes: Optional[float],
    entry_no: Optional[float],
    best_exit_yes: Optional[float],
    best_exit_no: Optional[float],
) -> str:
    market_flag = str(market.get("label_quality_flag") or "").strip()
    if not official_resolution_complete(market):
        return "MISSING_OFFICIAL_RESOLUTION"
    if market_flag and market_flag != "OFFICIAL_RESOLVED":
        return market_flag
    if entry_yes is None and entry_no is None:
        return "MISSING_ENTRY_PRICE"
    if best_exit_yes is None and best_exit_no is None:
        return "MISSING_PATH_DATA"
    return "OFFICIAL_RESOLVED"


def build_label_rows_for_market(market_row: sqlite3.Row, snapshot_rows: list[sqlite3.Row], label_version: str) -> list[dict[str, Any]]:
    market = dict(market_row)
    snapshots = [dict(row) for row in snapshot_rows]
    if not snapshots:
        return []

    slot_end_ts = int(market["slot_end_ts"])
    resolved_yes_price = safe_float(market.get("resolved_yes_price"))
    resolved_no_price = safe_float(market.get("resolved_no_price"))
    terminal_outcome = str(market.get("resolved_outcome") or "UNKNOWN")
    winner = winning_side(market)
    rows: list[dict[str, Any]] = []

    for snapshot in snapshots:
        decision_ts = int(snapshot["collected_ts"])
        decision_horizon = int(snapshot.get("seconds_to_resolution") or max(0, slot_end_ts - decision_ts))
        entry_yes = safe_float(snapshot.get("best_ask_yes"))
        entry_no = safe_float(snapshot.get("best_ask_no"))
        valid_path = future_valid_rows(snapshots, decision_ts, slot_end_ts)

        best_exit_yes, time_to_best_yes = best_exit_and_time(valid_path, "best_bid_yes", decision_ts)
        best_exit_no, time_to_best_no = best_exit_and_time(valid_path, "best_bid_no", decision_ts)
        tp_5c, tp_10c, sl_5c, sl_10c = tp_sl_flags(winner, entry_yes, entry_no, valid_path)
        quality_flag = label_quality_flag(market, entry_yes, entry_no, best_exit_yes, best_exit_no)

        rows.append(
            {
                "market_id": market["market_id"],
                "decision_ts": decision_ts,
                "label_horizon_sec": decision_horizon,
                "terminal_outcome": terminal_outcome,
                "resolved_yes_price": resolved_yes_price,
                "resolved_no_price": resolved_no_price,
                "mtm_return_if_buy_yes_hold_to_resolution": (
                    (resolved_yes_price - entry_yes) if resolved_yes_price is not None and entry_yes is not None else None
                ),
                "mtm_return_if_buy_no_hold_to_resolution": (
                    (resolved_no_price - entry_no) if resolved_no_price is not None and entry_no is not None else None
                ),
                "best_exit_yes_before_expiry": best_exit_yes,
                "best_exit_no_before_expiry": best_exit_no,
                "would_hit_tp_5c": tp_5c,
                "would_hit_tp_10c": tp_10c,
                "would_hit_sl_5c": sl_5c,
                "would_hit_sl_10c": sl_10c,
                "time_to_best_yes_sec": time_to_best_yes,
                "time_to_best_no_sec": time_to_best_no,
                "label_quality_flag": quality_flag,
                "label_version": label_version,
            }
        )

    return rows


def process_market(conn: sqlite3.Connection, market_row: sqlite3.Row, label_version: str) -> int:
    market = dict(market_row)
    snapshots = load_market_snapshots(conn, str(market["market_id"]))
    label_rows = build_label_rows_for_market(market_row, snapshots, label_version)
    delete_label_rows(conn, str(market["market_id"]), label_version)
    inserted = insert_label_rows(conn, label_rows)
    log(
        "LABELS | slug=%s | label_version=%s | rows=%s"
        % (market["market_slug"], label_version, inserted)
    )
    return inserted


def main() -> None:
    args = parse_args()
    label_version = str(args.label_version or DEFAULT_LABEL_VERSION).strip() or DEFAULT_LABEL_VERSION
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args),
        meta_json={
            "label_version": label_version,
            "lookback_hours": int(args.lookback_hours),
            "max_markets": int(args.max_markets),
            "market_slug": str(args.market_slug or ""),
            "min_decision_horizon_sec": MIN_DECISION_HORIZON_SEC,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    exit_status = "STOPPED"
    market_count = 0
    label_count = 0
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
            "Label build started | markets=%s | version=%s | db=%s"
            % (len(candidates), label_version, resolve_db_path())
        )

        for market_row in candidates:
            try:
                label_count += process_market(conn, market_row, label_version)
                market_count += 1
                update_collector_run(
                    conn,
                    run_id,
                    {
                        "market_count": market_count,
                        "snapshot_count": label_count,
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
                        "snapshot_count": label_count,
                        "error_count": error_count,
                        "status": "RUNNING",
                    },
                )
                log(f"WARN label_build_failed | slug={market_row['market_slug']} | reason={exc}")

        log(
            "SUMMARY | status=%s | markets=%s | label_rows=%s | version=%s"
            % ("COMPLETED" if error_count == 0 else "COMPLETED_WITH_ERRORS", market_count, label_count, label_version)
        )
        exit_status = "COMPLETED"
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status if error_count == 0 else "COMPLETED_WITH_ERRORS",
            market_count=market_count,
            snapshot_count=label_count,
            error_count=error_count,
            meta_json={
                "label_version": label_version,
                "lookback_hours": int(args.lookback_hours),
                "max_markets": int(args.max_markets),
                "market_slug": str(args.market_slug or ""),
                "min_decision_horizon_sec": MIN_DECISION_HORIZON_SEC,
                "market_count": market_count,
                "label_row_count": label_count,
                "error_count": error_count,
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
