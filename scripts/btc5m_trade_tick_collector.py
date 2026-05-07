"""Collect public Polymarket trade ticks into the BTC5M dataset DB."""

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
    insert_trade_tick,
    resolve_db_path,
    resolve_repo_path,
    start_collector_run,
    update_collector_run,
)
from common.btc5m_trade_tick_feed import (
    DATA_API_BASE_URL,
    DEFAULT_SOURCE_NAME,
    DEFAULT_TIMEOUT_SEC,
    DEFAULT_USER_AGENT,
    MAX_OFFSET,
    PAGE_LIMIT,
    TradeTickFeedError,
    build_trade_tick_session,
    iter_market_trades,
    normalize_trade_row,
)
from common.bot_notify import send_alert
from common.network_diagnostics import (
    build_network_intervention_message,
    clear_network_alert_state,
    is_network_reason,
    note_network_alert_state,
)
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-trade-tick-collector"
COLLECTOR_VERSION = "2026-05-05"
SOURCE_NAME = str(os.getenv("BTC5M_TRADE_TICK_SOURCE_NAME", DEFAULT_SOURCE_NAME)).strip() or DEFAULT_SOURCE_NAME
BASE_URL = str(os.getenv("BTC5M_TRADE_TICK_BASE_URL", DATA_API_BASE_URL)).strip() or DATA_API_BASE_URL
INTERVAL_SEC = max(5, int(os.getenv("BTC5M_TRADE_TICK_INTERVAL_SEC", "15")))
LIVE_LOOKBACK_SEC = max(60, int(os.getenv("BTC5M_TRADE_TICK_LIVE_LOOKBACK_SEC", "7200")))
TIMEOUT_SEC = max(1, int(os.getenv("BTC5M_TRADE_TICK_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))))
RETRY_COUNT = max(0, int(os.getenv("BTC5M_TRADE_TICK_RETRY_COUNT", "2")))
RETRY_BACKOFF_SEC = max(0.0, float(os.getenv("BTC5M_TRADE_TICK_RETRY_BACKOFF_SEC", "1.0")))
REQUEST_SLEEP_SEC = max(0.0, float(os.getenv("BTC5M_TRADE_TICK_REQUEST_SLEEP_SEC", "0.1")))
TAKER_ONLY = str(os.getenv("BTC5M_TRADE_TICK_TAKER_ONLY", "true")).strip().lower() in ("1", "true", "yes")
DEFAULT_LIVE_MAX_MARKETS = max(1, int(os.getenv("BTC5M_TRADE_TICK_MAX_MARKETS", "25")))
DEFAULT_HISTORICAL_MAX_MARKETS = max(1, int(os.getenv("BTC5M_TRADE_TICK_HISTORICAL_MAX_MARKETS", "200")))

_cutoff_raw = str(os.getenv("BTC5M_TRADE_TICK_HISTORICAL_CUTOFF_TS", "")).strip()
HISTORICAL_CUTOFF_TS: Optional[int] = int(_cutoff_raw) if _cutoff_raw.isdigit() else None

LOG_PATH = resolve_repo_path(
    os.getenv("BTC5M_TRADE_TICK_LOG_PATH"),
    default_path=ROOT_DIR / "runtime" / "logs" / "btc5m_trade_tick_collector.log",
)
LOCK_PATH = resolve_repo_path(
    os.getenv("BTC5M_TRADE_TICK_LOCK_PATH"),
    default_path=ROOT_DIR / "runtime" / "locks" / "btc5m_trade_tick_collector.lock",
)
ALERT_DEDUPE_SEC = max(120, int(os.getenv("BTC5M_TRADE_TICK_ALERT_DEDUPE_SEC", "600")))
NETWORK_ALERT_THRESHOLD = max(2, int(os.getenv("BTC5M_TRADE_TICK_NETWORK_ALERT_THRESHOLD", "3")))
NETWORK_ALERT_MIN_DURATION_SEC = max(15, int(os.getenv("BTC5M_TRADE_TICK_NETWORK_ALERT_MIN_DURATION_SEC", "30")))
NETWORK_ALERT_RESET_SEC = max(30, int(os.getenv("BTC5M_TRADE_TICK_NETWORK_ALERT_RESET_SEC", "120")))
NETWORK_ALERT_STATE_KEY = "btc5m-trade-tick-network"

_logger = logging.getLogger("btc5m_trade_tick_collector")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-TRADE | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-TRADE | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public Polymarket trade ticks into the BTC5M dataset DB.")
    parser.add_argument("--once", action="store_true", help="Run a single sweep and exit (live mode).")
    parser.add_argument("--historical", action="store_true", help="One-shot oldest->newest backfill of trade ticks.")
    parser.add_argument("--max-markets", type=int, default=0, help="Per-loop / per-batch cap (0 = use env default).")
    parser.add_argument("--market-slug", type=str, default="", help="Restrict to one market slug.")
    parser.add_argument("--from-ts", type=int, default=0, help="Historical mode lower bound (slot_start_ts >=).")
    parser.add_argument("--to-ts", type=int, default=0, help="Historical mode upper bound (slot_end_ts <=).")
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=0,
        help="Override live lookback window in hours (0 = use env LIVE_LOOKBACK_SEC).",
    )
    return parser.parse_args()


def collector_config_hash(args: argparse.Namespace, *, mode: str, max_markets: int) -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "source_name": SOURCE_NAME,
        "base_url": BASE_URL,
        "interval_sec": INTERVAL_SEC,
        "timeout_sec": TIMEOUT_SEC,
        "retry_count": RETRY_COUNT,
        "retry_backoff_sec": RETRY_BACKOFF_SEC,
        "request_sleep_sec": REQUEST_SLEEP_SEC,
        "taker_only": TAKER_ONLY,
        "live_lookback_sec": LIVE_LOOKBACK_SEC,
        "historical_cutoff_ts": HISTORICAL_CUTOFF_TS,
        "mode": mode,
        "max_markets": max_markets,
        "market_slug": str(args.market_slug or ""),
        "from_ts": int(args.from_ts or 0),
        "to_ts": int(args.to_ts or 0),
        "db_path": str(resolve_db_path()),
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_live_markets(
    conn: sqlite3.Connection,
    *,
    now_ts: int,
    lookback_sec: int,
    max_markets: int,
    market_slug: str,
) -> list[sqlite3.Row]:
    clauses = [
        "slot_end_ts >= ?",
        "slot_start_ts <= ?",
    ]
    params: list[Any] = [now_ts - lookback_sec, now_ts + 300]
    if market_slug:
        clauses.append("market_slug = ?")
        params.append(market_slug)
    sql = (
        "SELECT market_id, market_slug, slot_start_ts, slot_end_ts, yes_token_id, no_token_id "
        "FROM btc5m_markets "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY slot_start_ts DESC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def load_historical_markets(
    conn: sqlite3.Connection,
    *,
    cutoff_ts: int,
    from_ts: int,
    to_ts: int,
    max_markets: int,
    market_slug: str,
    after_slot_start_ts: Optional[int],
) -> list[sqlite3.Row]:
    clauses = ["slot_end_ts < ?"]
    params: list[Any] = [cutoff_ts]
    if from_ts > 0:
        clauses.append("slot_start_ts >= ?")
        params.append(from_ts)
    if to_ts > 0:
        clauses.append("slot_end_ts <= ?")
        params.append(to_ts)
    if market_slug:
        clauses.append("market_slug = ?")
        params.append(market_slug)
    if after_slot_start_ts is not None:
        clauses.append("slot_start_ts > ?")
        params.append(after_slot_start_ts)
    sql = (
        "SELECT market_id, market_slug, slot_start_ts, slot_end_ts, yes_token_id, no_token_id "
        "FROM btc5m_markets "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY slot_start_ts ASC "
        "LIMIT ?"
    )
    params.append(max(1, max_markets))
    return list(conn.execute(sql, params).fetchall())


def fetch_and_store_side(
    conn: sqlite3.Connection,
    session,
    market_row: sqlite3.Row,
    *,
    side: str,
    now_ts: int,
) -> tuple[int, int, bool]:
    """Fetch all trades for a market+side, slot-filter, insert. Returns (inserted, rejected, partial)."""
    inserted = 0
    rejected = 0
    partial = False
    market_id = str(market_row["market_id"])
    market_slug = str(market_row["market_slug"])
    slot_start = int(market_row["slot_start_ts"])
    slot_end = int(market_row["slot_end_ts"])
    yes_token_id = str(market_row["yes_token_id"] or "")
    no_token_id = str(market_row["no_token_id"] or "")

    pending: list[dict[str, Any]] = []
    try:
        for api_row in iter_market_trades(
            session,
            market_id=market_id,
            side=side,
            taker_only=TAKER_ONLY,
            limit=PAGE_LIMIT,
            max_offset=MAX_OFFSET,
            base_url=BASE_URL,
            timeout_sec=TIMEOUT_SEC,
            retry_count=RETRY_COUNT,
            retry_backoff_sec=RETRY_BACKOFF_SEC,
            request_sleep_sec=REQUEST_SLEEP_SEC,
        ):
            normalized = normalize_trade_row(
                api_row,
                market_id=market_id,
                market_slug=market_slug,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                source_name=SOURCE_NAME,
                collected_ts=now_ts,
            )
            if normalized is None:
                rejected += 1
                continue
            ts_utc = int(normalized["ts_utc"])
            if ts_utc < slot_start or ts_utc >= slot_end:
                rejected += 1
                continue
            pending.append(normalized)
    except TradeTickFeedError as exc:
        if str(exc) == "trades_offset_cap_partial":
            partial = True
        else:
            raise

    if pending:
        inserted = bulk_insert_trade_ticks(conn, pending)
    return inserted, rejected, partial


def bulk_insert_trade_ticks(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    """Single-transaction batch insert. Returns count of newly inserted rows."""
    if not rows:
        return 0
    from common.btc5m_dataset_db import TABLE_SPECS, _normalize_value
    allowed = TABLE_SPECS["btc5m_trade_ticks"]["columns"]
    columns = [c for c in allowed.keys() if c in rows[0]]
    sql = f"INSERT OR IGNORE INTO btc5m_trade_ticks ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
    payload = [tuple(_normalize_value(row.get(col)) for col in columns) for row in rows]
    before = conn.total_changes
    conn.executemany(sql, payload)
    conn.commit()
    return int(conn.total_changes - before)


def process_market(
    conn: sqlite3.Connection,
    session,
    market_row: sqlite3.Row,
    *,
    now_ts: int,
) -> tuple[int, int, bool]:
    total_inserted = 0
    total_rejected = 0
    partial = False
    for side in ("BUY", "SELL"):
        ins, rej, side_partial = fetch_and_store_side(
            conn, session, market_row, side=side, now_ts=now_ts,
        )
        total_inserted += ins
        total_rejected += rej
        partial = partial or side_partial
    return total_inserted, total_rejected, partial


def update_run_metrics(conn: sqlite3.Connection, run_id: str, stats: dict[str, Any]) -> None:
    meta_json: dict[str, Any] = {
        "rows_inserted": stats["rows_inserted"],
        "rows_rejected": stats["rows_rejected"],
        "partial_markets": stats["partial_markets"],
        "mode": stats["mode"],
        "first_success_ts": stats.get("first_success_ts"),
    }
    if stats.get("last_error_ts") is not None:
        meta_json["last_error_ts"] = int(stats["last_error_ts"])
    if stats.get("last_error_reason"):
        meta_json["last_error_reason"] = str(stats["last_error_reason"])
    update_collector_run(
        conn,
        run_id,
        {
            "market_count": stats["processed_count"],
            "error_count": stats["error_count"],
            "status": "RUNNING",
            "meta_json": meta_json,
        },
    )


def safe_update_run_metrics(conn: sqlite3.Connection, run_id: str, stats: dict[str, Any]) -> None:
    try:
        update_run_metrics(conn, run_id, stats)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log(f"WARN run_metrics_update_failed | reason={exc}")


def handle_feed_error(
    exc: TradeTickFeedError,
    *,
    market_slug: str,
) -> None:
    if is_network_reason(exc):
        state = note_network_alert_state(
            NETWORK_ALERT_STATE_KEY,
            str(exc),
            source=SOURCE_NAME,
            threshold_count=NETWORK_ALERT_THRESHOLD,
            min_duration_sec=NETWORK_ALERT_MIN_DURATION_SEC,
            reset_after_sec=NETWORK_ALERT_RESET_SEC,
        )
        if state["should_alert"]:
            send_alert(
                bot_label="BTC5M-TRADE",
                msg=build_network_intervention_message(
                    "Trade tick collector",
                    state["reason"],
                    source=str(state["source"] or SOURCE_NAME),
                    failure_count=int(state["count"]),
                    duration_sec=int(state["duration_sec"]),
                    extra=f"slug={market_slug}",
                ),
                level="WARN",
                dedupe_seconds=ALERT_DEDUPE_SEC,
            )


def run_historical(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    session,
    run_id: str,
    stats: dict[str, Any],
    max_markets: int,
) -> str:
    cutoff_ts = int(args.to_ts) if args.to_ts > 0 else (HISTORICAL_CUTOFF_TS if HISTORICAL_CUTOFF_TS is not None else int(time.time()))
    checkpoint_every = max(1, int(os.getenv("BTC5M_TRADE_TICK_HISTORICAL_CHECKPOINT_EVERY", "25")))
    log(
        "Trade tick historical fill | cutoff_ts=%s | from_ts=%s | to_ts=%s | batch=%s | slug=%s | checkpoint_every=%s"
        % (cutoff_ts, args.from_ts, args.to_ts, max_markets, args.market_slug or "-", checkpoint_every)
    )

    after_slot_start_ts: Optional[int] = None
    markets_since_checkpoint = 0
    while True:
        candidates = load_historical_markets(
            conn,
            cutoff_ts=cutoff_ts,
            from_ts=int(args.from_ts or 0),
            to_ts=int(args.to_ts or 0),
            max_markets=max_markets,
            market_slug=str(args.market_slug or "").strip(),
            after_slot_start_ts=after_slot_start_ts,
        )
        if not candidates:
            log("Historical backfill complete: no more candidate markets.")
            return "COMPLETED"
        for market_row in candidates:
            now_ts = int(time.time())
            try:
                inserted, rejected, partial = process_market(conn, session, market_row, now_ts=now_ts)
                stats["processed_count"] += 1
                stats["rows_inserted"] += inserted
                stats["rows_rejected"] += rejected
                if partial:
                    stats["partial_markets"] += 1
                if stats.get("first_success_ts") is None:
                    stats["first_success_ts"] = now_ts
                clear_network_alert_state(NETWORK_ALERT_STATE_KEY)
                safe_update_run_metrics(conn, run_id, stats)
                log(
                    "HIST | slug=%s | slot=%s-%s | inserted=%s | rejected=%s | partial=%s"
                    % (
                        market_row["market_slug"],
                        market_row["slot_start_ts"],
                        market_row["slot_end_ts"],
                        inserted,
                        rejected,
                        partial,
                    )
                )
                markets_since_checkpoint += 1
                if markets_since_checkpoint >= checkpoint_every:
                    try:
                        cp = conn.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
                        log(f"WAL_CHECKPOINT | result={cp}")
                    except Exception as cp_exc:
                        log(f"WARN wal_checkpoint_failed | reason={cp_exc}")
                    markets_since_checkpoint = 0
            except KeyboardInterrupt:
                raise
            except TradeTickFeedError as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                stats["error_count"] += 1
                stats["last_error_ts"] = int(time.time())
                stats["last_error_reason"] = str(exc)
                safe_update_run_metrics(conn, run_id, stats)
                log(f"WARN hist_fetch_failed | slug={market_row['market_slug']} | reason={exc}")
                handle_feed_error(exc, market_slug=str(market_row["market_slug"]))
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                stats["error_count"] += 1
                stats["last_error_ts"] = int(time.time())
                stats["last_error_reason"] = str(exc)
                safe_update_run_metrics(conn, run_id, stats)
                log(f"Runtime Error | slug={market_row['market_slug']} | reason={exc}")
            after_slot_start_ts = int(market_row["slot_start_ts"])


def run_live(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    session,
    run_id: str,
    stats: dict[str, Any],
    max_markets: int,
) -> str:
    lookback_sec = int(args.lookback_hours) * 3600 if args.lookback_hours > 0 else LIVE_LOOKBACK_SEC
    log(
        "Trade tick live collector started | source=%s | interval=%ss | lookback=%ss | retry=%s | db=%s"
        % (SOURCE_NAME, INTERVAL_SEC, lookback_sec, RETRY_COUNT, resolve_db_path())
    )
    exit_status = "STOPPED"
    while True:
        loop_started_at = time.perf_counter()
        try:
            now_ts = int(time.time())
            candidates = load_live_markets(
                conn,
                now_ts=now_ts,
                lookback_sec=lookback_sec,
                max_markets=max_markets,
                market_slug=str(args.market_slug or "").strip(),
            )
            if not candidates:
                log("No live markets in lookback window.")
            for market_row in candidates:
                try:
                    inserted, rejected, partial = process_market(conn, session, market_row, now_ts=now_ts)
                    stats["processed_count"] += 1
                    stats["rows_inserted"] += inserted
                    stats["rows_rejected"] += rejected
                    if partial:
                        stats["partial_markets"] += 1
                    if stats.get("first_success_ts") is None:
                        stats["first_success_ts"] = now_ts
                    clear_network_alert_state(NETWORK_ALERT_STATE_KEY)
                    safe_update_run_metrics(conn, run_id, stats)
                    if inserted > 0 or rejected > 0:
                        log(
                            "LIVE | slug=%s | inserted=%s | rejected=%s | partial=%s"
                            % (market_row["market_slug"], inserted, rejected, partial)
                        )
                except KeyboardInterrupt:
                    exit_status = "STOPPED"
                    raise SystemExit(0)
                except TradeTickFeedError as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    stats["error_count"] += 1
                    stats["last_error_ts"] = int(time.time())
                    stats["last_error_reason"] = str(exc)
                    safe_update_run_metrics(conn, run_id, stats)
                    log(f"WARN trade_fetch_failed | slug={market_row['market_slug']} | reason={exc}")
                    handle_feed_error(exc, market_slug=str(market_row["market_slug"]))
                    if args.once:
                        exit_status = "FAILED"
                        raise SystemExit(1)
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    stats["error_count"] += 1
                    stats["last_error_ts"] = int(time.time())
                    stats["last_error_reason"] = str(exc)
                    safe_update_run_metrics(conn, run_id, stats)
                    log(f"Runtime Error | slug={market_row['market_slug']} | reason={exc}")
                    if args.once:
                        exit_status = "FAILED"
                        raise
        except KeyboardInterrupt:
            exit_status = "STOPPED"
            raise SystemExit(0)
        except SystemExit:
            raise
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            stats["error_count"] += 1
            stats["last_error_ts"] = int(time.time())
            stats["last_error_reason"] = str(exc)
            safe_update_run_metrics(conn, run_id, stats)
            log(f"Runtime Error | reason={exc}")
            if args.once:
                exit_status = "FAILED"
                raise

        if args.once:
            return "COMPLETED"

        sleep_sec = max(0.0, INTERVAL_SEC - (time.perf_counter() - loop_started_at))
        time.sleep(sleep_sec)


def main() -> None:
    args = parse_args()
    mode = "historical" if args.historical else "live"
    if args.max_markets and args.max_markets > 0:
        max_markets = int(args.max_markets)
    else:
        max_markets = DEFAULT_HISTORICAL_MAX_MARKETS if mode == "historical" else DEFAULT_LIVE_MAX_MARKETS

    if mode == "historical":
        historical_lock_path = LOCK_PATH.with_name(LOCK_PATH.stem + "_historical" + LOCK_PATH.suffix)
        acquire_single_instance_lock(
            str(historical_lock_path),
            process_name=f"{COLLECTOR_NAME}-historical",
            on_log=log,
            takeover=False,
        )
    else:
        acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    session = build_trade_tick_session(DEFAULT_USER_AGENT)
    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(args, mode=mode, max_markets=max_markets),
        meta_json={
            "source_name": SOURCE_NAME,
            "base_url": BASE_URL,
            "mode": mode,
            "interval_sec": INTERVAL_SEC,
            "live_lookback_sec": LIVE_LOOKBACK_SEC,
            "historical_cutoff_ts": HISTORICAL_CUTOFF_TS,
            "retry_count": RETRY_COUNT,
            "retry_backoff_sec": RETRY_BACKOFF_SEC,
            "request_sleep_sec": REQUEST_SLEEP_SEC,
            "taker_only": TAKER_ONLY,
            "max_markets": max_markets,
            "market_slug": str(args.market_slug or ""),
            "from_ts": int(args.from_ts or 0),
            "to_ts": int(args.to_ts or 0),
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    stats: dict[str, Any] = {
        "processed_count": 0,
        "rows_inserted": 0,
        "rows_rejected": 0,
        "partial_markets": 0,
        "error_count": 0,
        "last_error_ts": None,
        "last_error_reason": None,
        "first_success_ts": None,
        "mode": mode,
    }
    exit_status = "STOPPED"

    try:
        if mode == "historical":
            exit_status = run_historical(args, conn, session, run_id, stats, max_markets)
        else:
            exit_status = run_live(args, conn, session, run_id, stats, max_markets)
    except SystemExit as exc:
        if exit_status == "STOPPED":
            exit_status = "FAILED" if exc.code not in (0, None) else "COMPLETED"
        raise
    except KeyboardInterrupt:
        exit_status = "STOPPED"
        raise
    except Exception:
        exit_status = "FAILED"
        raise
    finally:
        try:
            finish_collector_run(
                conn,
                run_id,
                status=exit_status,
                market_count=stats["processed_count"],
                error_count=stats["error_count"],
                meta_json={
                    "source_name": SOURCE_NAME,
                    "base_url": BASE_URL,
                    "mode": stats["mode"],
                    "rows_inserted": stats["rows_inserted"],
                    "rows_rejected": stats["rows_rejected"],
                    "partial_markets": stats["partial_markets"],
                    "first_success_ts": stats.get("first_success_ts"),
                    "last_error_ts": stats.get("last_error_ts"),
                    "last_error_reason": stats.get("last_error_reason"),
                    "market_slug": str(args.market_slug or ""),
                    "log_path": str(LOG_PATH),
                    "db_path": str(resolve_db_path()),
                },
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()
