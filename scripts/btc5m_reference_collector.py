"""Collects BTC reference ticks into the BTC5M dataset DB."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import (
    connect_db,
    finish_collector_run,
    insert_reference_ohlcv,
    insert_reference_tick,
    resolve_db_path,
    start_collector_run,
    update_collector_run,
)
from common.btc5m_reference_feed import (
    BINANCE_SPOT_BASE_URL,
    DEFAULT_SOURCE_NAME,
    DEFAULT_SYMBOL,
    DEFAULT_TIMEOUT_SEC,
    ReferenceFeedError,
    ReferenceOhlcvAggregator,
    build_reference_session,
    fetch_binance_spot_reference_tick,
    normalize_symbol,
)
from common.bot_notify import send_alert
from common.network_diagnostics import (
    build_network_intervention_message,
    clear_network_alert_state,
    is_network_reason,
    note_network_alert_state,
)
from common.single_instance import acquire_single_instance_lock

SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

COLLECTOR_NAME = "btc5m-reference-collector"
COLLECTOR_VERSION = "2026-03-15"
INTERVAL_SEC = max(1, int(os.getenv("BTC5M_REFERENCE_INTERVAL_SEC", "1")))
TIMEOUT_SEC = max(1, int(os.getenv("BTC5M_REFERENCE_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))))
SYMBOL = normalize_symbol(os.getenv("BTC5M_REFERENCE_SYMBOL", DEFAULT_SYMBOL))
SOURCE_NAME = str(os.getenv("BTC5M_REFERENCE_SOURCE_NAME", DEFAULT_SOURCE_NAME)).strip() or DEFAULT_SOURCE_NAME
BASE_URL = str(os.getenv("BTC5M_REFERENCE_BASE_URL", BINANCE_SPOT_BASE_URL)).strip() or BINANCE_SPOT_BASE_URL
LOG_PATH = Path(os.getenv("BTC5M_REFERENCE_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_reference_collector.log"))
LOCK_PATH = Path(os.getenv("BTC5M_REFERENCE_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_reference_collector.lock"))
ALERT_DEDUPE_SEC = max(120, int(os.getenv("BTC5M_REFERENCE_ALERT_DEDUPE_SEC", "600")))
NETWORK_ALERT_THRESHOLD = max(3, int(os.getenv("BTC5M_REFERENCE_NETWORK_ALERT_THRESHOLD", "3")))
NETWORK_ALERT_MIN_DURATION_SEC = max(10, int(os.getenv("BTC5M_REFERENCE_NETWORK_ALERT_MIN_DURATION_SEC", "15")))
NETWORK_ALERT_RESET_SEC = max(30, int(os.getenv("BTC5M_REFERENCE_NETWORK_ALERT_RESET_SEC", "60")))
NETWORK_ALERT_STATE_KEY = "btc5m-reference-network"

_logger = logging.getLogger("btc5m_reference_collector")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-REF | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-REF | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


def collector_config_hash() -> str:
    payload = {
        "collector_name": COLLECTOR_NAME,
        "collector_version": COLLECTOR_VERSION,
        "symbol": SYMBOL,
        "source_name": SOURCE_NAME,
        "base_url": BASE_URL,
        "interval_sec": INTERVAL_SEC,
        "timeout_sec": TIMEOUT_SEC,
        "db_path": str(resolve_db_path()),
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def update_run_metrics(conn, run_id: str, *, reference_tick_count: int, error_count: int) -> None:
    update_collector_run(
        conn,
        run_id,
        {
            "reference_tick_count": reference_tick_count,
            "error_count": error_count,
            "status": "RUNNING",
        },
    )


def maybe_insert_completed_candle(conn, aggregator: ReferenceOhlcvAggregator, tick_row: dict) -> int:
    candle_row = aggregator.update(tick_row)
    if not candle_row:
        return 0
    if bool((candle_row.get("meta_json") or {}).get("partial")):
        return 0
    return insert_reference_ohlcv(conn, candle_row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect BTC reference ticks into the BTC5M dataset.")
    parser.add_argument("--once", action="store_true", help="Fetch a single tick and exit.")
    parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N successful tick writes.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    acquire_single_instance_lock(str(LOCK_PATH), process_name=COLLECTOR_NAME, on_log=log, takeover=True)

    session = build_reference_session()
    conn = connect_db()
    run_id = start_collector_run(
        conn,
        collector_name=COLLECTOR_NAME,
        collector_version=COLLECTOR_VERSION,
        config_hash=collector_config_hash(),
        meta_json={
            "symbol": SYMBOL,
            "source_name": SOURCE_NAME,
            "base_url": BASE_URL,
            "log_path": str(LOG_PATH),
            "db_path": str(resolve_db_path()),
        },
    )

    tick_count = 0
    error_count = 0
    exit_status = "STOPPED"
    aggregator = ReferenceOhlcvAggregator(source_name=SOURCE_NAME, symbol=SYMBOL)

    log(
        "Reference collector started | symbol=%s | source=%s | interval=%ss | db=%s"
        % (SYMBOL, SOURCE_NAME, INTERVAL_SEC, resolve_db_path())
    )

    try:
        while True:
            loop_started_at = time.perf_counter()
            try:
                tick_row = fetch_binance_spot_reference_tick(
                    session,
                    symbol=SYMBOL,
                    source_name=SOURCE_NAME,
                    base_url=BASE_URL,
                    timeout_sec=TIMEOUT_SEC,
                )
                inserted = insert_reference_tick(conn, tick_row)
                tick_count += inserted
                clear_network_alert_state(NETWORK_ALERT_STATE_KEY)
                maybe_insert_completed_candle(conn, aggregator, tick_row)
                update_run_metrics(conn, run_id, reference_tick_count=tick_count, error_count=error_count)
                log(
                    "TICK | ts=%s | price=%.2f | bid=%s | ask=%s | latency=%sms"
                    % (
                        tick_row["ts_utc"],
                        float(tick_row["btc_price"]),
                        f"{float(tick_row['btc_bid']):.2f}" if tick_row.get("btc_bid") is not None else "-",
                        f"{float(tick_row['btc_ask']):.2f}" if tick_row.get("btc_ask") is not None else "-",
                        tick_row.get("latency_ms"),
                    )
                )

                if args.once or (args.max_ticks > 0 and tick_count >= args.max_ticks):
                    exit_status = "COMPLETED"
                    break
            except KeyboardInterrupt:
                exit_status = "STOPPED"
                raise SystemExit(0)
            except ReferenceFeedError as exc:
                error_count += 1
                update_run_metrics(conn, run_id, reference_tick_count=tick_count, error_count=error_count)
                log(f"WARN reference_fetch_failed | reason={exc}")
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
                            bot_label="BTC5M-REF",
                            msg=build_network_intervention_message(
                                "Reference collector",
                                state["reason"],
                                source=str(state["source"] or SOURCE_NAME),
                                failure_count=int(state["count"]),
                                duration_sec=int(state["duration_sec"]),
                                extra=f"symbol={SYMBOL}",
                            ),
                            level="WARN",
                            dedupe_seconds=ALERT_DEDUPE_SEC,
                        )
                if args.once:
                    raise SystemExit(1)
            except Exception as exc:
                error_count += 1
                update_run_metrics(conn, run_id, reference_tick_count=tick_count, error_count=error_count)
                log(f"Runtime Error: {exc}")
                if args.once:
                    raise

            sleep_sec = max(0.0, INTERVAL_SEC - (time.perf_counter() - loop_started_at))
            time.sleep(sleep_sec)
    finally:
        finish_collector_run(
            conn,
            run_id,
            status=exit_status,
            reference_tick_count=tick_count,
            error_count=error_count,
            meta_json={
                "symbol": SYMBOL,
                "source_name": SOURCE_NAME,
                "base_url": BASE_URL,
                "log_path": str(LOG_PATH),
                "db_path": str(resolve_db_path()),
            },
        )
        conn.close()


if __name__ == "__main__":
    main()
