"""Run a minimum execution-aware backtest on BTC5M decision data."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_backtest_engine import BacktestConfig, EntrySignal, Strategy, run_backtest
from common.btc5m_dataset_db import connect_db, resolve_db_path
from common.single_instance import acquire_single_instance_lock

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

DEFAULT_DATASET_VERSION = str(os.getenv("BTC5M_DATASET_VERSION", "")).strip() or "v1__v1"
DEFAULT_FEATURE_VERSION = str(os.getenv("BTC5M_FEATURE_VERSION", "v1")).strip() or "v1"
LOOKBACK_HOURS = max(1, int(os.getenv("BTC5M_DATASET_LOOKBACK_HOURS", "168")))
LOG_PATH = Path(os.getenv("BTC5M_BACKTEST_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_run_backtest.log"))
LOCK_PATH = Path(os.getenv("BTC5M_BACKTEST_LOCK_PATH", ROOT_DIR / "runtime" / "locks" / "btc5m_run_backtest.lock"))
OUTPUT_DIR = Path(os.getenv("BTC5M_BACKTEST_OUTPUT_DIR", ROOT_DIR / "runtime" / "backtests"))

_logger = logging.getLogger("btc5m_run_backtest")
_logger.setLevel(logging.INFO)
_logger.handlers.clear()
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-BACKTEST | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-BACKTEST | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)


def log(message: str) -> None:
    _logger.info(message)


@dataclass
class AlwaysSideStrategy:
    name: str
    side: str
    order_size: float
    take_profit_c: float
    stop_loss_c: float
    time_stop_sec: int
    require_trainable: bool

    def generate_signal(self, candidate_row: dict[str, Any]) -> Optional[EntrySignal]:
        if self.require_trainable and int(candidate_row.get("is_trainable") or 0) != 1:
            return None
        return EntrySignal(
            side=self.side,
            size=self.order_size,
            take_profit_c=self.take_profit_c,
            stop_loss_c=self.stop_loss_c,
            time_stop_sec=self.time_stop_sec,
            reason=f"{self.name}_baseline",
        )


@dataclass
class MomentumThresholdStrategy:
    name: str
    order_size: float
    take_profit_c: float
    stop_loss_c: float
    time_stop_sec: int
    require_trainable: bool
    min_return_60s: float
    min_depth_ratio: float
    min_order_imbalance: float
    max_spread_sum: float
    max_complement_gap: float
    min_quote_stability: float

    def generate_signal(self, candidate_row: dict[str, Any]) -> Optional[EntrySignal]:
        if self.require_trainable and int(candidate_row.get("is_trainable") or 0) != 1:
            return None

        spread_sum = safe_float(candidate_row.get("spread_sum"))
        complement_gap = safe_float(candidate_row.get("complement_gap"))
        quote_stability = safe_float(candidate_row.get("quote_stability_score"))
        return_60s = safe_float(candidate_row.get("return_60s"))
        if spread_sum is None or spread_sum > self.max_spread_sum:
            return None
        if complement_gap is None or complement_gap > self.max_complement_gap:
            return None
        if quote_stability is None or quote_stability < self.min_quote_stability:
            return None
        if return_60s is None:
            return None

        if (
            return_60s >= self.min_return_60s
            and (safe_float(candidate_row.get("depth_ratio_yes")) or 0.0) >= self.min_depth_ratio
            and (safe_float(candidate_row.get("order_imbalance_yes")) or 0.0) >= self.min_order_imbalance
        ):
            return EntrySignal(
                side="YES",
                size=self.order_size,
                take_profit_c=self.take_profit_c,
                stop_loss_c=self.stop_loss_c,
                time_stop_sec=self.time_stop_sec,
                reason="momentum_yes",
            )
        if (
            return_60s <= -self.min_return_60s
            and (safe_float(candidate_row.get("depth_ratio_no")) or 0.0) >= self.min_depth_ratio
            and (safe_float(candidate_row.get("order_imbalance_no")) or 0.0) >= self.min_order_imbalance
        ):
            return EntrySignal(
                side="NO",
                size=self.order_size,
                take_profit_c=self.take_profit_c,
                stop_loss_c=self.stop_loss_c,
                time_stop_sec=self.time_stop_sec,
                reason="momentum_no",
            )
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a minimum execution-aware BTC5M backtest.")
    parser.add_argument("--dataset-version", type=str, default=DEFAULT_DATASET_VERSION, help="Decision dataset version.")
    parser.add_argument("--feature-version", type=str, default=DEFAULT_FEATURE_VERSION, help="Feature version for strategy joins.")
    parser.add_argument("--market-slug", type=str, default="", help="Only backtest one market slug.")
    parser.add_argument("--split-bucket", type=str, default="", help="Restrict to one split bucket.")
    parser.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS, help="How far back to load candidates.")
    parser.add_argument("--max-markets", type=int, default=250, help="Maximum number of markets to backtest.")
    parser.add_argument("--strategy", type=str, default="momentum", choices=["momentum", "always_yes", "always_no"], help="Strategy preset.")
    parser.add_argument("--require-trainable", action="store_true", help="Only allow trainable decision rows.")
    parser.add_argument("--order-size", type=float, default=100.0, help="Requested order size per trade.")
    parser.add_argument("--fee-rate", type=float, default=0.02, help="Linear fee rate applied to executed notional.")
    parser.add_argument("--cooldown-sec", type=int, default=0, help="Cooldown after a trade exits.")
    parser.add_argument("--max-trades-per-market", type=int, default=1, help="Maximum entries per market.")
    parser.add_argument("--take-profit-c", type=float, default=0.05, help="Take-profit threshold in price points.")
    parser.add_argument("--stop-loss-c", type=float, default=0.05, help="Stop-loss threshold in price points.")
    parser.add_argument("--time-stop-sec", type=int, default=120, help="Time-stop threshold in seconds.")
    parser.add_argument("--min-return-60s", type=float, default=0.001, help="Momentum threshold for the built-in momentum strategy.")
    parser.add_argument("--min-depth-ratio", type=float, default=0.0, help="Minimum depth ratio for the built-in momentum strategy.")
    parser.add_argument("--min-order-imbalance", type=float, default=0.0, help="Minimum top-of-book imbalance for the built-in momentum strategy.")
    parser.add_argument("--max-spread-sum", type=float, default=0.08, help="Maximum spread sum for entry.")
    parser.add_argument("--max-complement-gap", type=float, default=0.05, help="Maximum complement gap for entry.")
    parser.add_argument("--min-quote-stability", type=float, default=1.0, help="Minimum quote stability score for entry.")
    return parser.parse_args()


def load_candidate_rows(
    conn: sqlite3.Connection,
    *,
    now_ts: int,
    dataset_version: str,
    feature_version: str,
    market_slug: str,
    split_bucket: str,
    lookback_hours: int,
    max_markets: int,
) -> list[dict[str, Any]]:
    lower_bound = now_ts - (max(1, lookback_hours) * 3600)
    clauses = [
        "m.slot_end_ts >= ?",
        "d.dataset_version = ?",
        "f.feature_version = ?",
    ]
    params: list[Any] = [lower_bound, dataset_version, feature_version]
    if market_slug:
        clauses.append("d.market_slug = ?")
        params.append(market_slug)
    if split_bucket:
        clauses.append("d.split_bucket = ?")
        params.append(split_bucket)
    sql = f"""
        SELECT
            d.market_id,
            d.decision_ts,
            d.seconds_to_resolution,
            d.market_slug,
            d.mid_yes,
            d.mid_no,
            d.spread_yes,
            d.spread_no,
            d.btc_price,
            d.quote_stability_score,
            d.terminal_outcome,
            d.target_yes_hold,
            d.target_no_hold,
            d.label_quality_flag,
            d.is_trainable,
            d.split_bucket,
            f.return_15s,
            f.return_30s,
            f.return_60s,
            f.volatility_30s,
            f.volatility_60s,
            f.complement_gap,
            f.order_imbalance_yes,
            f.order_imbalance_no,
            f.depth_ratio_yes,
            f.depth_ratio_no
        FROM btc5m_decision_dataset d
        JOIN btc5m_features f
          ON f.market_id = d.market_id
         AND f.ts_utc = d.decision_ts
        JOIN btc5m_markets m
          ON m.market_id = d.market_id
        WHERE {" AND ".join(clauses)}
        ORDER BY d.decision_ts ASC
    """
    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    if not rows:
        return []
    seen: list[str] = []
    limited_rows: list[dict[str, Any]] = []
    for row in rows:
        market_id = str(row["market_id"])
        if market_id not in seen:
            if len(seen) >= max(1, max_markets):
                break
            seen.append(market_id)
        if market_id in seen:
            limited_rows.append(row)
    return limited_rows


def load_market_contexts(conn: sqlite3.Connection, market_ids: list[str]) -> dict[str, dict[str, Any]]:
    contexts: dict[str, dict[str, Any]] = {}
    for market_id in market_ids:
        market_row = conn.execute(
            "SELECT market_id, market_slug, slot_end_ts, tick_size, resolved_yes_price, resolved_no_price, resolved_ts "
            "FROM btc5m_markets WHERE market_id=?",
            (market_id,),
        ).fetchone()
        if not market_row:
            continue
        snapshot_rows = conn.execute(
            """
            SELECT
                s.collected_ts,
                s.best_bid_yes,
                s.best_ask_yes,
                s.best_bid_no,
                s.best_ask_no,
                s.best_bid_size_yes,
                s.best_ask_size_yes,
                s.best_bid_size_no,
                s.best_ask_size_no,
                s.orderbook_exists_yes,
                s.orderbook_exists_no,
                d.yes_bid_depth_5,
                d.yes_ask_depth_5,
                d.no_bid_depth_5,
                d.no_ask_depth_5
            FROM btc5m_snapshots s
            LEFT JOIN btc5m_orderbook_depth d
              ON d.market_id = s.market_id
             AND d.collected_ts = s.collected_ts
            WHERE s.market_id=?
            ORDER BY s.collected_ts ASC
            """,
            (market_id,),
        ).fetchall()
        snapshots = [dict(row) for row in snapshot_rows]
        snapshot_map = {int(row["collected_ts"]): row for row in snapshots}
        lifecycle_rows = conn.execute(
            "SELECT event_type, event_ts FROM btc5m_lifecycle_events WHERE market_id=? ORDER BY event_ts ASC",
            (market_id,),
        ).fetchall()
        resolved_event_ts = None
        for row in lifecycle_rows:
            if str(row["event_type"]) == "RESOLVED":
                resolved_event_ts = int(row["event_ts"])
                break
        contexts[market_id] = {
            "market": dict(market_row),
            "snapshots": snapshots,
            "snapshot_map": snapshot_map,
            "resolved_event_ts": resolved_event_ts,
        }
    return contexts


def build_strategy(args: argparse.Namespace) -> Strategy:
    if args.strategy == "always_yes":
        return AlwaysSideStrategy(
            name="always_yes",
            side="YES",
            order_size=args.order_size,
            take_profit_c=args.take_profit_c,
            stop_loss_c=args.stop_loss_c,
            time_stop_sec=args.time_stop_sec,
            require_trainable=bool(args.require_trainable),
        )
    if args.strategy == "always_no":
        return AlwaysSideStrategy(
            name="always_no",
            side="NO",
            order_size=args.order_size,
            take_profit_c=args.take_profit_c,
            stop_loss_c=args.stop_loss_c,
            time_stop_sec=args.time_stop_sec,
            require_trainable=bool(args.require_trainable),
        )
    return MomentumThresholdStrategy(
        name="momentum",
        order_size=args.order_size,
        take_profit_c=args.take_profit_c,
        stop_loss_c=args.stop_loss_c,
        time_stop_sec=args.time_stop_sec,
        require_trainable=bool(args.require_trainable),
        min_return_60s=args.min_return_60s,
        min_depth_ratio=args.min_depth_ratio,
        min_order_imbalance=args.min_order_imbalance,
        max_spread_sum=args.max_spread_sum,
        max_complement_gap=args.max_complement_gap,
        min_quote_stability=args.min_quote_stability,
    )


def write_outputs(trades: list[dict[str, Any]], metrics: dict[str, Any], strategy_name: str, dataset_version: str) -> tuple[Path, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"{stamp}_{strategy_name}_{dataset_version}_trades.csv"
    json_path = OUTPUT_DIR / f"{stamp}_{strategy_name}_{dataset_version}_summary.json"

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        if trades:
            writer = csv.DictWriter(handle, fieldnames=list(trades[0].keys()))
            writer.writeheader()
            writer.writerows(trades)
        else:
            handle.write("")

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, ensure_ascii=True, indent=2, sort_keys=True)

    return csv_path, json_path


def main() -> None:
    args = parse_args()
    acquire_single_instance_lock(str(LOCK_PATH), process_name="btc5m-run-backtest", on_log=log, takeover=True)
    conn = connect_db()
    try:
        strategy = build_strategy(args)
        config = BacktestConfig(
            order_size=args.order_size,
            fee_rate=args.fee_rate,
            cooldown_sec=args.cooldown_sec,
            max_trades_per_market=args.max_trades_per_market,
        )
        now_ts = int(time.time())
        candidate_rows = load_candidate_rows(
            conn,
            now_ts=now_ts,
            dataset_version=str(args.dataset_version),
            feature_version=str(args.feature_version),
            market_slug=str(args.market_slug or "").strip(),
            split_bucket=str(args.split_bucket or "").strip(),
            lookback_hours=args.lookback_hours,
            max_markets=args.max_markets,
        )
        market_ids = sorted({str(row["market_id"]) for row in candidate_rows})
        market_contexts = load_market_contexts(conn, market_ids)
        log(
            "Backtest started | strategy=%s | candidates=%s | markets=%s | dataset_version=%s | db=%s"
            % (strategy.name, len(candidate_rows), len(market_ids), args.dataset_version, resolve_db_path())
        )
        trades, metrics = run_backtest(
            candidate_rows=candidate_rows,
            market_contexts=market_contexts,
            strategy=strategy,
            config=config,
        )
        metrics.update(
            {
                "dataset_version": str(args.dataset_version),
                "feature_version": str(args.feature_version),
                "split_bucket": str(args.split_bucket or "all"),
                "market_count": len(market_ids),
                "candidate_count": len(candidate_rows),
            }
        )
        csv_path, json_path = write_outputs(trades, metrics, strategy.name, str(args.dataset_version))
        log(
            "SUMMARY | strategy=%s | trades=%s | net_pnl=%.6f | win_rate=%.3f | csv=%s | summary=%s"
            % (
                strategy.name,
                metrics["total_trades"],
                float(metrics["net_pnl"]),
                float(metrics["win_rate"]),
                csv_path,
                json_path,
            )
        )
        print(json.dumps({"trade_log_csv": str(csv_path), "summary_json": str(json_path), "metrics": metrics}, ensure_ascii=True, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
