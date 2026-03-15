"""Minimum execution-aware backtest engine for BTC5M markets."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Protocol


@dataclass(frozen=True)
class EntrySignal:
    side: str
    size: float
    take_profit_c: float
    stop_loss_c: float
    time_stop_sec: int
    reason: str


@dataclass(frozen=True)
class BacktestConfig:
    order_size: float
    fee_rate: float
    cooldown_sec: int
    max_trades_per_market: int


class Strategy(Protocol):
    name: str

    def generate_signal(self, candidate_row: dict[str, Any]) -> Optional[EntrySignal]:
        ...


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def side_key(side: str) -> str:
    normalized = str(side or "").strip().upper()
    if normalized not in {"YES", "NO"}:
        raise ValueError(f"Unsupported side: {side}")
    return normalized.lower()


def approximate_depth_qty(depth_notional: Any, price: Any) -> Optional[float]:
    depth = safe_float(depth_notional)
    px = safe_float(price)
    if depth is None or px in (None, 0):
        return None
    return depth / px


def apply_fee(notional: float, fee_rate: float) -> float:
    return float(notional) * max(0.0, float(fee_rate))


def simulate_fill(
    *,
    remaining_qty: float,
    snapshot_row: dict[str, Any],
    side: str,
    action: str,
    tick_size: float,
) -> dict[str, Any]:
    normalized_side = side_key(side)
    if action == "entry":
        price_key = f"best_ask_{normalized_side}"
        size_key = f"best_ask_size_{normalized_side}"
        depth_key = f"{normalized_side}_ask_depth_5"
        direction = 1.0
    elif action == "exit":
        price_key = f"best_bid_{normalized_side}"
        size_key = f"best_bid_size_{normalized_side}"
        depth_key = f"{normalized_side}_bid_depth_5"
        direction = -1.0
    else:
        raise ValueError(f"Unsupported action: {action}")

    top_price = safe_float(snapshot_row.get(price_key))
    top_size = max(0.0, safe_float(snapshot_row.get(size_key)) or 0.0)
    total_depth_qty = approximate_depth_qty(snapshot_row.get(depth_key), top_price)
    if total_depth_qty is None:
        total_depth_qty = top_size
    total_depth_qty = max(top_size, total_depth_qty)
    fill_qty = min(max(0.0, remaining_qty), total_depth_qty)
    if top_price is None or fill_qty <= 0:
        return {"filled_qty": 0.0, "avg_price": None, "partial": False, "used_depth": False}

    used_depth = fill_qty > top_size and total_depth_qty > top_size
    if not used_depth:
        avg_price = top_price
    else:
        extra_ratio = (fill_qty - top_size) / max(total_depth_qty - top_size, 1e-9)
        slippage_ticks = min(4.0, max(0.0, extra_ratio * 4.0))
        avg_price = top_price + (direction * -1.0 * tick_size * slippage_ticks)
        avg_price = max(0.0, min(1.0, avg_price))
    return {
        "filled_qty": fill_qty,
        "avg_price": avg_price,
        "partial": fill_qty < remaining_qty,
        "used_depth": used_depth,
    }


def select_trigger_reason(
    *,
    best_bid: Optional[float],
    entry_price: float,
    elapsed_sec: int,
    signal: EntrySignal,
) -> Optional[str]:
    if best_bid is None:
        return None
    pnl_per_share = float(best_bid) - float(entry_price)
    if signal.take_profit_c > 0 and pnl_per_share >= signal.take_profit_c:
        return "take_profit"
    if signal.stop_loss_c > 0 and pnl_per_share <= -signal.stop_loss_c:
        return "stop_loss"
    if signal.time_stop_sec > 0 and elapsed_sec >= signal.time_stop_sec:
        return "time_stop"
    return None


def run_backtest(
    *,
    candidate_rows: list[dict[str, Any]],
    market_contexts: dict[str, dict[str, Any]],
    strategy: Strategy,
    config: BacktestConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    blocked_until: dict[str, int] = {}
    trade_count_by_market: dict[str, int] = {}

    for candidate in sorted(candidate_rows, key=lambda row: (int(row["decision_ts"]), str(row["market_id"]))):
        market_id = str(candidate["market_id"])
        if trade_count_by_market.get(market_id, 0) >= config.max_trades_per_market:
            continue
        if int(candidate["decision_ts"]) <= blocked_until.get(market_id, -1):
            continue

        signal = strategy.generate_signal(candidate)
        if signal is None:
            continue

        market_context = market_contexts.get(market_id)
        if not market_context:
            continue

        trade = simulate_trade(candidate, signal, market_context, config, strategy.name)
        if trade is None:
            continue
        trades.append(trade)
        trade_count_by_market[market_id] = trade_count_by_market.get(market_id, 0) + 1
        blocked_until[market_id] = int(trade["exit_ts"]) + config.cooldown_sec

    return trades, summarize_trades(trades, strategy.name)


def simulate_trade(
    candidate_row: dict[str, Any],
    signal: EntrySignal,
    market_context: dict[str, Any],
    config: BacktestConfig,
    strategy_name: str,
) -> Optional[dict[str, Any]]:
    side = side_key(signal.side)
    snapshots = market_context["snapshots"]
    snapshot_map = market_context["snapshot_map"]
    market = market_context["market"]
    decision_ts = int(candidate_row["decision_ts"])
    decision_snapshot = snapshot_map.get(decision_ts)
    if not decision_snapshot:
        return None

    tick_size = safe_float(market.get("tick_size")) or 0.01
    entry_fill = simulate_fill(
        remaining_qty=float(signal.size),
        snapshot_row=decision_snapshot,
        side=side,
        action="entry",
        tick_size=tick_size,
    )
    entry_qty = float(entry_fill["filled_qty"])
    entry_price = safe_float(entry_fill["avg_price"])
    if entry_qty <= 0 or entry_price is None:
        return None

    entry_notional = entry_qty * entry_price
    entry_fee = apply_fee(entry_notional, config.fee_rate)
    remaining_qty = entry_qty
    exit_notional = 0.0
    exit_fees = 0.0
    exit_qty = 0.0
    partial_exit_count = 0
    no_orderbook_seen = False
    force_exit_used = False
    settlement_exit_used = False
    exit_reason: Optional[str] = None
    exit_ts = decision_ts

    slot_end_ts = int(market["slot_end_ts"])
    best_bid_key = f"best_bid_{side}"
    orderbook_key = f"orderbook_exists_{side}"
    future_rows = [row for row in snapshots if int(row["collected_ts"]) >= decision_ts and int(row["collected_ts"]) <= slot_end_ts]

    for snapshot in future_rows:
        elapsed_sec = int(snapshot["collected_ts"]) - decision_ts
        best_bid = safe_float(snapshot.get(best_bid_key))
        has_book = int(snapshot.get(orderbook_key) or 0) == 1 and best_bid is not None
        if not has_book:
            no_orderbook_seen = True
            continue

        trigger_reason = select_trigger_reason(best_bid=best_bid, entry_price=entry_price, elapsed_sec=elapsed_sec, signal=signal)
        if trigger_reason is None:
            continue

        fill = simulate_fill(
            remaining_qty=remaining_qty,
            snapshot_row=snapshot,
            side=side,
            action="exit",
            tick_size=tick_size,
        )
        filled_qty = float(fill["filled_qty"])
        fill_price = safe_float(fill["avg_price"])
        if filled_qty <= 0 or fill_price is None:
            continue
        partial_exit_count += int(fill["partial"])
        proceeds = filled_qty * fill_price
        exit_notional += proceeds
        exit_fees += apply_fee(proceeds, config.fee_rate)
        exit_qty += filled_qty
        remaining_qty -= filled_qty
        exit_reason = trigger_reason
        exit_ts = int(snapshot["collected_ts"])
        if remaining_qty <= 0:
            break

    if remaining_qty > 0:
        for snapshot in reversed(future_rows):
            best_bid = safe_float(snapshot.get(best_bid_key))
            has_book = int(snapshot.get(orderbook_key) or 0) == 1 and best_bid is not None
            if not has_book:
                no_orderbook_seen = True
                continue
            fill = simulate_fill(
                remaining_qty=remaining_qty,
                snapshot_row=snapshot,
                side=side,
                action="exit",
                tick_size=tick_size,
            )
            filled_qty = float(fill["filled_qty"])
            fill_price = safe_float(fill["avg_price"])
            if filled_qty <= 0 or fill_price is None:
                continue
            partial_exit_count += int(fill["partial"])
            proceeds = filled_qty * fill_price
            exit_notional += proceeds
            exit_fees += apply_fee(proceeds, config.fee_rate)
            exit_qty += filled_qty
            remaining_qty -= filled_qty
            exit_reason = exit_reason or "force_exit_preexpiry"
            exit_ts = int(snapshot["collected_ts"])
            force_exit_used = True
            if remaining_qty <= 0:
                break

    if remaining_qty > 0:
        resolved_price_key = f"resolved_{side}_price"
        settlement_price = safe_float(market.get(resolved_price_key))
        if settlement_price is None:
            return None
        settlement_ts = int(market_context.get("resolved_event_ts") or market.get("resolved_ts") or slot_end_ts)
        exit_notional += remaining_qty * settlement_price
        exit_qty += remaining_qty
        remaining_qty = 0.0
        exit_reason = exit_reason or "settlement_exit"
        exit_ts = settlement_ts
        settlement_exit_used = True

    if exit_qty <= 0:
        return None

    avg_exit_price = exit_notional / exit_qty
    gross_pnl = exit_notional - entry_notional
    total_fees = entry_fee + exit_fees
    net_pnl = gross_pnl - total_fees

    return {
        "strategy": strategy_name,
        "market_id": market["market_id"],
        "market_slug": market["market_slug"],
        "split_bucket": candidate_row.get("split_bucket"),
        "decision_ts": decision_ts,
        "entry_ts": decision_ts,
        "exit_ts": exit_ts,
        "side": side.upper(),
        "entry_reason": signal.reason,
        "exit_reason": exit_reason,
        "requested_size": float(signal.size),
        "filled_entry_size": entry_qty,
        "entry_partial_fill": int(entry_fill["partial"]),
        "filled_exit_size": exit_qty,
        "exit_partial_fill_count": partial_exit_count,
        "entry_price": entry_price,
        "exit_price": avg_exit_price,
        "entry_notional": entry_notional,
        "exit_notional": exit_notional,
        "entry_fee": entry_fee,
        "exit_fee": exit_fees,
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "hold_sec": max(0, exit_ts - decision_ts),
        "force_exit_used": int(force_exit_used),
        "settlement_exit_used": int(settlement_exit_used),
        "no_orderbook_seen": int(no_orderbook_seen),
        "is_trainable": int(candidate_row.get("is_trainable") or 0),
    }


def summarize_trades(trades: list[dict[str, Any]], strategy_name: str) -> dict[str, Any]:
    total_trades = len(trades)
    gross_pnl = sum(float(trade["gross_pnl"]) for trade in trades)
    net_pnl = sum(float(trade["net_pnl"]) for trade in trades)
    fees_total = sum(float(trade["entry_fee"]) + float(trade["exit_fee"]) for trade in trades)
    wins = sum(1 for trade in trades if float(trade["net_pnl"]) > 0)
    avg_hold_sec = sum(float(trade["hold_sec"]) for trade in trades) / total_trades if total_trades else 0.0
    avg_net_pnl = net_pnl / total_trades if total_trades else 0.0
    partial_entry_count = sum(int(trade["entry_partial_fill"]) for trade in trades)
    partial_exit_trade_count = sum(1 for trade in trades if int(trade["exit_partial_fill_count"]) > 0)
    settlement_exit_count = sum(int(trade["settlement_exit_used"]) for trade in trades)
    force_exit_count = sum(int(trade["force_exit_used"]) for trade in trades)
    no_orderbook_trade_count = sum(int(trade["no_orderbook_seen"]) for trade in trades)
    return {
        "strategy": strategy_name,
        "total_trades": total_trades,
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "fees_total": fees_total,
        "win_rate": (wins / total_trades) if total_trades else 0.0,
        "avg_net_pnl": avg_net_pnl,
        "avg_hold_sec": avg_hold_sec,
        "partial_entry_count": partial_entry_count,
        "partial_exit_trade_count": partial_exit_trade_count,
        "settlement_exit_count": settlement_exit_count,
        "force_exit_count": force_exit_count,
        "no_orderbook_trade_count": no_orderbook_trade_count,
    }
