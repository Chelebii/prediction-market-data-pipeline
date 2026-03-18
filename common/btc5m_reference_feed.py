"""BTC reference feed helpers for the BTC5M dataset."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import requests

from common.network_diagnostics import classify_requests_exception

BINANCE_SPOT_BASE_URL = "https://api.binance.com"
BINANCE_PRICE_ENDPOINT = "/api/v3/ticker/price"
BINANCE_BOOK_ENDPOINT = "/api/v3/ticker/bookTicker"
DEFAULT_SOURCE_NAME = "binance_spot_rest"
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEOUT_SEC = 3
DEFAULT_USER_AGENT = "mavi-x-btc5m-reference-collector/1.0"


class ReferenceFeedError(RuntimeError):
    """Raised when the reference feed fetch fails."""


def _elapsed_ms(started_at: float) -> int:
    return max(0, int((time.perf_counter() - started_at) * 1000))


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def normalize_symbol(symbol: str) -> str:
    return str(symbol or DEFAULT_SYMBOL).strip().upper()


def build_reference_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def fetch_binance_spot_reference_tick(
    session: requests.Session,
    *,
    symbol: str = DEFAULT_SYMBOL,
    source_name: str = DEFAULT_SOURCE_NAME,
    base_url: str = BINANCE_SPOT_BASE_URL,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    symbol = normalize_symbol(symbol)

    price_started_at = time.perf_counter()
    try:
        price_response = session.get(
            f"{base_url}{BINANCE_PRICE_ENDPOINT}",
            params={"symbol": symbol},
            timeout=timeout_sec,
        )
    except requests.RequestException as exc:
        raise ReferenceFeedError(classify_requests_exception(exc)) from exc
    price_latency_ms = _elapsed_ms(price_started_at)
    if price_response.status_code != 200:
        raise ReferenceFeedError(f"price_http_{price_response.status_code}")

    book_started_at = time.perf_counter()
    try:
        book_response = session.get(
            f"{base_url}{BINANCE_BOOK_ENDPOINT}",
            params={"symbol": symbol},
            timeout=timeout_sec,
        )
    except requests.RequestException as exc:
        raise ReferenceFeedError(classify_requests_exception(exc)) from exc
    book_latency_ms = _elapsed_ms(book_started_at)
    if book_response.status_code != 200:
        raise ReferenceFeedError(f"book_http_{book_response.status_code}")

    try:
        price_payload = price_response.json()
        book_payload = book_response.json()
    except Exception as exc:
        raise ReferenceFeedError(f"json_decode_failed:{exc}") from exc

    btc_price = _safe_float(price_payload.get("price"))
    if btc_price is None:
        raise ReferenceFeedError("price_missing")

    btc_bid = _safe_float(book_payload.get("bidPrice"))
    btc_ask = _safe_float(book_payload.get("askPrice"))
    collected_ts_ms = int(time.time() * 1000)

    return {
        "ts_utc": collected_ts_ms // 1000,
        "source_name": str(source_name or DEFAULT_SOURCE_NAME),
        "symbol": symbol,
        "btc_price": btc_price,
        "btc_bid": btc_bid,
        "btc_ask": btc_ask,
        "btc_mark_price": None,
        "btc_index_price": None,
        "volume_1s": None,
        "latency_ms": _elapsed_ms(started_at),
        "meta_json": {
            "base_url": base_url,
            "collected_ts_ms": collected_ts_ms,
            "price_endpoint": BINANCE_PRICE_ENDPOINT,
            "book_endpoint": BINANCE_BOOK_ENDPOINT,
            "price_request_latency_ms": price_latency_ms,
            "book_request_latency_ms": book_latency_ms,
            "source_symbol": str(price_payload.get("symbol") or book_payload.get("symbol") or symbol),
            "source_ts_missing": True,
        },
    }


def candle_ts_for_tick(ts_utc: int) -> int:
    return (int(ts_utc) // 60) * 60


@dataclass
class _CandleState:
    candle_ts: int
    source_name: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    trade_count: int
    first_tick_ts: int
    last_tick_ts: int


class ReferenceOhlcvAggregator:
    """Aggregates 1-second reference ticks into 1-minute candles."""

    def __init__(self, *, source_name: str, symbol: str):
        self.source_name = str(source_name or DEFAULT_SOURCE_NAME)
        self.symbol = normalize_symbol(symbol)
        self._current: Optional[_CandleState] = None

    def update(self, tick_row: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        ts_utc = int(tick_row["ts_utc"])
        candle_ts = candle_ts_for_tick(ts_utc)
        price = float(tick_row["btc_price"])
        volume_1s = tick_row.get("volume_1s")

        if self._current is None:
            self._current = self._new_state(candle_ts, price, ts_utc, volume_1s)
            return None

        if candle_ts == self._current.candle_ts:
            self._current.high = max(self._current.high, price)
            self._current.low = min(self._current.low, price)
            self._current.close = price
            self._current.trade_count += 1
            self._current.last_tick_ts = ts_utc
            if volume_1s is not None:
                self._current.volume = (self._current.volume or 0.0) + float(volume_1s)
            return None

        completed = self._to_row(self._current)
        self._current = self._new_state(candle_ts, price, ts_utc, volume_1s)
        return completed

    def flush(self) -> Optional[dict[str, Any]]:
        if self._current is None:
            return None
        completed = self._to_row(self._current, partial=True)
        self._current = None
        return completed

    def _new_state(self, candle_ts: int, price: float, ts_utc: int, volume_1s: Any) -> _CandleState:
        return _CandleState(
            candle_ts=candle_ts,
            source_name=self.source_name,
            symbol=self.symbol,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=float(volume_1s) if volume_1s is not None else None,
            trade_count=1,
            first_tick_ts=ts_utc,
            last_tick_ts=ts_utc,
        )

    def _to_row(self, state: _CandleState, *, partial: bool = False) -> dict[str, Any]:
        return {
            "candle_ts": state.candle_ts,
            "source_name": state.source_name,
            "symbol": state.symbol,
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "volume": state.volume,
            "trade_count": state.trade_count,
            "meta_json": {
                "derived_from_ticks": True,
                "partial": partial,
                "first_tick_ts": state.first_tick_ts,
                "last_tick_ts": state.last_tick_ts,
            },
        }
