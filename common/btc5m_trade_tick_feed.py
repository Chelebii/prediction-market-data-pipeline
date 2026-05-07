"""Polymarket Data API trade-tick helpers for the BTC5M dataset."""

from __future__ import annotations

import time
from typing import Any, Iterator, Mapping, Optional

import requests

DATA_API_BASE_URL = "https://data-api.polymarket.com"
TRADES_ENDPOINT = "/trades"
DEFAULT_SOURCE_NAME = "polymarket_data_api_trades"
DEFAULT_TIMEOUT_SEC = 10
DEFAULT_USER_AGENT = "mavi-x-btc5m-trade-tick-collector/1.0"
PAGE_LIMIT = 1000
MAX_OFFSET = 3000
VALID_SIDES = ("BUY", "SELL")


class TradeTickFeedError(RuntimeError):
    """Raised when the Polymarket trades feed fetch fails."""


def build_trade_tick_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def fetch_trades_page(
    session: requests.Session,
    *,
    market_id: str,
    side: str,
    limit: int = PAGE_LIMIT,
    offset: int = 0,
    taker_only: bool = True,
    base_url: str = DATA_API_BASE_URL,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    retry_count: int = 2,
    retry_backoff_sec: float = 1.0,
) -> list[dict[str, Any]]:
    market = str(market_id or "").strip()
    if not market:
        raise TradeTickFeedError("market_id_missing")
    side_upper = str(side or "").strip().upper()
    if side_upper not in VALID_SIDES:
        raise TradeTickFeedError(f"invalid_side:{side}")

    params = {
        "market": market,
        "side": side_upper,
        "limit": int(limit),
        "offset": int(offset),
        "takerOnly": "true" if taker_only else "false",
    }

    last_error: Optional[str] = None
    attempts = max(1, int(retry_count) + 1)
    for attempt_idx in range(attempts):
        try:
            response = session.get(
                f"{base_url.rstrip('/')}{TRADES_ENDPOINT}",
                params=params,
                timeout=timeout_sec,
            )
        except requests.RequestException as exc:
            last_error = f"trades_request_failed:{exc.__class__.__name__}"
            if attempt_idx + 1 >= attempts:
                raise TradeTickFeedError(last_error) from exc
            time.sleep(max(0.0, float(retry_backoff_sec)))
            continue

        if response.status_code == 429:
            last_error = "trades_http_429"
            if attempt_idx + 1 < attempts:
                time.sleep(max(0.0, float(retry_backoff_sec)))
                continue
            raise TradeTickFeedError(last_error)

        if response.status_code != 200:
            body_text = (response.text or "").strip()
            if "max historical activity offset" in body_text.lower():
                raise TradeTickFeedError("trades_offset_cap_exceeded")
            last_error = f"trades_http_{response.status_code}"
            if response.status_code >= 500 and attempt_idx + 1 < attempts:
                time.sleep(max(0.0, float(retry_backoff_sec)))
                continue
            raise TradeTickFeedError(last_error)

        try:
            payload = response.json()
        except Exception as exc:
            raise TradeTickFeedError(f"trades_json_decode_failed:{exc}") from exc

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("trades") or []
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            return []

        raise TradeTickFeedError("trades_unexpected_payload")

    raise TradeTickFeedError(last_error or "trades_fetch_failed")


def iter_market_trades(
    session: requests.Session,
    *,
    market_id: str,
    side: str,
    taker_only: bool = True,
    limit: int = PAGE_LIMIT,
    max_offset: int = MAX_OFFSET,
    base_url: str = DATA_API_BASE_URL,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    retry_count: int = 2,
    retry_backoff_sec: float = 1.0,
    request_sleep_sec: float = 0.0,
) -> Iterator[dict[str, Any]]:
    """Yield raw API trade rows for a single market+side, paginating until empty page or offset cap.

    Raises TradeTickFeedError("trades_offset_cap_partial") if the feed truncates at the offset cap
    while still returning a full page (so the caller can record partial coverage).
    """
    offset = 0
    while True:
        page = fetch_trades_page(
            session,
            market_id=market_id,
            side=side,
            limit=limit,
            offset=offset,
            taker_only=taker_only,
            base_url=base_url,
            timeout_sec=timeout_sec,
            retry_count=retry_count,
            retry_backoff_sec=retry_backoff_sec,
        )
        if not page:
            return
        for row in page:
            yield row
        if len(page) < limit:
            return
        offset += limit
        if offset > max_offset:
            raise TradeTickFeedError("trades_offset_cap_partial")
        if request_sleep_sec > 0:
            time.sleep(float(request_sleep_sec))


def normalize_trade_row(
    api_row: Mapping[str, Any],
    *,
    market_id: str,
    market_slug: str,
    yes_token_id: str,
    no_token_id: str,
    source_name: str = DEFAULT_SOURCE_NAME,
    collected_ts: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Map an API trade dict to a btc5m_trade_ticks row. Returns None to reject."""
    asset_token_id = str(api_row.get("asset") or "").strip()
    if not asset_token_id:
        return None
    yes_id = str(yes_token_id or "").strip()
    no_id = str(no_token_id or "").strip()
    if asset_token_id == yes_id:
        outcome = "YES"
    elif asset_token_id == no_id:
        outcome = "NO"
    else:
        return None

    side = str(api_row.get("side") or "").strip().upper()
    if side not in VALID_SIDES:
        return None

    transaction_hash = str(api_row.get("transactionHash") or "").strip()
    if not transaction_hash:
        return None

    try:
        ts_utc = int(api_row.get("timestamp"))
    except (TypeError, ValueError):
        return None

    try:
        price = float(api_row.get("price"))
        size = float(api_row.get("size"))
    except (TypeError, ValueError):
        return None
    if price < 0 or size <= 0:
        return None

    proxy_wallet = api_row.get("proxyWallet")
    proxy_wallet_str = str(proxy_wallet).strip() if proxy_wallet else None

    api_condition_id = str(api_row.get("conditionId") or "").strip()
    api_outcome_label = api_row.get("outcome")
    api_outcome_index = api_row.get("outcomeIndex")

    meta = {
        "api_condition_id": api_condition_id or None,
        "api_outcome": str(api_outcome_label) if api_outcome_label is not None else None,
        "api_outcome_index": int(api_outcome_index) if isinstance(api_outcome_index, (int, float)) else None,
        "trader_name": str(api_row.get("name") or "") or None,
        "trader_pseudonym": str(api_row.get("pseudonym") or "") or None,
        "taker_only_query": True,
    }

    return {
        "market_id": str(market_id),
        "market_slug": str(market_slug),
        "ts_utc": ts_utc,
        "asset_token_id": asset_token_id,
        "outcome": outcome,
        "side": side,
        "price": price,
        "size": size,
        "notional": price * size,
        "transaction_hash": transaction_hash,
        "proxy_wallet": proxy_wallet_str,
        "source_name": source_name,
        "collected_ts": int(collected_ts if collected_ts is not None else time.time()),
        "meta_json": meta,
    }
