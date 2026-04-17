"""Official BTC5M market resolution helpers backed by Polymarket Gamma."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from urllib.parse import quote

import requests

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
MARKETS_ENDPOINT = "/markets"
MARKET_BY_SLUG_ENDPOINT = "/markets/slug/{slug}"
DEFAULT_SOURCE_NAME = "polymarket_gamma_official"
DEFAULT_TIMEOUT_SEC = 5
DEFAULT_USER_AGENT = "mavi-x-btc5m-resolution-collector/1.0"
RESOLVED_BINARY_MIN = 0.99
RESOLVED_BINARY_MAX = 0.01


class ResolutionFeedError(RuntimeError):
    """Raised when the official resolution feed fetch fails."""


@dataclass(frozen=True)
class ResolutionDecision:
    status: str
    updates: dict[str, Any]
    event_type: Optional[str]
    event_ts: Optional[int]
    event_reason: Optional[str]
    event_meta: dict[str, Any]
    outcome: Optional[str]
    quality_flag: str


def build_resolution_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def fetch_gamma_market_by_slug(
    session: requests.Session,
    *,
    market_slug: str,
    base_url: str = GAMMA_BASE_URL,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    retry_count: int = 1,
    retry_backoff_sec: float = 0.5,
) -> dict[str, Any]:
    slug = str(market_slug or "").strip()
    if not slug:
        raise ResolutionFeedError("market_slug_missing")

    slug_path = quote(slug, safe="")
    last_error: Optional[str] = None
    attempts = max(1, int(retry_count) + 1)
    for attempt_idx in range(attempts):
        started_at = time.perf_counter()
        try:
            response = session.get(
                f"{base_url.rstrip('/')}{MARKET_BY_SLUG_ENDPOINT.format(slug=slug_path)}",
                timeout=timeout_sec,
            )
        except requests.RequestException as exc:
            last_error = f"gamma_request_failed:{exc.__class__.__name__}"
            if attempt_idx + 1 >= attempts:
                raise ResolutionFeedError(last_error) from exc
            time.sleep(max(0.0, float(retry_backoff_sec)))
            continue

        latency_ms = _elapsed_ms(started_at)
        if response.status_code != 200:
            last_error = f"gamma_http_{response.status_code}"
            if response.status_code >= 500 and attempt_idx + 1 < attempts:
                time.sleep(max(0.0, float(retry_backoff_sec)))
                continue
            raise ResolutionFeedError(last_error)

        try:
            payload = response.json()
        except Exception as exc:
            raise ResolutionFeedError(f"gamma_json_decode_failed:{exc}") from exc

        market: Optional[dict[str, Any]]
        if isinstance(payload, dict):
            market = payload
        elif isinstance(payload, list) and payload:
            first_item = payload[0]
            market = first_item if isinstance(first_item, dict) else None
        else:
            market = None

        if market is None:
            fallback = _fetch_gamma_market_by_slug_via_list(
                session,
                market_slug=slug,
                base_url=base_url,
                timeout_sec=timeout_sec,
            )
            if fallback is None:
                raise ResolutionFeedError("market_not_found")
            market = fallback
            endpoint_used = MARKETS_ENDPOINT
        else:
            endpoint_used = MARKET_BY_SLUG_ENDPOINT

        return {
            "market": market,
            "fetch_meta": {
                "base_url": base_url,
                "endpoint": endpoint_used,
                "market_slug": slug,
                "latency_ms": latency_ms,
                "retry_count": attempt_idx,
            },
        }

    raise ResolutionFeedError(last_error or "gamma_fetch_failed")


def _fetch_gamma_market_by_slug_via_list(
    session: requests.Session,
    *,
    market_slug: str,
    base_url: str,
    timeout_sec: int,
) -> Optional[dict[str, Any]]:
    try:
        response = session.get(
            f"{base_url.rstrip('/')}{MARKETS_ENDPOINT}",
            params={"slug": str(market_slug or "").strip()},
            timeout=timeout_sec,
        )
    except requests.RequestException:
        return None

    if response.status_code != 200:
        return None

    try:
        payload = response.json()
    except Exception:
        return None

    if not isinstance(payload, list) or not payload:
        return None

    first_item = payload[0]
    return first_item if isinstance(first_item, dict) else None


def derive_resolution_decision(
    db_market: Mapping[str, Any],
    gamma_market: Mapping[str, Any],
    *,
    now_ts: Optional[int] = None,
    source_name: str = DEFAULT_SOURCE_NAME,
    fetch_meta: Optional[Mapping[str, Any]] = None,
) -> ResolutionDecision:
    db_market = dict(db_market)
    gamma_market = dict(gamma_market)
    now_ts = int(now_ts or time.time())
    slot_end_ts = _safe_int(db_market.get("slot_end_ts"))
    outcome_labels = parse_jsonish_list(gamma_market.get("outcomes"))
    outcome_prices = [_safe_float(value) for value in parse_jsonish_list(gamma_market.get("outcomePrices"))]
    condition_id = str(gamma_market.get("conditionId") or gamma_market.get("id") or "").strip()
    expected_market_id = str(db_market.get("market_id") or "").strip()
    resolution_source = str(gamma_market.get("resolutionSource") or source_name).strip() or source_name
    resolution_status_raw = str(gamma_market.get("umaResolutionStatus") or "").strip().lower()
    updated_at_ts = parse_iso_ts(gamma_market.get("updatedAt"))
    closed_time_ts = parse_iso_ts(gamma_market.get("closedTime"))
    uma_end_ts = parse_iso_ts(gamma_market.get("umaEndDate"))
    end_date_ts = parse_iso_ts(gamma_market.get("endDate"))

    event_meta = {
        "market_slug": str(db_market.get("market_slug") or gamma_market.get("slug") or ""),
        "gamma_market_id": str(gamma_market.get("id") or ""),
        "gamma_condition_id": condition_id,
        "resolution_status_raw": resolution_status_raw or None,
        "resolution_source": resolution_source,
        "outcomes": outcome_labels,
        "outcome_prices": outcome_prices,
        "closed": bool(gamma_market.get("closed")),
        "active": gamma_market.get("active"),
        "accepting_orders": gamma_market.get("acceptingOrders"),
        "enable_orderbook": gamma_market.get("enableOrderBook"),
        "archived": bool(gamma_market.get("archived")),
        "fetch_meta": dict(fetch_meta or {}),
    }

    if condition_id and expected_market_id and condition_id != expected_market_id:
        return ResolutionDecision(
            status=str(db_market.get("market_resolution_status") or "ACTIVE"),
            updates={
                "label_quality_flag": "MARKET_ID_MISMATCH",
                "resolution_source": resolution_source,
                "notes": f"gamma_condition_id_mismatch:{condition_id}",
            },
            event_type=None,
            event_ts=None,
            event_reason=None,
            event_meta=event_meta,
            outcome=None,
            quality_flag="MARKET_ID_MISMATCH",
        )

    if resolution_status_raw == "resolved":
        yes_price = outcome_prices[0] if len(outcome_prices) > 0 else None
        no_price = outcome_prices[1] if len(outcome_prices) > 1 else None
        winner_index, is_ambiguous = determine_winner_index(outcome_prices)
        resolved_outcome = None
        if winner_index is not None:
            if winner_index < len(outcome_labels):
                resolved_outcome = str(outcome_labels[winner_index])
            elif winner_index == 0:
                resolved_outcome = "YES"
            elif winner_index == 1:
                resolved_outcome = "NO"

        resolved_ts = first_not_none(uma_end_ts, closed_time_ts, updated_at_ts, slot_end_ts, now_ts)
        settled_ts = first_not_none(updated_at_ts, resolved_ts, slot_end_ts, now_ts)
        quality_flag = "AMBIGUOUS_OUTCOME_VECTOR" if is_ambiguous or winner_index is None else "OFFICIAL_RESOLVED"
        event_meta["winner_index"] = winner_index
        event_meta["ambiguous_outcome_vector"] = bool(is_ambiguous or winner_index is None)

        return ResolutionDecision(
            status="RESOLVED",
            updates={
                "market_status": "RESOLVED",
                "market_resolution_status": "RESOLVED",
                "resolved_outcome": resolved_outcome,
                "resolved_yes_price": yes_price,
                "resolved_no_price": no_price,
                "resolved_ts": resolved_ts,
                "settled_ts": settled_ts,
                "resolution_source": resolution_source,
                "label_quality_flag": quality_flag,
                "notes": None,
            },
            event_type="RESOLVED",
            event_ts=resolved_ts,
            event_reason="official_gamma_resolved",
            event_meta=event_meta,
            outcome=resolved_outcome,
            quality_flag=quality_flag,
        )

    if is_cancelled_market(gamma_market, resolution_status_raw):
        cancelled_ts = first_not_none(updated_at_ts, closed_time_ts, end_date_ts, slot_end_ts, now_ts)
        event_meta["cancelled_heuristic"] = True
        return ResolutionDecision(
            status="CANCELLED",
            updates={
                "market_status": "CANCELLED",
                "market_resolution_status": "CANCELLED",
                "settled_ts": cancelled_ts,
                "resolution_source": resolution_source,
                "label_quality_flag": "CANCELLED_OR_UNRESOLVED",
            },
            event_type="CANCELLED",
            event_ts=cancelled_ts,
            event_reason="official_gamma_cancelled",
            event_meta=event_meta,
            outcome=None,
            quality_flag="CANCELLED_OR_UNRESOLVED",
        )

    if bool(gamma_market.get("closed")) or (slot_end_ts is not None and slot_end_ts <= now_ts):
        pending_ts = first_not_none(slot_end_ts, end_date_ts, closed_time_ts, now_ts)
        return ResolutionDecision(
            status="PENDING_SETTLEMENT",
            updates={
                "market_status": "PENDING_SETTLEMENT",
                "market_resolution_status": "PENDING_SETTLEMENT",
                "resolution_source": resolution_source,
                "label_quality_flag": "MISSING_OFFICIAL_RESOLUTION",
            },
            event_type="PENDING_SETTLEMENT",
            event_ts=pending_ts,
            event_reason="official_gamma_pending_settlement",
            event_meta=event_meta,
            outcome=None,
            quality_flag="MISSING_OFFICIAL_RESOLUTION",
        )

    return ResolutionDecision(
        status="ACTIVE",
        updates={
            "market_status": "ACTIVE",
            "market_resolution_status": "ACTIVE",
            "resolution_source": resolution_source,
            "label_quality_flag": None,
        },
        event_type=None,
        event_ts=None,
        event_reason=None,
        event_meta=event_meta,
        outcome=None,
        quality_flag="ACTIVE",
    )


def determine_winner_index(outcome_prices: list[Optional[float]]) -> tuple[Optional[int], bool]:
    if len(outcome_prices) < 2:
        return None, True

    first = outcome_prices[0]
    second = outcome_prices[1]
    if first is None or second is None:
        return None, True

    if first >= RESOLVED_BINARY_MIN and second <= RESOLVED_BINARY_MAX:
        return 0, False
    if second >= RESOLVED_BINARY_MIN and first <= RESOLVED_BINARY_MAX:
        return 1, False

    if abs((first + second) - 1.0) <= 0.02:
        if first > second:
            return 0, True
        if second > first:
            return 1, True

    return None, True


def is_cancelled_market(gamma_market: Mapping[str, Any], resolution_status_raw: str) -> bool:
    if resolution_status_raw in {"cancelled", "canceled"}:
        return True

    archived = bool(gamma_market.get("archived"))
    active = gamma_market.get("active")
    accepting_orders = gamma_market.get("acceptingOrders")
    enable_orderbook = gamma_market.get("enableOrderBook")
    closed = bool(gamma_market.get("closed"))

    return archived and active is False and accepting_orders is False and enable_orderbook is False and closed


def parse_jsonish_list(raw_value: Any) -> list[Any]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return list(raw_value)
    if isinstance(raw_value, tuple):
        return list(raw_value)
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return [part.strip() for part in text.split(",") if part.strip()]
        return list(parsed) if isinstance(parsed, list) else []
    return []


def parse_iso_ts(raw_value: Any) -> Optional[int]:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, (int, float)):
        return int(raw_value)
    try:
        dt = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def first_not_none(*values: Optional[int]) -> Optional[int]:
    for value in values:
        if value is not None:
            return int(value)
    return None


def _elapsed_ms(started_at: float) -> int:
    return max(0, int((time.perf_counter() - started_at) * 1000))


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None
