"""Helpers for classifying collector network and transport failures."""

from __future__ import annotations

import time
from typing import Optional

import requests

_NETWORK_ALERT_STATES: dict[str, dict[str, object]] = {}


def classify_requests_exception(exc: BaseException) -> str:
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return f"request_connect_timeout:{exc.__class__.__name__}"
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return f"request_read_timeout:{exc.__class__.__name__}"
    if isinstance(exc, requests.exceptions.ProxyError):
        return f"request_proxy_error:{exc.__class__.__name__}"
    if isinstance(exc, requests.exceptions.SSLError):
        return f"request_ssl_error:{exc.__class__.__name__}"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return f"request_connection_error:{exc.__class__.__name__}"
    if isinstance(exc, requests.exceptions.RequestException):
        return f"request_error:{exc.__class__.__name__}"
    return f"runtime_error:{exc.__class__.__name__}"


def normalize_error_reason(reason: object) -> str:
    text = str(reason or "").strip()
    return text.lower()


def is_network_reason(reason: object) -> bool:
    text = normalize_error_reason(reason)
    if not text:
        return False
    prefixes = (
        "request_connect_timeout:",
        "request_read_timeout:",
        "request_proxy_error:",
        "request_ssl_error:",
        "request_connection_error:",
        "request_error:",
        "gamma_request_failed:",
    )
    if text.startswith(prefixes):
        return True
    return any(
        token in text
        for token in (
            "proxyerror",
            "connecttimeout",
            "readtimeout",
            "connectionerror",
            "connection reset",
            "name resolution",
            "temporarily unavailable",
            "tunnel",
            "ssl",
        )
    )


def build_network_alert_message(component: str, reason: object, *, extra: Optional[str] = None) -> str:
    pieces = [f"{component} detected a network/VPN issue", f"error={reason}"]
    if extra:
        pieces.append(str(extra))
    return " | ".join(pieces)


def note_network_alert_state(
    state_key: str,
    reason: object,
    *,
    source: str = "",
    threshold_count: int = 3,
    min_duration_sec: int = 0,
    reset_after_sec: int = 120,
) -> dict[str, object]:
    now = time.time()
    key = str(state_key or "").strip().lower() or "default"
    normalized_reason = str(reason or "").strip() or "unknown"
    normalized_source = str(source or "").strip()
    reset_window = max(30, int(reset_after_sec))
    threshold = max(1, int(threshold_count))

    state = dict(_NETWORK_ALERT_STATES.get(key) or {})
    last_ts = state.get("last_ts")
    if last_ts is None or (now - float(last_ts)) > reset_window:
        state = {
            "first_ts": now,
            "last_ts": now,
            "count": 1,
            "alert_sent": False,
            "last_reason": normalized_reason,
            "last_source": normalized_source,
        }
    else:
        state["last_ts"] = now
        state["count"] = int(state.get("count") or 0) + 1
        state["last_reason"] = normalized_reason
        state["last_source"] = normalized_source

    duration_sec = max(0, int(now - float(state.get("first_ts") or now)))
    should_alert = (
        (not bool(state.get("alert_sent")))
        and int(state.get("count") or 0) >= threshold
        and duration_sec >= max(0, int(min_duration_sec))
    )
    if should_alert:
        state["alert_sent"] = True

    _NETWORK_ALERT_STATES[key] = state
    return {
        "should_alert": should_alert,
        "count": int(state.get("count") or 0),
        "duration_sec": duration_sec,
        "reason": str(state.get("last_reason") or normalized_reason),
        "source": str(state.get("last_source") or normalized_source),
    }


def clear_network_alert_state(state_key: str) -> None:
    key = str(state_key or "").strip().lower()
    if not key:
        return
    _NETWORK_ALERT_STATES.pop(key, None)


def build_network_intervention_message(
    component: str,
    reason: object,
    *,
    source: str = "",
    failure_count: int,
    duration_sec: int,
    extra: Optional[str] = None,
) -> str:
    pieces = [
        f"Action required: {component} network/VPN issue is persisting",
        f"error={reason}",
        f"consecutive_failures={int(failure_count)}",
        f"duration_sec={max(0, int(duration_sec))}",
    ]
    if source:
        pieces.append(f"source={source}")
    if extra:
        pieces.append(str(extra))
    return " | ".join(pieces)
