"""Helpers for classifying collector network and transport failures."""

from __future__ import annotations

from typing import Optional

import requests


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
    pieces = [f"{component} network/vpn sorunu tespit etti", f"error={reason}"]
    if extra:
        pieces.append(str(extra))
    return " | ".join(pieces)
