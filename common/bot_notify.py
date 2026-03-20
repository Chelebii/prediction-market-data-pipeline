import html
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

_LAST_ALERT_TS: dict[str, float] = {}
_DEDUPE_DIR = Path(__file__).resolve().parents[1] / "state"


def _dedupe_path(key: str) -> Path:
    safe = key.replace("::", "_").replace(" ", "_").replace("/", "_").replace("\\", "_")
    return _DEDUPE_DIR / f".dedupe_{safe}"


def _check_file_dedupe(key: str, window_sec: int) -> bool:
    path = _dedupe_path(key)
    try:
        if path.exists() and (time.time() - path.stat().st_mtime) < window_sec:
            return True
    except Exception:
        pass
    return False


def _set_file_dedupe(key: str) -> None:
    try:
        _DEDUPE_DIR.mkdir(parents=True, exist_ok=True)
        _dedupe_path(key).write_text(str(time.time()), encoding="utf-8")
    except Exception:
        pass


def _ensure_env_loaded() -> None:
    if os.getenv("TELEGRAM_BOT_TOKEN", "").strip() and os.getenv("TELEGRAM_CHAT_ID", "").strip():
        return

    candidate_paths = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[1] / "polymarket_scanner" / ".env",
    ]
    for env_path in candidate_paths:
        try:
            if env_path.exists():
                load_dotenv(env_path, override=False)
        except Exception:
            pass
        if os.getenv("TELEGRAM_BOT_TOKEN", "").strip() and os.getenv("TELEGRAM_CHAT_ID", "").strip():
            return


def _display_label(bot_label: str) -> str:
    normalized = (bot_label or "").strip().upper()
    label_map = {
        "BTC5M-CLOB": "BTC5M Scanner",
        "BTC5M-REF": "BTC5M Reference",
        "BTC5M-RES": "BTC5M Resolution",
        "BTC5M-DATA": "BTC5M Health",
    }
    if normalized in label_map:
        return label_map[normalized]
    if not normalized:
        return "BTC5M"
    return (bot_label or "").strip()


def _normalize_message(msg: str) -> str:
    return " ".join(str(msg or "").strip().split())


def send_alert(bot_label: str, msg: str, level: str = "ERROR", dedupe_seconds: int = 600) -> None:
    _ensure_env_loaded()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    normalized_level = (level or "INFO").upper()
    normalized_msg = _normalize_message(msg)
    now = time.time()
    key = f"alert::{(bot_label or '').upper()}::{normalized_msg}::{normalized_level}"

    last_ts = _LAST_ALERT_TS.get(key)
    if dedupe_seconds and last_ts and (now - last_ts) < dedupe_seconds:
        return
    if dedupe_seconds and _check_file_dedupe(key, dedupe_seconds):
        return

    icon_map = {
        "ERROR": "X",
        "INFO": "i",
        "SUCCESS": "OK",
        "WARN": "!",
        "ALERT": "!!",
    }
    icon = icon_map.get(normalized_level, "")
    ts = datetime.now().strftime("%H:%M:%S")
    label = _display_label(bot_label)
    text = f"{icon} <b>{html.escape(label)}</b> | {normalized_level}\n<code>{ts} | {html.escape(normalized_msg)}</code>"

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if dedupe_seconds:
            _LAST_ALERT_TS[key] = now
            _set_file_dedupe(key)
    except Exception:
        pass
