import os
import time
import html
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

_LAST_EVENT_TS = {}
_EVENT_HISTORY = {}
_MUTE_UNTIL = {}

# File-based dedupe: process restart'larinda bile cift bildirim engeller
_DEDUPE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "state")


def _file_dedupe_key_path(key: str) -> str:
    safe = key.replace("::", "_").replace(" ", "_").replace("/", "_")
    return os.path.join(_DEDUPE_DIR, f".dedupe_{safe}")


def _check_file_dedupe(key: str, window_sec: int) -> bool:
    """True = dedupe aktif (gonderme), False = gonder."""
    path = _file_dedupe_key_path(key)
    try:
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            if (time.time() - mtime) < window_sec:
                return True  # dedupe: gonderme
    except Exception:
        pass
    return False


def _set_file_dedupe(key: str) -> None:
    try:
        os.makedirs(_DEDUPE_DIR, exist_ok=True)
        path = _file_dedupe_key_path(key)
        with open(path, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def _ensure_env_loaded() -> None:
    if os.getenv("TELEGRAM_BOT_TOKEN", "").strip() and os.getenv("TELEGRAM_CHAT_ID", "").strip():
        return
    try:
        env_path = Path.cwd() / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
    except Exception:
        pass


def _display_label(bot_label: str) -> str:
    s = (bot_label or "").strip().upper()
    if s.startswith("F") and s[1:].isdigit():
        return "Fast"
    if s.startswith("A") and s[1:].isdigit():
        return "Core"
    if s.startswith("P") and s[1:].isdigit():
        return "Pair"
    if s.startswith("S") and s[1:].isdigit():
        return "Sports"
    if s == "5MIN":
        return "5min"
    return (bot_label or "").strip().capitalize()


def _humanize_alert_message(msg: str) -> str:
    raw = (msg or "").strip()
    low = raw.lower()

    if raw.startswith("CLOB API hatasi"):
        action = "buy" if "market_buy" in low else ("sell" if "market_sell" in low else "order")
        if "order couldn't be fully filled" in low or "fully filled or killed" in low:
            return (
                f"Order failed: the {action} order could not be fully filled inside the allowed price limit. "
                "FOK means the order must fill completely or be cancelled. Most likely cause: thin orderbook or tight price cap."
            )
        if "not enough balance / allowance" in low:
            return (
                f"Order failed: the {action} order was rejected because the exchange reported missing balance or allowance. "
                "This can also happen when the token balance is gone or the market is already closing."
            )
        if "no orderbook exists" in low:
            return "Order failed: this market no longer has an active orderbook. It is likely expired or waiting for settlement."

    if raw.startswith("CLOB order basarisiz"):
        if "market_buy" in low and "tum retry'lar basarisiz" in low:
            return (
                "Buy order failed after retries. The bot tried to open a position but could not get a full fill. "
                "No new position was opened."
            )
        if "market_sell" in low and "tum retry'lar basarisiz" in low:
            return (
                "Sell order failed after retries. The bot tried to close the position but could not execute the exit. "
                "Check liquidity, settlement state, or token balance."
            )

    if raw.startswith("LIVE CLOSE BASARISIZ"):
        return (
            "Live close failed. The bot tried to exit the open position, but the sell order could not be fully filled. "
            "This usually means the orderbook was too thin at that moment."
        )

    if "kill switch aktif" in low and "live close failed" in low:
        return (
            "Kill switch activated because the bot could not close a live position safely. "
            "New live trades were stopped to prevent a larger uncontrolled loss."
        )

    if "kill switch aktif" in low:
        return raw

    return raw


def send_bot_event(bot_label: str, event: str, details: str = "", level: str = "INFO") -> None:
    _ensure_env_loaded()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    now = time.time()
    event_u = (event or "").upper()
    details_s = (details or "").strip()
    bot_u = (bot_label or "").upper()

    # STOPPED INFO spamini engelle: sadece non-zero code olanlar kalsin.
    if event_u == "STOPPED":
        code = None
        if "code=" in details_s:
            try:
                code = int(details_s.split("code=")[-1].split()[0].strip())
            except Exception:
                code = None
        if code in (0, None):
            return

    # Loop guard: ayni bot 60 sn icinde 3x START + 3x STOP (toplam>=6) gorurse,
    # hata/kriz disi eventleri 30 dk sustur.
    hist = _EVENT_HISTORY.get(bot_u, [])
    hist = [(ts, ev) for ts, ev in hist if (now - ts) <= 60]
    if event_u in {"STARTED", "STOPPED"}:
        hist.append((now, event_u))
    _EVENT_HISTORY[bot_u] = hist

    start_count = sum(1 for _, ev in hist if ev == "STARTED")
    stop_count = sum(1 for _, ev in hist if ev == "STOPPED")
    if start_count >= 3 and stop_count >= 3:
        _MUTE_UNTIL[bot_u] = now + 1800

    mute_until = _MUTE_UNTIL.get(bot_u, 0)
    if now < mute_until and event_u not in {"DEAD", "TARGET_HIT"}:
        return

    dedupe_window = 0
    if event_u == "STARTED":
        dedupe_window = 120
    elif event_u == "STOPPED":
        dedupe_window = 120
    elif event_u in {"DEAD", "TARGET_HIT"}:
        dedupe_window = 30

    key = f"{bot_u}::{event_u}"
    if event_u in {"DEAD", "TARGET_HIT"}:
        key = f"{bot_u}::{event_u}::{details_s}"

    last_ts = _LAST_EVENT_TS.get(key)
    if dedupe_window and last_ts and (now - last_ts) < dedupe_window:
        return

    # File-based dedupe: process restart sonrasi da cift bildirim engeller
    if dedupe_window and _check_file_dedupe(key, dedupe_window):
        return

    icon_map = {
        "INFO": "ℹ️",
        "WARN": "⚠️",
        "ERROR": "❌",
        "SUCCESS": "✅",
    }
    icon = icon_map.get(level.upper(), "")
    ts = datetime.now().strftime("%H:%M:%S")

    line2 = f"<code>{ts}</code>"
    if details:
        line2 = f"<code>{ts} | {details}</code>"

    label = _display_label(bot_label)
    text = f"{icon} <b>{label}</b> | {html.escape(event)}\n{line2}"

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if dedupe_window:
            _LAST_EVENT_TS[key] = now
            _set_file_dedupe(key)
    except Exception:
        pass


def send_alert(bot_label: str, msg: str, level: str = "ERROR", dedupe_seconds: int = 600) -> None:
    _ensure_env_loaded()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    now = time.time()
    key = f"alert::{(bot_label or '').upper()}::{(msg or '').strip()}::{(level or '').upper()}"
    last_ts = _LAST_EVENT_TS.get(key)
    if dedupe_seconds and last_ts and (now - last_ts) < dedupe_seconds:
        return

    icon_map = {
        "ERROR": "❌",
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARN": "⚠️",
        "ALERT": "🚨",
    }
    icon = icon_map.get((level or "").upper(), "")
    ts = datetime.now().strftime("%H:%M:%S")
    label = _display_label(bot_label)
    readable_msg = _humanize_alert_message(msg)
    text = f"{icon} <b>{label}</b> | {(level or 'INFO').upper()}\n<code>{ts} | {html.escape(readable_msg)}</code>"

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if dedupe_seconds:
            _LAST_EVENT_TS[key] = now
    except Exception:
        pass


def send_trade_notification(
    bot_label: str,
    *,
    event: str,
    coin: str,
    outcome: str,
    entry_price: float | None = None,
    exit_price: float | None = None,
    pnl_usd: float | None = None,
    pnl_pct: float | None = None,
    size_usd: float | None = None,
    reason: str = "",
    balance_usd: float | None = None,
    mode: str = "",
) -> None:
    _ensure_env_loaded()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    label = _display_label(bot_label)
    ts = datetime.now().strftime("%H:%M:%S")
    event_u = (event or "").upper().strip()

    if event_u == "OPEN":
        icon = "🟦"
        title = "Position Opened"
    elif event_u == "CLOSE":
        pnl_value = float(pnl_usd or 0.0)
        icon = "🟢" if pnl_value >= 0 else "🔴"
        title = "Position Closed"
    else:
        icon = "📌"
        title = event_u or "Trade Update"

    lines = [f"{icon} <b>{label}</b> | {html.escape(title)}", f"<code>{ts}</code>"]
    if mode:
        lines.append(f"Mode: <b>{html.escape(str(mode).upper())}</b>")
    lines.append(f"Asset: <b>{html.escape(str(coin))}</b> {html.escape(str(outcome))}")
    if size_usd is not None:
        lines.append(f"Size: <b>${float(size_usd):.2f}</b>")
    if entry_price is not None:
        lines.append(f"Buy: <b>{float(entry_price):.3f}</b>")
    if exit_price is not None:
        lines.append(f"Sell: <b>{float(exit_price):.3f}</b>")
    if pnl_usd is not None:
        pnl_value = float(pnl_usd)
        pnl_icon = "🟢" if pnl_value >= 0 else "🔴"
        lines.append(f"{pnl_icon} PnL: <b>{pnl_value:+.2f}$</b>")
    if pnl_pct is not None:
        pnlp = float(pnl_pct) * 100.0
        pct_icon = "🟢" if pnlp >= 0 else "🔴"
        lines.append(f"{pct_icon} Return: <b>{pnlp:+.2f}%</b>")
    if reason:
        lines.append(f"Reason: <b>{html.escape(reason)}</b>")
    if balance_usd is not None:
        lines.append(f"Wallet: <b>${float(balance_usd):.2f}</b>")

    text = "\n".join(lines)
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass
