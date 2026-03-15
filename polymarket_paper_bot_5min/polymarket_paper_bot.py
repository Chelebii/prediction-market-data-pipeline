"""
Polymarket 5-Minute Crypto Up/Down Paper Bot (v2 - Snapshot based)
==================================================================
Trades BTC, ETH, SOL, XRP 5-minute prediction markets using shared snapshot.
"""

import os
import sys
import json
import csv
import time
import sqlite3
import logging
import requests
import argparse
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from common.single_instance import acquire_single_instance_lock
from common.bot_notify import send_alert, send_trade_notification
from common.run_registry import touch_heartbeat
from common.execution import ExecutionEngine, migrate_db
from common.safety import SafetyManager
from common.clob_client import ClobClientManager
from common.wallet_manager import WalletManager, load_credentials_from_env

def telegram_alert(msg, level="ERROR"):
    bot_label = os.getenv("BOT_LABEL", os.path.basename(os.path.dirname(__file__))).strip()
    send_alert(bot_label=bot_label, msg=msg, level=level)

_no_snapshot_count = 0
_last_no_snapshot_log_ts = 0
_quote_stability_state: Dict[str, Dict[str, float]] = {}
_last_entry_guard_log_ts = 0

# Bu bot her calistiginda tek bir run klasoru kullanir.
# O klasorun icinde DB, log ve CSV dosyalari tutulur.
parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", type=str, default=".", help="Run directory")
args = parser.parse_args()

RUN_DIR = args.run_dir
os.makedirs(RUN_DIR, exist_ok=True)
acquire_single_instance_lock(os.path.join(RUN_DIR, "bot.lock"), process_name="5min-bot", on_log=print)

load_dotenv(os.path.join(RUN_DIR, ".env"))
load_dotenv()

# Env ayarlari botun ana davranisini belirler.
# Yeni bir ayar denemek istediginde ilk bakman gereken blok genelde burasidir.
COINS = ["btc"]
COIN_NAMES = {"btc": "Bitcoin", "eth": "Ethereum", "sol": "Solana", "xrp": "XRP"}
BINANCE_SYMBOLS = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}

# Temel calisma ayarlari:
# - SCAN_INTERVAL_SEC: snapshot kac saniyede bir okunacak
# - POSITION_SIZE_USD: tek isleme ayrilan dolar miktari
# - INITIAL_BALANCE: equity ve dashboard hesaplarinda baz alinacak baslangic para
SCAN_INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "10"))
POSITION_SIZE_USD = float(os.getenv("POSITION_SIZE_USD", "5"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "10"))
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000"))

SHARED_SNAPSHOT_PATH = os.getenv("SHARED_SNAPSHOT_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "runtime", "snapshots", "btc_5min_clob_snapshot.json"))
SHARED_MAX_AGE_SEC = 60
ALLOW_NEW_ENTRIES = (os.getenv("ALLOW_NEW_ENTRIES", "1").strip() != "0")

# Strategy ayarlari:
# Bot burada "hangi setup trade acmaya deger" sorusuna cevap veriyor.
# En cok oynanan ayarlar genelde fiyat bandi, spread limiti ve momentum esigidir.
ENTRY_MIN_PRICE = float(os.getenv("ENTRY_MIN_PRICE", "0.20"))
ENTRY_MAX_PRICE = float(os.getenv("ENTRY_MAX_PRICE", "0.80"))
TARGET_EXIT_PRICE_UP = float(os.getenv("TARGET_EXIT_PRICE_UP", "0.85"))
_raw_target_exit_down = float(os.getenv("TARGET_EXIT_PRICE_DOWN", "0.85"))
# Legacy configs stored the complementary YES-side price (for example 0.15).
# Live trading uses the actual NO token, so normalize sub-0.5 values to NO-side targets.
TARGET_EXIT_PRICE_DOWN = _raw_target_exit_down if _raw_target_exit_down >= 0.5 else round(1.0 - _raw_target_exit_down, 3)
ENTRY_CUTOFF_SEC = int(os.getenv("ENTRY_CUTOFF_SEC", "270"))
MIN_ENTRY_SEC = int(os.getenv("MIN_ENTRY_SEC", "20"))
FORCE_EXIT_SEC = int(os.getenv("FORCE_EXIT_SEC", "285"))
MAX_TRADES_PER_SLOT = int(os.getenv("MAX_TRADES_PER_SLOT", "1"))
MAX_ENTRY_SPREAD = float(os.getenv("MAX_ENTRY_SPREAD", "0.03"))
MOMENTUM_MIN_PCT = float(os.getenv("MOMENTUM_MIN_PCT", "0.10"))
FALLBACK_SIGNAL_MIN_PCT = float(os.getenv("FALLBACK_SIGNAL_MIN_PCT", "0.08"))
SHORT_MOMENTUM_TOLERANCE_PCT = float(os.getenv("SHORT_MOMENTUM_TOLERANCE_PCT", "0.02"))
FALLBACK_ENTRY_MAX_PRICE = float(os.getenv("FALLBACK_ENTRY_MAX_PRICE", "0.42"))
FALLBACK_MAX_ENTRY_SPREAD = float(os.getenv("FALLBACK_MAX_ENTRY_SPREAD", "0.015"))
FALLBACK_MIN_QUOTE_STABLE_PASSES = int(os.getenv("FALLBACK_MIN_QUOTE_STABLE_PASSES", "3"))
MAX_LOSS_USD_PER_TRADE = float(os.getenv("MAX_LOSS_USD_PER_TRADE", "2.0"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.50"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.30"))
PRINCIPAL_TAKE_MULTIPLIER = float(os.getenv("PRINCIPAL_TAKE_MULTIPLIER", "2.0"))
RUNNER_FINAL_TARGET_PRICE = float(os.getenv("RUNNER_FINAL_TARGET_PRICE", "0.97"))
RUNNER_TRAILING_STOP_PCT = float(os.getenv("RUNNER_TRAILING_STOP_PCT", "0.18"))
LIVE_MARKET_BUY_BUFFER = float(os.getenv("LIVE_MARKET_BUY_BUFFER", "0.02"))
LIVE_MARKET_SELL_BUFFER = float(os.getenv("LIVE_MARKET_SELL_BUFFER", "0.02"))
THIN_BOOK_EXIT_SPREAD = float(os.getenv("THIN_BOOK_EXIT_SPREAD", "0.08"))
THIN_BOOK_EXIT_CRITICAL_SPREAD = float(os.getenv("THIN_BOOK_EXIT_CRITICAL_SPREAD", "0.14"))
THIN_BOOK_EXIT_SEC_LEFT = int(os.getenv("THIN_BOOK_EXIT_SEC_LEFT", "60"))
MAX_TOTAL_DRAWDOWN_USD = float(os.getenv("MAX_TOTAL_DRAWDOWN_USD", "20"))
LIVE_MIN_CLOSED_TRADES = int(os.getenv("LIVE_MIN_CLOSED_TRADES", "30"))
LIVE_MIN_NET_PNL_USD = float(os.getenv("LIVE_MIN_NET_PNL_USD", "5"))
LIVE_EVAL_LOOKBACK_TRADES = int(os.getenv("LIVE_EVAL_LOOKBACK_TRADES", "20"))
LIVE_MIN_PROFIT_FACTOR = float(os.getenv("LIVE_MIN_PROFIT_FACTOR", "1.20"))
LIVE_MAX_FORCE_EXIT_RATE = float(os.getenv("LIVE_MAX_FORCE_EXIT_RATE", "0.45"))
LIVE_MAX_STOP_RATE = float(os.getenv("LIVE_MAX_STOP_RATE", "0.30"))
LIVE_MIN_RECENT_PNL_USD = float(os.getenv("LIVE_MIN_RECENT_PNL_USD", "1.0"))
LIVE_ALLOW_UNPROVEN = os.getenv("LIVE_ALLOW_UNPROVEN", "0").strip().lower() in {"1", "true", "yes", "on"}
HEALTH_MIN_CLOSED_TRADES = int(os.getenv("HEALTH_MIN_CLOSED_TRADES", "12"))
HEALTH_LOOKBACK_TRADES = int(os.getenv("HEALTH_LOOKBACK_TRADES", "12"))
HEALTH_MIN_PROFIT_FACTOR = float(os.getenv("HEALTH_MIN_PROFIT_FACTOR", "1.05"))
HEALTH_MAX_FORCE_EXIT_RATE = float(os.getenv("HEALTH_MAX_FORCE_EXIT_RATE", "0.55"))
HEALTH_MAX_STOP_RATE = float(os.getenv("HEALTH_MAX_STOP_RATE", "0.35"))
HEALTH_MIN_RECENT_PNL_USD = float(os.getenv("HEALTH_MIN_RECENT_PNL_USD", "-1.0"))
MIN_QUOTE_STABLE_PASSES = int(os.getenv("MIN_QUOTE_STABLE_PASSES", "2"))
MAX_QUOTE_JUMP_PCT = float(os.getenv("MAX_QUOTE_JUMP_PCT", "0.05"))
CONSECUTIVE_LOSS_COOLDOWN_TRIGGER = int(os.getenv("CONSECUTIVE_LOSS_COOLDOWN_TRIGGER", "3"))
LOSS_COOLDOWN_SLOTS = int(os.getenv("LOSS_COOLDOWN_SLOTS", "2"))
SIMULATED_FEE_PCT = float(os.getenv("SIMULATED_FEE_PCT", "0.02"))
SIMULATED_SPREAD_ENABLED = os.getenv("SIMULATED_SPREAD_ENABLED", "1").strip() != "0"
DEPTH_FACTOR = float(os.getenv("DEPTH_FACTOR", "0.1"))
EXIT_SLIPPAGE_MULTIPLIER = float(os.getenv("EXIT_SLIPPAGE_MULTIPLIER", "1.5"))
TP_EXIT_BUFFER = float(os.getenv("TP_EXIT_BUFFER", "0.015"))
FORCE_EXIT_BUFFER = float(os.getenv("FORCE_EXIT_BUFFER", "0.03"))
TRADE_NOTIFICATIONS_ENABLED = os.getenv("TRADE_NOTIFICATIONS_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

DB_PATH = os.path.join(RUN_DIR, "paper_trades.db")
LOG_PATH = os.path.join(RUN_DIR, "bot.log")
BALANCE_PATH = os.path.join(RUN_DIR, "CURRENT_BALANCE.txt")
TRADE_EVENTS_CSV_PATH = os.path.join(RUN_DIR, "trade_events.csv")
CLOSED_TRADES_CSV_PATH = os.path.join(RUN_DIR, "closed_trades.csv")
BOT_KEY = "5min"

# Trading mode:
# - paper: her sey simule edilir
# - dry-run: live akis test edilir ama gercek emir gitmez
# - live: gercek emir gonderilir
TRADING_MODE = os.getenv("TRADING_MODE", "paper").strip().lower()
_engine = None

# Paper/dry-run modda birebir gercek fill alamayiz.
# Bu fonksiyon spread, fee ve market impact ekleyip daha gercekci bir tahmini fill fiyati uretir.
def simulate_execution_price(mid: float, spread: float, size_usd: float, liquidity: float, side: str, aggressiveness: float = 1.0) -> float:
    mid_v = max(0.01, float(mid))
    spr_v = max(0.0, float(spread))
    liq_v = max(1.0, float(liquidity))
    depth_v = max(0.0001, float(DEPTH_FACTOR))
    ask = mid_v + (spr_v / 2.0)
    bid = max(0.01, mid_v - (spr_v / 2.0))
    impact = (float(size_usd) / (liq_v * depth_v)) * mid_v * max(1.0, float(aggressiveness))
    fee = SIMULATED_FEE_PCT * max(1.0, float(aggressiveness))
    if side == "buy":
        return min(0.99, max(0.01, ask + impact + fee))
    return min(0.99, max(0.01, bid - impact - fee))

_bot_logger = logging.getLogger("5min_bot")
_bot_logger.setLevel(logging.INFO)
_bot_console = logging.StreamHandler()
_bot_console.setFormatter(logging.Formatter("[%(asctime)s] 5MIN | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_bot_logger.addHandler(_bot_console)
_bot_file = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_bot_file.setFormatter(logging.Formatter("[%(asctime)s] 5MIN | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_bot_logger.addHandler(_bot_file)


def log(msg: str):
    _bot_logger.info(msg)


def current_wallet_balance_for_notify(conn: sqlite3.Connection) -> Optional[float]:
    # Telegram'da kullaniciya local equity degil, olabildigince gercek bakiye goster.
    if TRADING_MODE == "live" and _engine is not None and _engine.clob_client is not None:
        try:
            bal = _engine.clob_client.get_collateral_balance()
            if bal is not None:
                return float(bal)
        except Exception:
            pass
    try:
        return float(current_balance(conn))
    except Exception:
        return None


def notify_trade_open(coin: str, outcome: str, entry_price: float, size_usd: float, signal_mode: str) -> None:
    if not TRADE_NOTIFICATIONS_ENABLED:
        return
    send_trade_notification(
        BOT_LABEL,
        event="OPEN",
        coin=coin,
        outcome=outcome,
        entry_price=entry_price,
        size_usd=size_usd,
        reason=signal_mode,
        mode=TRADING_MODE,
    )


def notify_trade_close(
    conn: sqlite3.Connection,
    *,
    coin: str,
    outcome: str,
    entry_price: float,
    exit_price: float,
    pnl_usd: float,
    pnl_pct: float,
    size_usd: float,
    reason: str,
) -> None:
    if not TRADE_NOTIFICATIONS_ENABLED:
        return
    send_trade_notification(
        BOT_LABEL,
        event="CLOSE",
        coin=coin,
        outcome=outcome,
        entry_price=entry_price,
        exit_price=exit_price,
        pnl_usd=pnl_usd,
        pnl_pct=pnl_pct,
        size_usd=size_usd,
        reason=reason,
        balance_usd=current_wallet_balance_for_notify(conn),
        mode=TRADING_MODE,
    )


# CSV export dosyalari sonradan trade incelemek icin tutulur.
# trade_events.csv her olayi, closed_trades.csv ise kapanan trade ozetini yazar.
TRADE_EVENT_HEADERS = [
    "recorded_at_utc",
    "run_id",
    "trading_mode",
    "event",
    "market_id",
    "market_slug",
    "question",
    "outcome",
    "token_id",
    "price",
    "size_usd",
    "pnl_usd",
    "reason",
    "coin",
    "signal_mode",
    "requested_entry_price",
    "entry_price",
    "principal_take_price",
    "runner_target",
    "pnl_pct",
    "order_id",
    "shares_sold",
    "remaining_shares",
    "remaining_cost_basis",
]

CLOSED_TRADE_HEADERS = [
    "closed_at_utc",
    "run_id",
    "trading_mode",
    "coin",
    "market_id",
    "market_slug",
    "question",
    "outcome",
    "opened_at_utc",
    "closed_ts",
    "entry_price",
    "exit_price",
    "position_size_usd_at_close",
    "original_size_usd",
    "realized_before_close",
    "pnl_usd",
    "pnl_pct",
    "close_reason",
    "principal_recovered",
    "entry_order_id",
    "close_order_id",
]


def _csv_iso(ts: Optional[int]) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""


def ensure_trade_csv_files() -> None:
    for path, headers in (
        (TRADE_EVENTS_CSV_PATH, TRADE_EVENT_HEADERS),
        (CLOSED_TRADES_CSV_PATH, CLOSED_TRADE_HEADERS),
    ):
        try:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                continue
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
        except Exception:
            pass


def append_csv_row(path: str, headers: List[str], row: Dict[str, Any]) -> None:
    ensure_trade_csv_files()
    cleaned: Dict[str, Any] = {}
    for key in headers:
        value = row.get(key, "")
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        elif value is None:
            value = ""
        cleaned[key] = value
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writerow(cleaned)

# SQLite dosyasi botun hafizasidir.
# Acik pozisyonlar, kapanan trade'ler ve signal journal burada tutulur.
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS paper_positions ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "coin TEXT, market_id TEXT, market_slug TEXT, question TEXT, slot_ts INTEGER, "
        "outcome TEXT, token_id TEXT, entry_price REAL, size_usd REAL, original_size_usd REAL, "
        "realized_pnl_usd REAL DEFAULT 0, principal_recovered INTEGER DEFAULT 0, opened_ts INTEGER, "
        "status TEXT, exit_price REAL, pnl_usd REAL, pnl_pct REAL, closed_ts INTEGER, "
        "high_price REAL, exit_tier INTEGER DEFAULT 0, close_reason TEXT)"
    )
    for ddl in [
        "ALTER TABLE paper_positions ADD COLUMN market_id TEXT",
        "ALTER TABLE paper_positions ADD COLUMN question TEXT",
        "ALTER TABLE paper_positions ADD COLUMN token_id TEXT",
        "ALTER TABLE paper_positions ADD COLUMN pnl_pct REAL",
        "ALTER TABLE paper_positions ADD COLUMN high_price REAL",
        "ALTER TABLE paper_positions ADD COLUMN original_size_usd REAL",
        "ALTER TABLE paper_positions ADD COLUMN realized_pnl_usd REAL DEFAULT 0",
        "ALTER TABLE paper_positions ADD COLUMN principal_recovered INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass
    try: conn.execute("ALTER TABLE paper_positions ADD COLUMN exit_tier INTEGER DEFAULT 0")
    except Exception: pass
    try: conn.execute("ALTER TABLE paper_positions ADD COLUMN close_reason TEXT")
    except Exception: pass
    conn.execute("CREATE TABLE IF NOT EXISTS signal_journal (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, event TEXT NOT NULL, market_id TEXT, market_slug TEXT, token_id TEXT, outcome TEXT, price REAL, size_usd REAL, pnl_usd REAL, reason TEXT, meta_json TEXT)")
    migrate_db(conn)
    conn.commit()
    ensure_trade_csv_files()
    return conn

# Binance verisi burada sadece yon teyidi icin kullaniliyor.
# Yani trade Polymarket'te aciliyor ama karar oncesi kisa vadeli momentum Binance'tan okunuyor.
def get_binance_momentum(symbol: str) -> Optional[float]:
    """Son 3 dakikalik momentum (2 kline)."""
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": symbol, "interval": "1m", "limit": 3}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 3:
                o = float(k[0][1])  # 3dk onceki open
                c = float(k[-1][4])  # son close
                return ((c - o) / o) * 100
    except Exception:
        pass
    return None


def get_binance_momentum_short(symbol: str) -> Optional[float]:
    """Son 1 dakikalik momentum (confirmation)."""
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": symbol, "interval": "1m", "limit": 1}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 1:
                o, c = float(k[0][1]), float(k[0][4])
                return ((c - o) / o) * 100
    except Exception:
        pass
    return None


def get_two_candle_direction(symbol: str) -> Optional[str]:
    """Son 2 kapanmis 1m mum ayni yone bakiyorsa Up veya Down doner."""
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": symbol, "interval": "1m", "limit": 3}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 3:
                closed = k[-3:-1]
                colors = []
                for row in closed:
                    o = float(row[1])
                    c = float(row[4])
                    if c > o:
                        colors.append("green")
                    elif c < o:
                        colors.append("red")
                    else:
                        return None
                if len(colors) == 2 and colors[0] == colors[1] == "green":
                    return "Up"
                if len(colors) == 2 and colors[0] == colors[1] == "red":
                    return "Down"
    except Exception:
        pass
    return None


def get_last_closed_candle_direction(symbol: str) -> Optional[str]:
    """Use the most recent closed 1m candle as a fallback directional hint."""
    try:
        r = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": symbol, "interval": "1m", "limit": 3}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 3:
                row = k[-2]
                o = float(row[1])
                c = float(row[4])
                if c > o:
                    return "Up"
                if c < o:
                    return "Down"
    except Exception:
        pass
    return None

# Scanner'in urettigi snapshot bu botun ana veri kaynagidir.
# Snapshot yoksa veya bayatsa bot yeni trade acmaz.
def read_snapshot():
    try:
        if not os.path.exists(SHARED_SNAPSHOT_PATH):
            return None
        with open(SHARED_SNAPSHOT_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        if (time.time() - d.get("ts", 0)) > SHARED_MAX_AGE_SEC:
            return None
        return d
    except Exception:
        return None


def normalize_snapshot(raw: dict):
    # Scanner farkli formatta veri uretse bile bot tek tip bir yapiyla calissin diye
    # once burada normalize ediyoruz.
    if not raw:
        return None
    if raw.get("source") in {"clob_book", "clob_price_mid"}:
        if not raw.get("book_valid"):
            return None
        slug = raw.get("market_slug")
        yes_tid = raw.get("yes_token_id")
        no_tid = raw.get("no_token_id")
        if not slug or not yes_tid or not no_tid:
            return None
        yes_mid = raw.get("yes_mid")
        no_mid = raw.get("no_mid")
        yes_spread = raw.get("spread_yes")
        no_spread = raw.get("spread_no")
        if None in (yes_mid, no_mid, yes_spread, no_spread):
            return None
        liquidity = float(raw.get("meta", {}).get("liquidity") or 5000)
        market_id = str(raw.get("market_id") or raw.get("conditionId") or slug)
        question = str(raw.get("question") or raw.get("meta", {}).get("question") or slug)
        return {
            "ts": raw.get("ts", 0),
            "source": raw.get("source", "clob_price_mid"),
            "book_valid": True,
            "markets": [{
                "market_id": market_id,
                "slug": slug,
                "question": question,
                "yes_token_id": yes_tid,
                "no_token_id": no_tid,
                "liquidity": liquidity,
                "tick_size": raw.get("tick_size"),
                "min_order_size": raw.get("min_order_size"),
            }],
            "mids": {
                yes_tid: float(yes_mid),
                no_tid: float(no_mid),
            },
            "spreads": {
                yes_tid: float(yes_spread),
                no_tid: float(no_spread),
            },
        }
    return raw


def compute_equity(conn, markets, mids):
    # Equity = baslangic bakiye + kapanan trade'ler + acik trade'lerin anlik etkisi.
    # Dashboard'da gordugun wallet/equity mantigi burada hesaplanir.
    closed = conn.execute("SELECT COALESCE(SUM(pnl_usd), 0) FROM paper_positions WHERE status='CLOSED'").fetchone()[0] or 0.0
    realized_open = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl_usd), 0) FROM paper_positions WHERE status IN ('OPEN', 'PENDING_SETTLEMENT')"
    ).fetchone()[0] or 0.0
    equity = INITIAL_BALANCE + float(closed) + float(realized_open)

    slug_map = {m.get("slug"): m for m in markets if m.get("slug")}
    rows = conn.execute("SELECT market_slug, outcome, token_id, entry_price, size_usd FROM paper_positions WHERE status='OPEN'").fetchall()
    for slug, outcome, token_id, entry, size_usd in rows:
        m = slug_map.get(slug)
        if not m:
            continue
        resolved_token_id = token_id or (m.get("yes_token_id") if outcome == "Up" else m.get("no_token_id"))
        cur = mids.get(resolved_token_id)
        if cur is None or entry in (None, 0):
            continue
        shares = size_usd / entry
        equity += (cur - entry) * shares

    return equity

def target_exit_price(outcome: str) -> float:
    return TARGET_EXIT_PRICE_UP if outcome == "Up" else TARGET_EXIT_PRICE_DOWN


def select_token_id(market: dict, outcome: str) -> Optional[str]:
    return market.get("yes_token_id") if outcome == "Up" else market.get("no_token_id")


def market_question(market: dict) -> str:
    return str(market.get("question") or market.get("slug") or "BTC Up or Down - 5 Minute Market")


def market_identifier(market: dict) -> str:
    return str(market.get("market_id") or market.get("slug") or "")


def market_tick_size(market: dict) -> Optional[float]:
    try:
        tick = float(market.get("tick_size") or 0.0)
    except Exception:
        return None
    return tick if tick > 0 else None


def market_min_order_size(market: dict) -> Optional[float]:
    try:
        value = float(market.get("min_order_size") or 0.0)
    except Exception:
        return None
    return value if value > 0 else None


def total_drawdown_floor() -> float:
    # Equity bu seviyenin altina inerse bot "yeter, bugunluk dur" der ve kendini kapatir.
    return max(0.0, INITIAL_BALANCE - max(0.0, MAX_TOTAL_DRAWDOWN_USD))


def live_buy_worst_price(mid_price: float, spread: float) -> float:
    ask = float(mid_price) + (max(0.0, float(spread)) / 2.0)
    buffer = max(max(0.0, float(spread)) * 0.5, LIVE_MARKET_BUY_BUFFER)
    return min(0.99, ask + buffer)


def live_sell_worst_price(mid_price: float, spread: float) -> float:
    bid = max(0.01, float(mid_price) - (max(0.0, float(spread)) / 2.0))
    buffer = max(max(0.0, float(spread)) * 0.5, LIVE_MARKET_SELL_BUFFER)
    return max(0.01, bid - buffer)


def position_shares(pos: Dict[str, Any]) -> float:
    entry = max(1e-9, float(pos["entry_price"]))
    return float(pos["size_usd"]) / entry


def live_token_has_orderbook(token_id: str) -> Optional[bool]:
    # Expiry sonrasi orderbook yoksa ayni tokeni tekrar tekrar satmaya calismayiz.
    if not token_id or TRADING_MODE != "live" or _engine is None or _engine.clob_client is None:
        return None
    checker = getattr(_engine.clob_client, "token_has_orderbook", None)
    if checker is None:
        return None
    try:
        return checker(token_id)
    except Exception:
        return None


def principal_take_price(pos: Dict[str, Any]) -> float:
    # Ana para geri alma hedefi.
    return min(0.99, float(pos["entry_price"]) * PRINCIPAL_TAKE_MULTIPLIER)


def fixed_take_profit_price(pos: Dict[str, Any]) -> float:
    # Yeni sade TP modeli:
    # fiyat entry'nin TAKE_PROFIT_PCT kadar ustune cikarsa pozisyonun tamami kapatilir.
    return min(0.99, float(pos["entry_price"]) * (1.0 + max(0.0, TAKE_PROFIT_PCT)))


def max_entry_price_for_principal_take() -> float:
    # Giris cok pahaliysa principal alma icin yeterli alan kalmaz.
    # Bu helper, giris fiyati icin ust mantikli siniri hesaplar.
    multiplier = max(1.01, PRINCIPAL_TAKE_MULTIPLIER)
    return max(ENTRY_MIN_PRICE, min(ENTRY_MAX_PRICE, (0.99 / multiplier) - 0.01))


def max_loss_stop_price(pos: Dict[str, Any]) -> float:
    if STOP_LOSS_PCT > 0:
        return max(0.01, float(pos["entry_price"]) * (1.0 - min(STOP_LOSS_PCT, 0.99)))
    shares = max(1e-9, position_shares(pos))
    stop = float(pos["entry_price"]) - (MAX_LOSS_USD_PER_TRADE / shares)
    return max(0.01, stop)


def runner_trailing_stop_price(pos: Dict[str, Any]) -> float:
    high = max(float(pos.get("high_price") or pos["entry_price"]), float(pos["entry_price"]))
    return max(0.01, high * (1.0 - RUNNER_TRAILING_STOP_PCT))


def trend_is_confirmed(
    outcome: str,
    symbol: str,
    *,
    min_momentum_pct: Optional[float] = None,
    short_tolerance_pct: Optional[float] = None,
) -> bool:
    # Yon sinyali tek basina yetmez.
    # Bu fonksiyon momentum o yonu destekliyor mu diye ikinci bir kontrol yapar.
    momentum = get_binance_momentum(symbol)
    short_momentum = get_binance_momentum_short(symbol)
    if momentum is None or short_momentum is None:
        return False
    threshold = MOMENTUM_MIN_PCT if min_momentum_pct is None else float(min_momentum_pct)
    tolerance = SHORT_MOMENTUM_TOLERANCE_PCT if short_tolerance_pct is None else max(0.0, float(short_tolerance_pct))
    if outcome == "Up":
        return momentum >= threshold and short_momentum >= -tolerance
    return momentum <= -threshold and short_momentum <= tolerance


def get_entry_signal(symbol: str) -> Tuple[Optional[str], str]:
    # Kullanici istegiyle momentum filtresi kapatildi.
    # Artik trade yonu sadece son 2 kapanmis 1m mumun ayni yone bakmasina gore belirlenir.
    strict_signal = get_two_candle_direction(symbol)
    if strict_signal:
        return strict_signal, "two_candle"
    return None, "no_signal"


def stop_reason_label() -> str:
    amount = f"{MAX_LOSS_USD_PER_TRADE:.2f}".rstrip("0").rstrip(".")
    return f"SL_MAX_{amount}USD"


def stop_pct_reason_label() -> str:
    pct = int(round(max(0.0, STOP_LOSS_PCT) * 100))
    return f"SL_{pct}PCT"


def take_profit_reason_label() -> str:
    pct = int(round(max(0.0, TAKE_PROFIT_PCT) * 100))
    return f"TP_{pct}PCT"


def summarize_closed_trades(
    conn: sqlite3.Connection,
    *,
    trading_mode: str = "paper",
    lookback: Optional[int] = None,
) -> Dict[str, Any]:
    # Son kapanan trade'lerden performans ozeti cikarilir.
    # Health gate ve live evidence kararlarinin cogu bu ozetle verilir.
    rows = conn.execute(
        "SELECT pnl_usd, COALESCE(close_reason, ''), COALESCE(slot_ts, 0), COALESCE(closed_ts, 0) "
        "FROM paper_positions "
        "WHERE status='CLOSED' AND COALESCE(trading_mode, 'paper')=? "
        "ORDER BY COALESCE(closed_ts, 0) DESC, id DESC",
        (trading_mode,),
    ).fetchall()
    if lookback is not None:
        rows = rows[: max(0, int(lookback))]

    closed_count = len(rows)
    net_pnl_usd = float(sum(float(row[0] or 0.0) for row in rows))
    gross_win = float(sum(max(float(row[0] or 0.0), 0.0) for row in rows))
    gross_loss = float(sum(abs(min(float(row[0] or 0.0), 0.0)) for row in rows))
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
    win_rate = (sum(1 for row in rows if float(row[0] or 0.0) > 0) / closed_count) if closed_count else 0.0
    force_exit_count = sum(1 for row in rows if str(row[1]).startswith("FORCE_EXIT"))
    stop_count = sum(
        1
        for row in rows
        if str(row[1]).startswith("SL_MAX_") or str(row[1]).startswith("SL_")
    )
    force_exit_rate = (force_exit_count / closed_count) if closed_count else 0.0
    stop_rate = (stop_count / closed_count) if closed_count else 0.0
    consecutive_losses = 0
    for pnl_usd, _, _, _ in rows:
        if float(pnl_usd or 0.0) < 0:
            consecutive_losses += 1
        else:
            break
    latest_slot_ts = max((int(row[2] or 0) for row in rows), default=0)
    return {
        "closed_count": closed_count,
        "net_pnl_usd": net_pnl_usd,
        "gross_win_usd": gross_win,
        "gross_loss_usd": gross_loss,
        "profit_factor": profit_factor,
        "win_rate": win_rate,
        "force_exit_rate": force_exit_rate,
        "stop_rate": stop_rate,
        "consecutive_losses": consecutive_losses,
        "latest_slot_ts": latest_slot_ts,
    }


def latest_paper_evaluation_summary() -> Optional[Dict[str, Any]]:
    runs_dir = Path(__file__).resolve().parent / "runs"
    if not runs_dir.exists():
        return None

    run_paths = sorted(
        [p for p in runs_dir.iterdir() if p.is_dir() and p.name.startswith("Run_")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for run_path in run_paths:
        db_path = run_path / "paper_trades.db"
        if not db_path.exists():
            continue
        conn = sqlite3.connect(str(db_path))
        try:
            summary = summarize_closed_trades(conn, trading_mode="paper")
            if summary["closed_count"] <= 0:
                continue
            recent = summarize_closed_trades(conn, trading_mode="paper", lookback=LIVE_EVAL_LOOKBACK_TRADES)
            summary.update(
                {
                    "run_path": str(run_path),
                    "recent_pnl_usd": recent["net_pnl_usd"],
                    "recent_count": recent["closed_count"],
                }
            )
            return summary
        finally:
            conn.close()
    return None


def paper_evidence_is_ready(summary: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not summary:
        return False, "no_completed_paper_run"
    if summary["closed_count"] < LIVE_MIN_CLOSED_TRADES:
        return False, f"closed_count={summary['closed_count']}<{LIVE_MIN_CLOSED_TRADES}"
    if summary["net_pnl_usd"] < LIVE_MIN_NET_PNL_USD:
        return False, f"net_pnl=${summary['net_pnl_usd']:.2f}<${LIVE_MIN_NET_PNL_USD:.2f}"
    if summary["profit_factor"] < LIVE_MIN_PROFIT_FACTOR:
        return False, f"profit_factor={summary['profit_factor']:.2f}<{LIVE_MIN_PROFIT_FACTOR:.2f}"
    if summary["force_exit_rate"] > LIVE_MAX_FORCE_EXIT_RATE:
        return False, f"force_exit_rate={summary['force_exit_rate']:.1%}>{LIVE_MAX_FORCE_EXIT_RATE:.1%}"
    if summary["stop_rate"] > LIVE_MAX_STOP_RATE:
        return False, f"stop_rate={summary['stop_rate']:.1%}>{LIVE_MAX_STOP_RATE:.1%}"
    if summary.get("recent_count", 0) > 0 and summary.get("recent_pnl_usd", 0.0) < LIVE_MIN_RECENT_PNL_USD:
        return False, f"recent_pnl=${summary['recent_pnl_usd']:.2f}<${LIVE_MIN_RECENT_PNL_USD:.2f}"
    return True, "ok"


def quote_is_stable(
    slot_ts: int,
    market_slug: str,
    outcome: str,
    preview_entry: float,
    spread: float,
    *,
    required_passes: Optional[int] = None,
) -> bool:
    # Anlik fiyatlar bir ileri bir geri zipliyorsa hemen trade acmak istemiyoruz.
    # Bu state ayni quote'un kac tur ust uste kabul edilebilir oldugunu sayar.
    key = f"{slot_ts}:{market_slug}:{outcome}"
    current_price = max(0.01, float(preview_entry))
    current_spread = max(0.0, float(spread))
    stale_keys = [k for k in _quote_stability_state.keys() if not k.startswith(f"{slot_ts}:")]
    for stale_key in stale_keys:
        _quote_stability_state.pop(stale_key, None)

    prev = _quote_stability_state.get(key)
    if prev:
        jump_pct = abs(current_price - prev["price"]) / max(0.01, prev["price"])
        spread_jump = abs(current_spread - prev["spread"])
        pass_count = (int(prev["pass_count"]) + 1) if jump_pct <= MAX_QUOTE_JUMP_PCT and spread_jump <= MAX_ENTRY_SPREAD else 1
    else:
        pass_count = 1
    _quote_stability_state[key] = {"price": current_price, "spread": current_spread, "pass_count": pass_count}
    required = MIN_QUOTE_STABLE_PASSES if required_passes is None else int(required_passes)
    return pass_count >= max(1, required)


def cooldown_until_slot(conn: sqlite3.Connection) -> int:
    # Arka arkaya zarar gelirse bot birkac slot boyunca kendine mola verir.
    limit = max(CONSECUTIVE_LOSS_COOLDOWN_TRIGGER, 4)
    rows = conn.execute(
        "SELECT COALESCE(slot_ts, 0), COALESCE(pnl_usd, 0), COALESCE(close_reason, '') "
        "FROM paper_positions WHERE status='CLOSED' "
        "ORDER BY COALESCE(closed_ts, 0) DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        return 0
    consecutive_losses = 0
    for _, pnl_usd, _ in rows:
        if float(pnl_usd or 0.0) < 0:
            consecutive_losses += 1
        else:
            break
    recent_stop_count = sum(
        1
        for _, _, reason in rows[:4]
        if str(reason).startswith("SL_MAX_") or str(reason).startswith("SL_")
    )
    if consecutive_losses < CONSECUTIVE_LOSS_COOLDOWN_TRIGGER and recent_stop_count < 2:
        return 0
    anchor_slot = max(int(row[0] or 0) for row in rows[: max(CONSECUTIVE_LOSS_COOLDOWN_TRIGGER, 4)])
    if anchor_slot <= 0:
        return 0
    return anchor_slot + (LOSS_COOLDOWN_SLOTS * 300)


def runtime_strategy_health(conn: sqlite3.Connection) -> Tuple[bool, str]:
    # Son performans cok bozulduysa yeni entry acmak gecici olarak durdurulur.
    full = summarize_closed_trades(conn, trading_mode="paper")
    if full["closed_count"] < HEALTH_MIN_CLOSED_TRADES:
        return True, f"warming_up_{full['closed_count']}"
    recent = summarize_closed_trades(conn, trading_mode="paper", lookback=HEALTH_LOOKBACK_TRADES)
    if recent["profit_factor"] < HEALTH_MIN_PROFIT_FACTOR:
        return False, f"profit_factor={recent['profit_factor']:.2f}"
    if recent["force_exit_rate"] > HEALTH_MAX_FORCE_EXIT_RATE:
        return False, f"force_exit_rate={recent['force_exit_rate']:.1%}"
    if recent["stop_rate"] > HEALTH_MAX_STOP_RATE:
        return False, f"stop_rate={recent['stop_rate']:.1%}"
    if recent["net_pnl_usd"] < HEALTH_MIN_RECENT_PNL_USD:
        return False, f"recent_pnl=${recent['net_pnl_usd']:.2f}"
    return True, "ok"


def to_position(row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "coin": row[1],
        "market_id": row[2],
        "market_slug": row[3],
        "question": row[4],
        "slot_ts": row[5],
        "outcome": row[6],
        "token_id": row[7],
        "entry_price": row[8],
        "size_usd": row[9],
        "original_size_usd": row[10],
        "realized_pnl_usd": row[11],
        "principal_recovered": row[12],
        "high_price": row[13],
    }


def append_trade_event_csv(
    now_ts: int,
    event: str,
    market: dict,
    outcome: str,
    token_id: str,
    price: float,
    size_usd: float,
    pnl_usd: Optional[float],
    reason: str,
    meta: Optional[dict],
) -> None:
    meta = meta or {}
    append_csv_row(
        TRADE_EVENTS_CSV_PATH,
        TRADE_EVENT_HEADERS,
        {
            "recorded_at_utc": _csv_iso(now_ts),
            "run_id": os.path.basename(RUN_DIR),
            "trading_mode": meta.get("trading_mode", TRADING_MODE),
            "event": event,
            "market_id": market_identifier(market),
            "market_slug": market.get("slug", ""),
            "question": market_question(market),
            "outcome": outcome,
            "token_id": token_id,
            "price": price,
            "size_usd": size_usd,
            "pnl_usd": pnl_usd,
            "reason": reason,
            "coin": meta.get("coin", ""),
            "signal_mode": meta.get("signal_mode", ""),
            "requested_entry_price": meta.get("requested_entry_price", ""),
            "entry_price": meta.get("entry_price", ""),
            "principal_take_price": meta.get("principal_take_price", ""),
            "runner_target": meta.get("runner_target", ""),
            "pnl_pct": meta.get("pnl_pct", ""),
            "order_id": meta.get("order_id", ""),
            "shares_sold": meta.get("shares_sold", ""),
            "remaining_shares": meta.get("remaining_shares", ""),
            "remaining_cost_basis": meta.get("remaining_cost_basis", ""),
        },
    )


def append_closed_trade_csv(
    conn: sqlite3.Connection,
    pos: Dict[str, Any],
    market: dict,
    now_ts: int,
    exit_price: float,
    pnl_usd: float,
    pnl_pct: float,
    reason: str,
    close_order_id: str = "",
) -> None:
    row = conn.execute(
        "SELECT COALESCE(opened_ts, 0), COALESCE(order_id, ''), COALESCE(trading_mode, ?) "
        "FROM paper_positions WHERE id=?",
        (TRADING_MODE, pos["id"]),
    ).fetchone()
    opened_ts = int(row[0]) if row and row[0] else 0
    entry_order_id = str(row[1]) if row else ""
    trading_mode = str(row[2]) if row else TRADING_MODE
    append_csv_row(
        CLOSED_TRADES_CSV_PATH,
        CLOSED_TRADE_HEADERS,
        {
            "closed_at_utc": _csv_iso(now_ts),
            "run_id": os.path.basename(RUN_DIR),
            "trading_mode": trading_mode,
            "coin": pos.get("coin", ""),
            "market_id": market_identifier(market),
            "market_slug": market.get("slug", ""),
            "question": market_question(market),
            "outcome": pos.get("outcome", ""),
            "opened_at_utc": _csv_iso(opened_ts),
            "closed_ts": now_ts,
            "entry_price": pos.get("entry_price", ""),
            "exit_price": exit_price,
            "position_size_usd_at_close": pos.get("size_usd", ""),
            "original_size_usd": pos.get("original_size_usd", ""),
            "realized_before_close": pos.get("realized_pnl_usd", ""),
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "close_reason": reason,
            "principal_recovered": pos.get("principal_recovered", ""),
            "entry_order_id": entry_order_id,
            "close_order_id": close_order_id,
        },
    )


def mark_position_pending_settlement(
    conn: sqlite3.Connection,
    pos: Dict[str, Any],
    market: dict,
    now_ts: int,
    reason: str,
) -> None:
    # Market expiry sonrasi orderbook yoksa pozisyon hemen satilamaz.
    # Bu state, botun sonsuz close retry loop'una girmesini engeller.
    realized_before = float(pos.get("realized_pnl_usd") or 0.0)
    conn.execute(
        "UPDATE paper_positions SET status='PENDING_SETTLEMENT', close_reason=?, trading_mode=? WHERE id=?",
        (reason, TRADING_MODE, pos["id"]),
    )
    insert_signal_event(
        conn=conn,
        now_ts=now_ts,
        event="PENDING_SETTLEMENT",
        market=market,
        outcome=pos["outcome"],
        token_id=pos["token_id"] or "",
        price=float(pos["entry_price"]),
        size_usd=float(pos["size_usd"]),
        pnl_usd=realized_before,
        reason=reason,
        meta={
            "entry_price": pos["entry_price"],
            "remaining_shares": position_shares(pos),
            "trading_mode": TRADING_MODE,
            "realized_before_pending": realized_before,
        },
    )
    conn.commit()
    log(
        f"SETTLEMENT PENDING {pos['coin']} {pos['outcome']} | "
        f"{reason} | realized={realized_before:+.2f}$"
    )


def insert_signal_event(
    conn: sqlite3.Connection,
    now_ts: int,
    event: str,
    market: dict,
    outcome: str,
    token_id: str,
    price: float,
    size_usd: float,
    pnl_usd: Optional[float] = None,
    reason: str = "",
    meta: Optional[dict] = None,
) -> None:
    conn.execute(
        "INSERT INTO signal_journal (ts, event, market_id, market_slug, token_id, outcome, price, size_usd, pnl_usd, reason, meta_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            now_ts,
            event,
            market_identifier(market),
            market.get("slug"),
            token_id,
            outcome,
            price,
            size_usd,
            pnl_usd,
            reason,
            json.dumps(meta or {}, ensure_ascii=False),
        ),
    )
    append_trade_event_csv(
        now_ts=now_ts,
        event=event,
        market=market,
        outcome=outcome,
        token_id=token_id,
        price=price,
        size_usd=size_usd,
        pnl_usd=pnl_usd,
        reason=reason,
        meta=meta,
    )


def paper_open_position(
    conn: sqlite3.Connection,
    market: dict,
    coin: str,
    slot_ts: int,
    outcome: str,
    token_id: str,
    entry_price: float,
    now_ts: int,
) -> float:
    # Paper modda gercek emir yok; pozisyon sadece kayit altina alinir.
    conn.execute(
        "INSERT INTO paper_positions "
        "(coin, market_id, market_slug, question, slot_ts, outcome, token_id, entry_price, size_usd, original_size_usd, realized_pnl_usd, principal_recovered, opened_ts, status, high_price, trading_mode) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 'OPEN', ?, 'paper')",
        (
            coin,
            market_identifier(market),
            market.get("slug"),
            market_question(market),
            slot_ts,
            outcome,
            token_id,
            entry_price,
            POSITION_SIZE_USD,
            POSITION_SIZE_USD,
            now_ts,
            entry_price,
        ),
    )
    insert_signal_event(
        conn=conn,
        now_ts=now_ts,
        event="OPEN",
        market=market,
        outcome=outcome,
        token_id=token_id,
        price=entry_price,
        size_usd=POSITION_SIZE_USD,
        meta={
            "coin": coin,
            "two_candle_direction": outcome,
            "principal_take_price": principal_take_price({"entry_price": entry_price, "size_usd": POSITION_SIZE_USD}),
            "runner_target": RUNNER_FINAL_TARGET_PRICE,
            "trading_mode": "paper",
        },
    )
    conn.commit()
    return entry_price


def open_position(
    conn: sqlite3.Connection,
    market: dict,
    coin: str,
    slot_ts: int,
    outcome: str,
    mid_price: float,
    spread: float,
    liquidity: float,
    now_ts: int,
    signal_mode: str = "two_candle",
) -> bool:
    # Trade acma icin tek fonksiyon.
    # Moda gore ya simulasyon yapar ya da gercek order yollar.
    token_id = select_token_id(market, outcome)
    if not token_id:
        return False

    if TRADING_MODE == "paper" or _engine is None:
        adjusted_entry = (
            simulate_execution_price(mid_price, spread, POSITION_SIZE_USD, liquidity, "buy")
            if SIMULATED_SPREAD_ENABLED
            else mid_price * (1 + SIMULATED_FEE_PCT)
        )
        final_entry = paper_open_position(conn, market, coin, slot_ts, outcome, token_id, adjusted_entry, now_ts)
        stop_price = max_loss_stop_price({"entry_price": final_entry, "size_usd": POSITION_SIZE_USD})
        take_profit = fixed_take_profit_price({"entry_price": final_entry, "size_usd": POSITION_SIZE_USD})
        log(
            f"OPEN {coin} {outcome} @ {final_entry:.3f} "
            f"({TRADING_MODE.upper()} TP:{take_profit:.2f} "
            f"SL:{stop_price:.2f} SIG:{signal_mode})"
        )
        notify_trade_open(coin, outcome, final_entry, POSITION_SIZE_USD, signal_mode)
        return True

    result = _engine.execute_open(
        conn=conn,
        market_id=market_identifier(market),
        market_slug=market.get("slug", ""),
        question=market_question(market),
        outcome=outcome,
        token_id=token_id,
        entry_price=mid_price,
        size_usd=POSITION_SIZE_USD,
        now_ts=now_ts,
        worst_price=live_buy_worst_price(mid_price, spread),
        order_tick_size=market_tick_size(market),
        extra_cols={"coin": coin, "slot_ts": slot_ts, "original_size_usd": POSITION_SIZE_USD, "realized_pnl_usd": 0.0, "principal_recovered": 0},
    )
    if not result["success"]:
        log(f"OPEN rejected {coin} {outcome}: {result['error']}")
        return False

    executed_entry = float(result.get("fill_price") or mid_price)

    insert_signal_event(
        conn=conn,
        now_ts=now_ts,
        event="OPEN",
        market=market,
        outcome=outcome,
        token_id=token_id,
        price=executed_entry,
        size_usd=float(result.get("actual_size_usd") or POSITION_SIZE_USD),
        meta={
            "coin": coin,
            "two_candle_direction": outcome,
            "signal_mode": signal_mode,
            "principal_take_price": min(0.99, executed_entry * PRINCIPAL_TAKE_MULTIPLIER),
            "runner_target": RUNNER_FINAL_TARGET_PRICE,
            "trading_mode": TRADING_MODE,
            "order_id": result.get("order_id", ""),
            "requested_entry_price": mid_price,
            "matched_shares": result.get("matched_shares"),
        },
    )
    conn.commit()
    opened_size_usd = float(result.get("actual_size_usd") or POSITION_SIZE_USD)
    stop_price = max_loss_stop_price({"entry_price": executed_entry, "size_usd": opened_size_usd})
    take_profit = fixed_take_profit_price({"entry_price": executed_entry, "size_usd": opened_size_usd})
    log(
        f"OPEN {coin} {outcome} @ {executed_entry:.3f} "
        f"({TRADING_MODE.upper()} TP:{take_profit:.2f} "
        f"SL:{stop_price:.2f} SIG:{signal_mode})"
    )
    notify_trade_open(coin, outcome, executed_entry, opened_size_usd, signal_mode)
    return True


def take_principal_off(
    conn: sqlite3.Connection,
    pos: Dict[str, Any],
    market: dict,
    exit_price: float,
    now_ts: int,
    spread: float,
    liquidity: float,
) -> bool:
    # Kar belirli seviyeye geldiyse pozisyonun bir kismini kapatir
    # ve ana parayi geri almayi hedefler.
    current_shares = position_shares(pos)
    if current_shares <= 0:
        return False
    target_exit = max(0.01, float(exit_price))
    shares_to_sell = min(current_shares * 0.5, float(pos["original_size_usd"] or POSITION_SIZE_USD) / target_exit)
    if shares_to_sell <= 0 or shares_to_sell >= current_shares:
        return False
    min_order_size = market_min_order_size(market)
    remaining_shares_preview = current_shares - shares_to_sell
    if min_order_size is not None and (
        shares_to_sell < min_order_size or remaining_shares_preview < min_order_size
    ):
        close_position(conn, pos, market, target_exit, now_ts, "TP_FULL_2X_MINSIZE", spread, liquidity)
        row = conn.execute("SELECT status FROM paper_positions WHERE id=?", (pos["id"],)).fetchone()
        return bool(row and row[0] == "CLOSED")

    if TRADING_MODE == "paper" or _engine is None:
        fill_price = (
            simulate_execution_price(target_exit, spread, shares_to_sell * float(pos["entry_price"]), liquidity, "sell", EXIT_SLIPPAGE_MULTIPLIER)
            if SIMULATED_SPREAD_ENABLED
            else target_exit * (1 - SIMULATED_FEE_PCT)
        )
        order_id = ""
    elif TRADING_MODE == "live":
        if _engine.clob_client is None:
            log(f"REDUCE failed {pos['coin']} {pos['outcome']}: CLOB client missing")
            return False
        order_resp = _engine.clob_client.market_sell(
            token_id=pos["token_id"],
            shares=shares_to_sell,
            worst_price=live_sell_worst_price(target_exit, spread),
            tick_size=market_tick_size(market),
        )
        if not order_resp.success:
            log(f"REDUCE failed {pos['coin']} {pos['outcome']}: {order_resp.error}")
            return False
        verified, verify_reason = _engine.verify_live_order(order_resp)
        if not verified:
            log(f"REDUCE failed {pos['coin']} {pos['outcome']}: verify={verify_reason}")
            if _engine.safety_manager is not None:
                _engine.safety_manager.activate_kill_switch(f"5MIN live reduce verify failed: {verify_reason}")
            return False
        fill_price = _engine.extract_live_fill_price(order_resp, "sell", target_exit)
        actual_sold_shares = float(_engine.extract_live_matched_shares(order_resp) or shares_to_sell)
        actual_remaining_shares = None
        if hasattr(_engine.clob_client, "get_conditional_token_balance"):
            actual_remaining_shares = _engine.clob_client.get_conditional_token_balance(pos["token_id"])
        order_id = order_resp.order_id
    else:
        fill_price = target_exit
        order_id = f"DRY-RUN-REDUCE-{int(time.time())}"
        actual_sold_shares = shares_to_sell
        actual_remaining_shares = None

    realized_pnl = (fill_price - float(pos["entry_price"])) * actual_sold_shares
    remaining_shares = actual_remaining_shares if actual_remaining_shares is not None else max(0.0, current_shares - actual_sold_shares)
    remaining_cost = remaining_shares * float(pos["entry_price"])
    new_realized = float(pos.get("realized_pnl_usd") or 0.0) + realized_pnl
    new_high = max(float(pos.get("high_price") or pos["entry_price"]), fill_price)
    conn.execute(
        "UPDATE paper_positions SET size_usd=?, realized_pnl_usd=?, principal_recovered=1, high_price=?, trading_mode=? WHERE id=?",
        (remaining_cost, new_realized, new_high, TRADING_MODE if TRADING_MODE != "paper" else "paper", pos["id"]),
    )
    insert_signal_event(
        conn=conn,
        now_ts=now_ts,
        event="REDUCE",
        market=market,
        outcome=pos["outcome"],
        token_id=pos["token_id"],
        price=fill_price,
        size_usd=float(pos["original_size_usd"] or POSITION_SIZE_USD),
        pnl_usd=realized_pnl,
        reason="TAKE_PRINCIPAL_2X",
        meta={
            "shares_sold": actual_sold_shares,
            "remaining_shares": remaining_shares,
            "remaining_cost_basis": remaining_cost,
            "order_id": order_id,
            "trading_mode": TRADING_MODE,
        },
    )
    conn.commit()
    if TRADING_MODE == "live" and _engine is not None and _engine.safety_manager is not None:
        _engine.safety_manager.record_trade_pnl(BOT_KEY, realized_pnl)
    log(f"REDUCE {pos['coin']} {pos['outcome']} @ {fill_price:.3f} (Realized: {realized_pnl:+.2f}$ TAKE_PRINCIPAL_2X)")
    return True


def close_position(
    conn: sqlite3.Connection,
    pos: Dict[str, Any],
    market: dict,
    exit_price: float,
    now_ts: int,
    reason: str,
    spread: float,
    liquidity: float,
) -> Tuple[float, float, float]:
    # Tam kapanis burada yapilir.
    # PnL hesaplama, DB update ve CSV export ayni yerde tutulur.
    remaining_shares = position_shares(pos)
    realized_before = float(pos.get("realized_pnl_usd") or 0.0)
    original_size = max(1e-9, float(pos.get("original_size_usd") or pos["size_usd"]))
    if TRADING_MODE == "paper" or _engine is None:
        adjusted_exit = (
            simulate_execution_price(exit_price, spread, pos["size_usd"], liquidity, "sell", EXIT_SLIPPAGE_MULTIPLIER)
            if SIMULATED_SPREAD_ENABLED
            else exit_price * (1 - SIMULATED_FEE_PCT)
        )
        remaining_pnl = (float(adjusted_exit) - float(pos["entry_price"])) * remaining_shares
        pnl_usd = realized_before + remaining_pnl
        pnl_pct = pnl_usd / original_size
        conn.execute(
            "UPDATE paper_positions SET status='CLOSED', exit_price=?, pnl_usd=?, pnl_pct=?, closed_ts=?, close_reason=?, trading_mode='paper', fill_price=?, high_price=? WHERE id=?",
            (adjusted_exit, pnl_usd, pnl_pct, now_ts, reason, adjusted_exit, max(float(pos.get('high_price') or pos['entry_price']), adjusted_exit), pos["id"]),
        )
        insert_signal_event(
            conn=conn,
            now_ts=now_ts,
            event="CLOSE",
            market=market,
            outcome=pos["outcome"],
            token_id=pos["token_id"],
            price=adjusted_exit,
            size_usd=pos["size_usd"],
            pnl_usd=pnl_usd,
            reason=reason,
            meta={"entry_price": pos["entry_price"], "pnl_pct": pnl_pct, "realized_before_close": realized_before, "trading_mode": "paper"},
        )
        append_closed_trade_csv(
            conn=conn,
            pos=pos,
            market=market,
            now_ts=now_ts,
            exit_price=adjusted_exit,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            reason=reason,
        )
        conn.commit()
        log(f"CLOSE {pos['coin']} {pos['outcome']} @ {adjusted_exit:.3f} (PnL: {pnl_usd:+.2f}$ {reason})")
        notify_trade_close(
            conn,
            coin=pos["coin"],
            outcome=pos["outcome"],
            entry_price=float(pos["entry_price"]),
            exit_price=adjusted_exit,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            size_usd=float(pos["size_usd"]),
            reason=reason,
        )
        return pnl_usd, pnl_pct, adjusted_exit

    result = _engine.execute_close(
        conn=conn,
        pos_id=pos["id"],
        token_id=pos["token_id"],
        entry_price=pos["entry_price"],
        size_usd=pos["size_usd"],
        exit_price=exit_price,
        now_ts=now_ts,
        reason=reason,
        worst_price=live_sell_worst_price(exit_price, spread),
        order_tick_size=market_tick_size(market),
        market_slug=pos["market_slug"],
        question=pos["question"],
        outcome=pos["outcome"],
    )
    if not result["success"]:
        log(f"CLOSE failed {pos['coin']} {pos['outcome']}: {result['error']}")
        return 0.0, 0.0, exit_price

    fill_price = float(result.get("fill_price") or exit_price)
    sold_shares = float(result.get("sold_shares") or remaining_shares)
    remaining_pnl = (fill_price - float(pos["entry_price"])) * sold_shares
    pnl_usd = realized_before + remaining_pnl
    pnl_pct = pnl_usd / original_size
    conn.execute(
        "UPDATE paper_positions SET pnl_usd=?, pnl_pct=?, high_price=? WHERE id=?",
        (pnl_usd, pnl_pct, max(float(pos.get('high_price') or pos['entry_price']), fill_price), pos["id"]),
    )
    insert_signal_event(
        conn=conn,
        now_ts=now_ts,
        event="CLOSE",
        market=market,
        outcome=pos["outcome"],
        token_id=pos["token_id"],
        price=fill_price,
        size_usd=pos["size_usd"],
        pnl_usd=pnl_usd,
        reason=reason,
        meta={"entry_price": pos["entry_price"], "pnl_pct": pnl_pct, "realized_before_close": realized_before, "trading_mode": TRADING_MODE, "order_id": result.get("order_id", "")},
    )
    append_closed_trade_csv(
        conn=conn,
        pos=pos,
        market=market,
        now_ts=now_ts,
        exit_price=fill_price,
        pnl_usd=pnl_usd,
        pnl_pct=pnl_pct,
        reason=reason,
        close_order_id=str(result.get("order_id", "")),
    )
    conn.commit()
    log(f"CLOSE {pos['coin']} {pos['outcome']} @ {fill_price:.3f} (PnL: {pnl_usd:+.2f}$ {reason})")
    notify_trade_close(
        conn,
        coin=pos["coin"],
        outcome=pos["outcome"],
        entry_price=float(pos["entry_price"]),
        exit_price=fill_price,
        pnl_usd=pnl_usd,
        pnl_pct=pnl_pct,
        size_usd=float(pos["size_usd"]),
        reason=reason,
    )
    return pnl_usd, pnl_pct, fill_price


# Bu fonksiyon botun tek dongudeki tum isini yapar.
# Sira soyle:
# 1) snapshot oku
# 2) acik pozisyonlarin cikislarini kontrol et
# 3) slot bitenleri expiry ile kapat
# 4) yeni entry ara
# 5) equity ve heartbeat yaz
def scan_once(conn):
    global _no_snapshot_count, _last_no_snapshot_log_ts, _last_entry_guard_log_ts
    now = int(time.time())
    current_slot = (now // 300) * 300
    sec_in = now - current_slot
    try:
        os.utime(LOG_PATH, None)
    except Exception:
        pass
    raw_snap = read_snapshot()
    snap = normalize_snapshot(raw_snap)
    if not snap or snap.get("source") not in {"clob_book", "clob_price_mid"} or not snap.get("book_valid"):
        _no_snapshot_count += 1
        if (now - _last_no_snapshot_log_ts) >= 60:
            print(f"[{BOT_KEY}] No valid CLOB snapshot available (count={_no_snapshot_count})", flush=True)
            _last_no_snapshot_log_ts = now
        rows = conn.execute(
            "SELECT id, coin, market_id, market_slug, question, slot_ts, outcome, token_id, entry_price, size_usd, original_size_usd, realized_pnl_usd, principal_recovered, high_price "
            "FROM paper_positions WHERE status='OPEN'"
        ).fetchall()
        for row in rows:
            pos = to_position(row)
            if now < pos["slot_ts"] + 300:
                continue
            fallback_market = {
                "market_id": pos["market_id"] or pos["market_slug"],
                "slug": pos["market_slug"],
                "question": pos["question"] or pos["market_slug"],
            }
            if TRADING_MODE != "paper" and _engine is not None:
                orderbook_exists = live_token_has_orderbook(pos["token_id"] or "")
                if orderbook_exists is False:
                    mark_position_pending_settlement(conn, pos, fallback_market, now, "NOSNAP_SETTLEMENT_PENDING")
                    continue
                log(f"LIVE NOSNAP HOLD {pos['coin']} {pos['outcome']} | snapshot unavailable, skip synthetic close")
                continue
            realized_before = float(pos.get("realized_pnl_usd") or 0.0)
            original_size = max(1e-9, float(pos.get("original_size_usd") or pos["size_usd"]))
            pnl_pct = realized_before / original_size
            conn.execute(
                "UPDATE paper_positions SET status='CLOSED', exit_price=?, pnl_usd=?, pnl_pct=?, closed_ts=?, close_reason='NOSNAP', fill_price=?, high_price=? WHERE id=?",
                (
                    pos["entry_price"],
                    realized_before,
                    pnl_pct,
                    now,
                    pos["entry_price"],
                    max(float(pos.get("high_price") or pos["entry_price"]), float(pos["entry_price"])),
                    pos["id"],
                ),
            )
            insert_signal_event(
                conn=conn,
                now_ts=now,
                event="CLOSE",
                market=fallback_market,
                outcome=pos["outcome"],
                token_id=pos["token_id"] or "",
                price=float(pos["entry_price"]),
                size_usd=float(pos["size_usd"]),
                pnl_usd=realized_before,
                reason="NOSNAP",
                meta={"entry_price": pos["entry_price"], "pnl_pct": pnl_pct, "trading_mode": "paper", "realized_before_close": realized_before},
            )
            append_closed_trade_csv(
                conn=conn,
                pos=pos,
                market=fallback_market,
                now_ts=now,
                exit_price=float(pos["entry_price"]),
                pnl_usd=realized_before,
                pnl_pct=pnl_pct,
                reason="NOSNAP",
            )
            conn.commit()
            log(f"CLOSE {pos['coin']} {pos['outcome']} @ {pos['entry_price']:.3f} (PnL: {realized_before:+.2f}$ NOSNAP)")
            notify_trade_close(
                conn,
                coin=pos["coin"],
                outcome=pos["outcome"],
                entry_price=float(pos["entry_price"]),
                exit_price=float(pos["entry_price"]),
                pnl_usd=realized_before,
                pnl_pct=pnl_pct,
                size_usd=float(pos["size_usd"]),
                reason="NOSNAP",
            )
        return

    _no_snapshot_count = 0
    markets = snap.get("markets", [])
    mids = snap.get("mids", {})
    spreads_snap = snap.get("spreads", {})
    market_by_slug = {m.get("slug"): m for m in markets if m.get("slug")}

    # Her token icin spread ve liquidity bilgisi cikariliyor.
    # Paper simulasyonlarda ve cikis kararlarinda bunlar kullaniliyor.
    token_spread: Dict[str, float] = {}
    token_liquidity: Dict[str, float] = {}
    for m in markets:
        liq = float(m.get("liquidity") or 5000)
        for tid_key in ("yes_token_id", "no_token_id"):
            tid = m.get(tid_key, "")
            if tid:
                token_spread[tid] = float(spreads_snap.get(tid, 0.04))
                token_liquidity[tid] = liq

    # 0. Acik trade'lerin cikis tarafi:
    # hard stop -> principal take -> runner
    # Not: son 15 saniye basladiginda artik partial reduce birakmiyoruz;
    # elde ne kaldiysa full close oncelikli oluyor.
    mid_rows = conn.execute(
        "SELECT id, coin, market_id, market_slug, question, slot_ts, outcome, token_id, entry_price, size_usd, original_size_usd, realized_pnl_usd, principal_recovered, high_price "
        "FROM paper_positions WHERE status='OPEN'"
    ).fetchall()
    for row in mid_rows:
        pos = to_position(row)
        if now >= pos["slot_ts"] + 300:
            continue
        market = market_by_slug.get(pos["market_slug"])
        if not market:
            continue
        token_id = pos["token_id"] or select_token_id(market, pos["outcome"])
        if not token_id:
            continue
        if not pos["token_id"]:
            conn.execute(
                "UPDATE paper_positions SET token_id=?, market_id=COALESCE(market_id, ?), question=COALESCE(question, ?) WHERE id=?",
                (token_id, market_identifier(market), market_question(market), pos["id"]),
            )
            conn.commit()
            pos["token_id"] = token_id
        cur_price = mids.get(token_id)
        if cur_price is None:
            continue

        _spr = token_spread.get(token_id, 0.04)
        _liq = token_liquidity.get(token_id, 5000)
        cur_price = float(cur_price)
        new_high = max(float(pos.get("high_price") or pos["entry_price"]), cur_price)
        if new_high > float(pos.get("high_price") or 0):
            conn.execute("UPDATE paper_positions SET high_price=? WHERE id=?", (new_high, pos["id"]))
            conn.commit()
            pos["high_price"] = new_high

        stop_mid = max(0.01, cur_price)
        profit_mid = max(0.01, cur_price - TP_EXIT_BUFFER)
        force_mid = max(0.01, float(cur_price) - FORCE_EXIT_BUFFER)
        sec_left = max(0, 300 - sec_in)

        # Book anormal sekilde incelirse normal hedefi beklemek yerine daha erken cik.
        # Ama bunu her spread artisinda degil, ancak belirgin bozulmada kullan.
        if _spr >= THIN_BOOK_EXIT_CRITICAL_SPREAD:
            close_position(conn, pos, market, force_mid, now, "THIN_BOOK_EXIT", _spr, _liq)
            continue
        if sec_left <= THIN_BOOK_EXIT_SEC_LEFT and _spr >= THIN_BOOK_EXIT_SPREAD:
            close_position(conn, pos, market, force_mid, now, "THIN_BOOK_EXIT_LATE", _spr, _liq)
            continue

        # Slot kapanisina son 15 saniye kaldiysa artik "runner" birakmayiz.
        # Bu nokta, principal take'den de once gelir; boylece partial reduce
        # yapip expiry'ye runner tasima riski azalir.
        if sec_in >= FORCE_EXIT_SEC:
            close_position(conn, pos, market, force_mid, now, "FORCE_EXIT_15S", _spr, _liq)
            continue

        if cur_price <= max_loss_stop_price(pos):
            close_position(conn, pos, market, stop_mid, now, stop_pct_reason_label(), _spr, _liq)
            continue
        if cur_price >= fixed_take_profit_price(pos):
            close_position(conn, pos, market, profit_mid, now, take_profit_reason_label(), _spr, _liq)
            continue

    # 1. Slot bitti ise pozisyon daha fazla tutulmaz.
    rows = conn.execute(
        "SELECT id, coin, market_id, market_slug, question, slot_ts, outcome, token_id, entry_price, size_usd, original_size_usd, realized_pnl_usd, principal_recovered, high_price "
        "FROM paper_positions WHERE status='OPEN'"
    ).fetchall()
    for row in rows:
        pos = to_position(row)
        if now < pos["slot_ts"] + 300:
            continue
        market = market_by_slug.get(pos["market_slug"])
        if not market:
            market = {
                "market_id": pos["market_id"] or pos["market_slug"],
                "slug": pos["market_slug"],
                "question": pos["question"] or pos["market_slug"],
            }
        token_id = pos["token_id"] or select_token_id(market, pos["outcome"])
        if token_id and not pos["token_id"]:
            conn.execute(
                "UPDATE paper_positions SET token_id=?, market_id=COALESCE(market_id, ?), question=COALESCE(question, ?) WHERE id=?",
                (token_id, market_identifier(market), market_question(market), pos["id"]),
            )
            conn.commit()
            pos["token_id"] = token_id
        exit_price = mids.get(token_id, pos["entry_price"]) if token_id else pos["entry_price"]
        if exit_price >= 0.95:
            exit_price = 1.0
        elif exit_price <= 0.05:
            exit_price = 0.0
        spread = token_spread.get(token_id, 0.04) if token_id else 0.04
        liquidity = token_liquidity.get(token_id, 5000) if token_id else 5000
        if TRADING_MODE == "live" and token_id:
            orderbook_exists = live_token_has_orderbook(token_id)
            if orderbook_exists is False:
                mark_position_pending_settlement(conn, pos, market, now, "EXPIRY_SETTLEMENT_PENDING")
                continue
        close_position(conn, pos, market, float(exit_price), now, "EXPIRY", spread, liquidity)
        row = conn.execute("SELECT status FROM paper_positions WHERE id=?", (pos["id"],)).fetchone()
        if row and row[0] == "CLOSED":
            continue
        if TRADING_MODE == "live" and token_id and live_token_has_orderbook(token_id) is False:
            mark_position_pending_settlement(conn, pos, market, now, "EXPIRY_SETTLEMENT_PENDING")

    # 2. Yeni giris ara.
    # En cok "neden trade acmadi?" sorusunun cevabi genelde bu blokta bulunur.
    if ALLOW_NEW_ENTRIES and MIN_ENTRY_SEC <= sec_in < ENTRY_CUTOFF_SEC:
        cooldown_slot = cooldown_until_slot(conn)
        if cooldown_slot and current_slot < cooldown_slot:
            if (now - _last_entry_guard_log_ts) >= 60:
                remaining_slots = max(0, (cooldown_slot - current_slot) // 300)
                log(f"ENTRY PAUSED | loss cooldown active for {remaining_slots} slot(s)")
                _last_entry_guard_log_ts = now
            return

        health_ok, health_reason = runtime_strategy_health(conn)
        if not health_ok:
            if (now - _last_entry_guard_log_ts) >= 60:
                log(f"ENTRY PAUSED | runtime health gate | {health_reason}")
                _last_entry_guard_log_ts = now
            return

        open_count = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='OPEN'").fetchone()[0]
        slot_trades = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE slot_ts=?", (current_slot,)).fetchone()[0]
        if open_count < MAX_OPEN_POSITIONS and slot_trades < MAX_TRADES_PER_SLOT:
            signals = []
            open_slot_rows = conn.execute("SELECT coin, outcome, market_slug FROM paper_positions WHERE status='OPEN' AND slot_ts=?", (current_slot,)).fetchall()
            opened_slot_rows = conn.execute("SELECT outcome, market_slug FROM signal_journal WHERE event='OPEN' AND market_slug=?", (f"btc-updown-5m-{current_slot}",)).fetchall()
            existing_slot_keys = {(coin, outcome, market_slug) for coin, outcome, market_slug in open_slot_rows}
            seen_open_keys = {("btc", outcome, market_slug) for outcome, market_slug in opened_slot_rows}
            for coin in COINS:
                outcome, signal_mode = get_entry_signal(BINANCE_SYMBOLS[coin])
                if outcome is None:
                    continue

                slug = f"{coin}-updown-5m-{current_slot}"
                if (coin, outcome, slug) in existing_slot_keys or (coin, outcome, slug) in seen_open_keys:
                    continue
                market_ref = market_by_slug.get(slug)
                target_tid = select_token_id(market_ref, outcome) if market_ref else None
                if not target_tid or not market_ref:
                    continue

                mid_price = mids.get(target_tid)
                spread = float(spreads_snap.get(target_tid, 0.04))
                if mid_price is None or spread <= 0:
                    continue
                signal_mode = str(signal_mode or "two_candle")
                # Fallback sinyal ana sinyal kadar guclu olmadigi icin
                # bazi filtreler burada daha dikkatli uygulanir.
                allowed_spread = MAX_ENTRY_SPREAD if signal_mode == "two_candle" else min(MAX_ENTRY_SPREAD, FALLBACK_MAX_ENTRY_SPREAD)
                allowed_entry_max = ENTRY_MAX_PRICE if signal_mode == "two_candle" else min(ENTRY_MAX_PRICE, FALLBACK_ENTRY_MAX_PRICE)
                required_stability = MIN_QUOTE_STABLE_PASSES if signal_mode == "two_candle" else max(MIN_QUOTE_STABLE_PASSES, FALLBACK_MIN_QUOTE_STABLE_PASSES)
                if spread > allowed_spread:
                    continue
                if mid_price <= ENTRY_MIN_PRICE or mid_price >= allowed_entry_max:
                    continue

                ask_price = mid_price + (spread / 2.0)
                liq = float(market_ref.get("liquidity") or 5000)
                preview_entry = simulate_execution_price(mid_price, spread, POSITION_SIZE_USD, liq, "buy") if SIMULATED_SPREAD_ENABLED else ask_price * (1 + SIMULATED_FEE_PCT)
                if preview_entry < ENTRY_MIN_PRICE or preview_entry >= allowed_entry_max:
                    continue
                if preview_entry > max_entry_price_for_principal_take():
                    continue
                min_order_size = market_min_order_size(market_ref)
                # Venue minimum share istiyorsa, oraya yetmeyen emirleri direkt eliyoruz.
                if min_order_size is not None:
                    preview_shares = POSITION_SIZE_USD / max(preview_entry, 1e-9)
                    if preview_shares < min_order_size:
                        continue
                if not quote_is_stable(current_slot, slug, outcome, preview_entry, spread, required_passes=required_stability):
                    continue

                signals.append({
                    'coin': coin,
                    'outcome': outcome,
                    'signal_mode': signal_mode,
                    'market': market_ref,
                    'mid_price': mid_price,
                    'spread': spread,
                    'liquidity': liq,
                    'preview_entry': preview_entry,
                    'allowed_entry_max': allowed_entry_max,
                    'allowed_spread': allowed_spread,
                })

            signals.sort(key=lambda x: (x['preview_entry'], x['spread']))
            for sig in signals[:MAX_TRADES_PER_SLOT - slot_trades]:
                open_position(
                    conn=conn,
                    market=sig['market'],
                    coin=sig['coin'],
                    slot_ts=current_slot,
                    outcome=sig['outcome'],
                    mid_price=float(sig['mid_price']),
                    spread=float(sig['spread']),
                    liquidity=float(sig['liquidity']),
                    now_ts=now,
                    signal_mode=str(sig.get('signal_mode') or "unknown"),
                )

    # 3. Dashboard ve dis servisler guncel equity'i bu dosyadan okur.
    try:
        equity = compute_equity(conn, markets, mids)
        with open(BALANCE_PATH, "w", encoding="utf-8") as f:
            f.write(str(equity))
    except Exception:
        pass
    else:
        if MAX_TOTAL_DRAWDOWN_USD > 0 and equity <= total_drawdown_floor():
            msg = (
                f"MAX TOTAL DRAWDOWN HIT | equity=${equity:.2f} "
                f"floor=${total_drawdown_floor():.2f} | stopping 5MIN"
            )
            log(msg)
            telegram_alert(msg, level="ERROR")
            raise SystemExit(89)

    # 4. Manager ve dashboard botun yasadigini heartbeat ile anlar.
    try:
        touch_heartbeat(BOT_KEY)
    except Exception:
        pass

def main():
    global _engine
    # main() startup akisidir:
    # - DB acilir
    # - live ise credential ve bakiye kontrol edilir
    # - execution engine kurulur
    # - sonsuz scan dongusu baslar
    conn = db_connect()
    if TRADING_MODE != "paper":
        creds = load_credentials_from_env()
        signature_type = int(creds.get("signature_type", "0") or "0")
        clob = ClobClientManager(
            private_key=creds.get("private_key", ""), api_key=creds.get("api_key", ""),
            api_secret=creds.get("api_secret", ""), api_passphrase=creds.get("api_passphrase", ""),
            funder_address=creds.get("funder_address", ""),
            signature_type=signature_type, bot_label="5MIN",
        ) if TRADING_MODE == "live" else None
        wallet = WalletManager(private_key=creds.get("private_key", "")) if TRADING_MODE == "live" else None
        if TRADING_MODE == "live" and wallet is not None:
            if not wallet.is_valid():
                log("5MIN live mode blocked: invalid or missing POLY_PRIVATE_KEY")
                raise SystemExit(90)
            derived_address = (wallet.get_address() or "").lower()
            configured_address = (creds.get("funder_address", "") or "").lower()
            if signature_type == 0 and configured_address and derived_address and configured_address != derived_address:
                log("5MIN live mode blocked: POLY_FUNDER_ADDRESS does not match POLY_PRIVATE_KEY for signature_type=0")
                raise SystemExit(90)
            live_balance = clob.get_collateral_balance() if clob is not None else None
            if live_balance is None:
                live_balance = wallet.get_total_usdc_balance()
            if live_balance is not None and live_balance < POSITION_SIZE_USD:
                log(
                    f"5MIN live mode blocked: insufficient collateral balance "
                    f"${live_balance:.2f} < ${POSITION_SIZE_USD:.2f}"
                )
                raise SystemExit(90)
            paper_summary = latest_paper_evaluation_summary()
            ready, reason = paper_evidence_is_ready(paper_summary)
            if not ready:
                if LIVE_ALLOW_UNPROVEN:
                    pass
                else:
                    if paper_summary:
                        log(
                            "5MIN live mode blocked: paper evidence insufficient | "
                            f"reason={reason} | "
                            f"closed={paper_summary['closed_count']} "
                            f"net_pnl=${paper_summary['net_pnl_usd']:.2f} "
                            f"pf={paper_summary['profit_factor']:.2f} "
                            f"force_exit={paper_summary['force_exit_rate']:.1%} "
                            f"stop={paper_summary['stop_rate']:.1%} "
                            f"recent_pnl=${paper_summary.get('recent_pnl_usd', 0.0):.2f} "
                            f"run={paper_summary['run_path']}"
                        )
                    else:
                        log("5MIN live mode blocked: no completed paper evaluation run found")
                    raise SystemExit(90)
        safety = SafetyManager(bot_label="5MIN")
        _engine = ExecutionEngine(
            mode=TRADING_MODE, bot_label="5MIN", bot_key="5min",
            conn=conn, clob_client=clob, wallet_manager=wallet, safety_manager=safety,
            daily_loss_limit=float(os.getenv("DAILY_LOSS_LIMIT_USD", "20")),
            max_position_size=float(os.getenv("MAX_POSITION_SIZE_USD", "25")),
        )
    while True:
        try:
            scan_once(conn)
            time.sleep(SCAN_INTERVAL_SEC)
        except KeyboardInterrupt: sys.exit(0)
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
