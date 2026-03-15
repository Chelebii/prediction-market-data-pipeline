"""
BTC 5MIN CLOB-ONLY Scanner
- Gamma sadece market discovery icin kullanilir.
- Fiyat/spread verisi sadece CLOB /book kaynagindan gelir.
- Fallback fiyat YOKTUR.
"""

import atexit
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import (
    connect_db,
    finish_collector_run,
    insert_orderbook_depth,
    insert_snapshot,
    insert_lifecycle_event,
    resolve_db_path,
    start_collector_run,
    update_collector_run,
    upsert_market,
)
from common.single_instance import acquire_single_instance_lock
from common.bot_notify import send_alert

APP_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_DIR = os.path.dirname(APP_DIR)
load_dotenv(os.path.join(APP_DIR, ".env"))
load_dotenv()

# Bu ayarlar scanner'in ne kadar sik tarayacagini ve veriyi ne kadar sert filtreleyecegini belirler.
# Yeni baslayan biri icin en kritik alanlar:
# - SCAN_INTERVAL_SEC
# - MAX_PRICE_MID_GAP
# - MAX_COMPLEMENT_GAP
# - MIN_LIQUIDITY
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
COIN = "btc"
SCAN_INTERVAL_SEC = int(os.getenv("BTC_5MIN_SCAN_INTERVAL_SEC", "10"))
MAX_BOOK_AGE_SEC = int(os.getenv("BTC_5MIN_MAX_BOOK_AGE_SEC", "20"))
NO_DATA_ALERT_AFTER_SEC = int(os.getenv("BTC_5MIN_NO_DATA_ALERT_AFTER_SEC", "45"))
NO_DATA_ALERT_COOLDOWN_SEC = int(os.getenv("BTC_5MIN_NO_DATA_ALERT_COOLDOWN_SEC", "180"))
NO_DATA_NEW_SLOT_GRACE_SEC = int(os.getenv("BTC_5MIN_NO_DATA_NEW_SLOT_GRACE_SEC", "20"))
MAX_SPREAD = float(os.getenv("BTC_5MIN_MAX_SPREAD", "0.25"))
MAX_PRICE_MID_GAP = float(os.getenv("BTC_5MIN_MAX_PRICE_MID_GAP", "0.015"))
MAX_SIDE_MID_DEVIATION = float(os.getenv("BTC_5MIN_MAX_SIDE_MID_DEVIATION", "0.01"))
MAX_COMPLEMENT_GAP = float(os.getenv("BTC_5MIN_MAX_COMPLEMENT_GAP", "0.03"))
MIN_LIQUIDITY = float(os.getenv("BTC_5MIN_MIN_LIQUIDITY", "5000"))
NEXT_SLOT_PUBLISH_AFTER_SEC = int(os.getenv("BTC_5MIN_NEXT_SLOT_PUBLISH_AFTER_SEC", "295"))
MIN_STABLE_PASSES = int(os.getenv("BTC_5MIN_MIN_STABLE_PASSES", "2"))
SNAPSHOT_PATH = os.getenv(
    "BTC_5MIN_SNAPSHOT_PATH",
    os.path.join(WORKSPACE_DIR, "runtime", "snapshots", "btc_5min_clob_snapshot.json"),
)
LOG_PATH = os.path.join(APP_DIR, "btc_5min_clob_scanner.log")
LOCK_FILE = os.path.join(APP_DIR, "btc_5min_clob_scanner.lock")
USER_AGENT = "mavi-x-btc-5min-clob-scanner/1.0"
COLLECTOR_NAME = "btc5m-clob-scanner"
COLLECTOR_VERSION = "2026-03-15"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

_logger = logging.getLogger("btc_5min_clob_scanner")
_logger.setLevel(logging.INFO)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-CLOB | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_console)
_file_handler = RotatingFileHandler(LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-CLOB | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)

_last_candidate_slug = None
_last_candidate_passes = 0
_dataset_conn: Optional[sqlite3.Connection] = None
_dataset_run_id: Optional[str] = None
_dataset_run_closed = False
_dataset_atexit_registered = False
_dataset_snapshot_count = 0
_dataset_error_count = 0
_dataset_market_ids: set[str] = set()
_dataset_discovered_markets: set[str] = set()
DEPTH_WINDOWS = (("within_1c", 0.01), ("within_2c", 0.02), ("within_5c", 0.05))


def log(msg: str):
    _logger.info(msg)


def telegram_alert(msg: str, level: str = "ERROR"):
    send_alert(bot_label="BTC5M-CLOB", msg=msg, level=level)


def snapshot_age_seconds() -> Optional[float]:
    path = Path(SNAPSHOT_PATH)
    if not path.exists():
        return None
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return None


def snapshot_slot_ts() -> Optional[int]:
    path = Path(SNAPSHOT_PATH)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        slot_ts = data.get("slot_ts")
        return int(slot_ts) if slot_ts is not None else None
    except Exception:
        return None


def http_get(url: str, params: dict = None, timeout: int = 5) -> Optional[requests.Response]:
    # Hata durumunda scanner'in cok gurultulu exception basmamasi icin
    # HTTP istegini kontrollu sekilde sariyoruz.
    try:
        r = session.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r
    except Exception:
        return None
    return None


def market_slot_ts_from_slug(slug: str) -> Optional[int]:
    parts = slug.split("-")
    if len(parts) < 4:
        return None
    try:
        return int(parts[-1])
    except Exception:
        return None


def is_market_active(market: dict, now_ts: int) -> bool:
    # Gamma marketi aktif gosterse bile slot bitmisse bizim icin artik kullanisli degildir.
    if market.get("closed") is True:
        return False
    if market.get("active") is False:
        return False
    end_date = market.get("endDate") or market.get("end_date")
    if end_date:
        try:
            dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
            if dt.timestamp() <= now_ts:
                return False
        except Exception:
            pass
    slot_ts = market_slot_ts_from_slug(str(market.get("slug", "")))
    if slot_ts is None:
        return False
    if now_ts >= slot_ts + 300:
        return False
    return True


def fetch_btc_5min_markets() -> List[dict]:
    # Discovery icin sadece simdiki slot ve bir sonraki slot aranir.
    # Boylece gereksiz market taramasi yapmayiz.
    now_slot = (int(time.time()) // 300) * 300
    slugs = [f"{COIN}-updown-5m-{now_slot}", f"{COIN}-updown-5m-{now_slot + 300}"]
    found = []
    for slug in slugs:
        r = http_get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=5)
        if not r:
            continue
        try:
            events = r.json()
            if events and events[0].get("markets"):
                found.append(events[0]["markets"][0])
        except Exception:
            continue
    return found


def pick_target_markets(markets: List[dict], now_ts: int) -> List[Tuple[dict, str]]:
    # Once current slot tercih edilir.
    # Son saniyelere gelinmisse bir sonraki slotu publish etmeye izin verilir.
    valid = []
    for market in markets:
        slug = str(market.get("slug", ""))
        if not slug.startswith("btc-updown-5m-"):
            continue
        if not is_market_active(market, now_ts):
            continue
        slot_ts = market_slot_ts_from_slug(slug)
        if slot_ts is None:
            continue
        valid.append((slot_ts, market))
    valid.sort(key=lambda item: item[0])
    if not valid:
        return []
    current_slot = (now_ts // 300) * 300
    sec_in = now_ts - current_slot
    current = []
    next_slot = []
    for slot_ts, market in valid:
        if slot_ts == current_slot:
            current.append((market, "ok"))
        elif slot_ts == current_slot + 300:
            next_slot.append((market, "ok_next_slot"))
    if current:
        return current
    if sec_in >= NEXT_SLOT_PUBLISH_AFTER_SEC:
        return next_slot
    return []


def parse_clob_ids(market: dict) -> Tuple[Optional[str], Optional[str]]:
    raw = market.get("clobTokenIds")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not raw or len(raw) < 2:
        return None, None
    return str(raw[0]), str(raw[1])


def _elapsed_ms(start_perf: float) -> int:
    return max(0, int((time.perf_counter() - start_perf) * 1000))


def _safe_float(value) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _normalize_book_levels(levels: list, *, reverse: bool) -> list[dict]:
    rows = []
    for level in levels or []:
        price = _safe_float(level.get("price"))
        size = _safe_float(level.get("size"))
        if price is None or size is None or size <= 0:
            continue
        rows.append({"price": price, "size": size})
    rows.sort(key=lambda row: row["price"], reverse=reverse)
    return rows


def _sum_notional(levels: list[dict]) -> Optional[float]:
    if not levels:
        return None
    return sum(float(level["price"]) * float(level["size"]) for level in levels)


def _levels_within(levels: list[dict], *, best_price: Optional[float], cents: float, side: str) -> list[dict]:
    if not levels or best_price is None:
        return []
    threshold = float(cents)
    if side == "bid":
        return [level for level in levels if (best_price - float(level["price"])) <= threshold + 1e-12]
    return [level for level in levels if (float(level["price"]) - best_price) <= threshold + 1e-12]


def summarize_book_depth(bids: list, asks: list) -> dict:
    bid_levels = _normalize_book_levels(bids, reverse=True)
    ask_levels = _normalize_book_levels(asks, reverse=False)
    best_bid = bid_levels[0]["price"] if bid_levels else None
    best_ask = ask_levels[0]["price"] if ask_levels else None

    summary = {
        "bid_depth_3": _sum_notional(bid_levels[:3]),
        "ask_depth_3": _sum_notional(ask_levels[:3]),
        "bid_depth_5": _sum_notional(bid_levels[:5]),
        "ask_depth_5": _sum_notional(ask_levels[:5]),
        "bid_level_count": len(bid_levels),
        "ask_level_count": len(ask_levels),
    }
    for label, cents in DEPTH_WINDOWS:
        summary[f"bid_depth_{label}"] = _sum_notional(_levels_within(bid_levels, best_price=best_bid, cents=cents, side="bid"))
        summary[f"ask_depth_{label}"] = _sum_notional(_levels_within(ask_levels, best_price=best_ask, cents=cents, side="ask"))
    return summary


def fetch_book(token_id: str) -> Tuple[Optional[dict], str, Optional[int]]:
    # Book'tan en iyi bid/ask, tick size ve minimum order size bilgileri cekilir.
    started_at = time.perf_counter()
    try:
        r = session.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=4)
        latency_ms = _elapsed_ms(started_at)
        if r.status_code != 200:
            return None, f"http_{r.status_code}", latency_ms
        data = r.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid_level = max(bids, key=lambda row: float(row.get("price", 0))) if bids else None
        best_ask_level = min(asks, key=lambda row: float(row.get("price", 1))) if asks else None
        best_bid = float(best_bid_level["price"]) if best_bid_level else None
        best_ask = float(best_ask_level["price"]) if best_ask_level else None
        spread = None
        if best_bid is not None and best_ask is not None and best_ask >= best_bid:
            spread = best_ask - best_bid
        depth_summary = summarize_book_depth(bids, asks)
        return {
            "bid": best_bid,
            "ask": best_ask,
            "spread": spread,
            "bid_size": float(best_bid_level.get("size", 0)) if best_bid_level else None,
            "ask_size": float(best_ask_level.get("size", 0)) if best_ask_level else None,
            "tick_size": float(data.get("tick_size") or data.get("tickSize") or 0) or None,
            "min_order_size": float(data.get("min_order_size") or data.get("minOrderSize") or 0) or None,
            "request_latency_ms": latency_ms,
            **depth_summary,
        }, "ok", latency_ms
    except Exception as e:
        return None, f"error:{e}", _elapsed_ms(started_at)


def fetch_price(token_id: str, side: str) -> Tuple[Optional[float], str, Optional[int]]:
    started_at = time.perf_counter()
    try:
        r = session.get(f"{CLOB_BASE}/price", params={"token_id": token_id, "side": side}, timeout=4)
        latency_ms = _elapsed_ms(started_at)
        if r.status_code != 200:
            return None, f"http_{r.status_code}", latency_ms
        data = r.json()
        return float(data.get("price")), "ok", latency_ms
    except Exception as e:
        return None, f"error:{e}", _elapsed_ms(started_at)


def fetch_midpoint(token_id: str) -> Tuple[Optional[float], str, Optional[int]]:
    started_at = time.perf_counter()
    try:
        r = session.get(f"{CLOB_BASE}/midpoint", params={"token_id": token_id}, timeout=4)
        latency_ms = _elapsed_ms(started_at)
        if r.status_code != 200:
            return None, f"http_{r.status_code}", latency_ms
        data = r.json()
        mid = data.get("mid")
        if mid is None:
            mid = data.get("mid_price")
        return float(mid), "ok", latency_ms
    except Exception as e:
        return None, f"error:{e}", _elapsed_ms(started_at)


def build_side_snapshot(token_id: str) -> Tuple[Optional[dict], str, dict]:
    # Tek bir taraf (YES veya NO) icin gereken tum quote verisini topluyoruz.
    # Buradaki amac su:
    # - price endpoint ile midpoint birbirini dogruluyor mu
    # - spread mantikli mi
    # - book metadata'si mevcut mu
    side_started_at = time.perf_counter()
    book_data, book_reason, book_latency_ms = fetch_book(token_id)
    buy_price, buy_reason, buy_latency_ms = fetch_price(token_id, "BUY")
    sell_price, sell_reason, sell_latency_ms = fetch_price(token_id, "SELL")
    midpoint, mid_reason, mid_latency_ms = fetch_midpoint(token_id)
    timing_meta = {
        "book_request": book_latency_ms,
        "price_buy_request": buy_latency_ms,
        "price_sell_request": sell_latency_ms,
        "midpoint_request": mid_latency_ms,
    }
    side_meta = {
        "timing_ms": {
            **timing_meta,
            "side_total": _elapsed_ms(side_started_at),
        },
        "request_status": {
            "book": book_reason,
            "price_buy": buy_reason,
            "price_sell": sell_reason,
            "midpoint": mid_reason,
        },
        "book_snapshot": {
            "bid": book_data.get("bid") if book_data else None,
            "ask": book_data.get("ask") if book_data else None,
            "bid_size": book_data.get("bid_size") if book_data else None,
            "ask_size": book_data.get("ask_size") if book_data else None,
            "tick_size": book_data.get("tick_size") if book_data else None,
            "min_order_size": book_data.get("min_order_size") if book_data else None,
        },
        "orderbook_exists": bool(book_data and (book_data.get("bid") is not None or book_data.get("ask") is not None)),
    }

    if buy_price is None or sell_price is None:
        return None, f"price_missing buy={buy_reason} sell={sell_reason}", side_meta
    if midpoint is None:
        return None, f"mid_missing {mid_reason}", side_meta

    gap_buy = abs(buy_price - midpoint)
    gap_sell = abs(sell_price - midpoint)
    if gap_buy > MAX_PRICE_MID_GAP or gap_sell > MAX_PRICE_MID_GAP:
        return None, f"price_mid_gap buy={gap_buy:.4f} sell={gap_sell:.4f}", side_meta

    derived_mid = (buy_price + sell_price) / 2.0
    if abs(derived_mid - midpoint) > MAX_SIDE_MID_DEVIATION:
        return None, f"mid_deviation midpoint={midpoint:.4f} derived={derived_mid:.4f}", side_meta
    spread = sell_price - buy_price
    if spread < 0 or spread > MAX_SPREAD:
        return None, f"spread_invalid spread={spread:.4f}", side_meta

    return {
        "bid": buy_price,
        "ask": sell_price,
        "mid": midpoint,
        "spread": spread,
        "derived_mid": derived_mid,
        "book_bid": book_data.get("bid") if book_data else None,
        "book_ask": book_data.get("ask") if book_data else None,
        "book_spread": book_data.get("spread") if book_data else None,
        "book_bid_size": book_data.get("bid_size") if book_data else None,
        "book_ask_size": book_data.get("ask_size") if book_data else None,
        "price_mid_gap_buy": gap_buy,
        "price_mid_gap_sell": gap_sell,
        "tick_size": book_data.get("tick_size") if book_data else None,
        "min_order_size": book_data.get("min_order_size") if book_data else None,
        "book_request_latency_ms": book_latency_ms,
        "price_buy_request_latency_ms": buy_latency_ms,
        "price_sell_request_latency_ms": sell_latency_ms,
        "midpoint_request_latency_ms": mid_latency_ms,
        "side_fetch_latency_ms": side_meta["timing_ms"]["side_total"],
        "bid_depth_3": book_data.get("bid_depth_3") if book_data else None,
        "ask_depth_3": book_data.get("ask_depth_3") if book_data else None,
        "bid_depth_5": book_data.get("bid_depth_5") if book_data else None,
        "ask_depth_5": book_data.get("ask_depth_5") if book_data else None,
        "bid_depth_within_1c": book_data.get("bid_depth_within_1c") if book_data else None,
        "ask_depth_within_1c": book_data.get("ask_depth_within_1c") if book_data else None,
        "bid_depth_within_2c": book_data.get("bid_depth_within_2c") if book_data else None,
        "ask_depth_within_2c": book_data.get("ask_depth_within_2c") if book_data else None,
        "bid_depth_within_5c": book_data.get("bid_depth_within_5c") if book_data else None,
        "ask_depth_within_5c": book_data.get("ask_depth_within_5c") if book_data else None,
        "book_bid_level_count": book_data.get("bid_level_count") if book_data else None,
        "book_ask_level_count": book_data.get("ask_level_count") if book_data else None,
    }, "ok", side_meta


def validate_cross_market(market: dict, yes_data: dict, no_data: dict) -> Tuple[bool, str]:
    # YES ve NO tokenlari birbirinin tamamlaniyor olmasi gerekir.
    # Toplamlari 1'e cok uzaksa veri supheli sayilir.
    liquidity = float(market.get("liquidity") or market.get("liquidityNum") or 0)
    if liquidity < MIN_LIQUIDITY:
        return False, f"liquidity_low {liquidity:.2f} < {MIN_LIQUIDITY:.2f}"

    mid_sum_gap = abs((float(yes_data["mid"]) + float(no_data["mid"])) - 1.0)
    bid_ask_gap = abs((float(yes_data["bid"]) + float(no_data["ask"])) - 1.0)
    ask_bid_gap = abs((float(yes_data["ask"]) + float(no_data["bid"])) - 1.0)
    if mid_sum_gap > MAX_COMPLEMENT_GAP:
        return False, f"mid_sum_gap {mid_sum_gap:.4f}"
    if bid_ask_gap > MAX_COMPLEMENT_GAP:
        return False, f"bid_ask_gap {bid_ask_gap:.4f}"
    if ask_bid_gap > MAX_COMPLEMENT_GAP:
        return False, f"ask_bid_gap {ask_bid_gap:.4f}"
    return True, "ok"


def build_snapshot(market: dict, yes_data: dict, no_data: dict, reason: str) -> dict:
    # Botun okuyacagi tek dosya bu payload ile uretilir.
    # O yuzden gereken metadata'yi burada acikca sakliyoruz.
    now_ts = int(time.time())
    yes_token_id, no_token_id = parse_clob_ids(market)
    slug = str(market.get("slug", ""))
    slot_ts = market_slot_ts_from_slug(slug)
    return {
        "ts": now_ts,
        "source": "clob_price_mid",
        "coin": COIN,
        "market_slug": slug,
        "slot_ts": slot_ts,
        "market_id": str(market.get("conditionId", market.get("id", ""))),
        "question": market.get("question", ""),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "yes_bid": yes_data["bid"] if yes_data else None,
        "yes_ask": yes_data["ask"] if yes_data else None,
        "yes_mid": yes_data["mid"] if yes_data else None,
        "no_bid": no_data["bid"] if no_data else None,
        "no_ask": no_data["ask"] if no_data else None,
        "no_mid": no_data["mid"] if no_data else None,
        "spread_yes": yes_data["spread"] if yes_data else None,
        "spread_no": no_data["spread"] if no_data else None,
        "tick_size": yes_data.get("tick_size") if yes_data and yes_data.get("tick_size") else (no_data.get("tick_size") if no_data else None),
        "min_order_size": yes_data.get("min_order_size") if yes_data and yes_data.get("min_order_size") else (no_data.get("min_order_size") if no_data else None),
        "book_valid": bool(yes_data and no_data),
        "meta": {
            "reason": reason,
            "max_book_age_sec": MAX_BOOK_AGE_SEC,
            "max_price_mid_gap": MAX_PRICE_MID_GAP,
            "max_side_mid_deviation": MAX_SIDE_MID_DEVIATION,
            "max_complement_gap": MAX_COMPLEMENT_GAP,
            "min_liquidity": MIN_LIQUIDITY,
            "end_date": market.get("endDate"),
            "question": market.get("question", ""),
            "liquidity": float(market.get("liquidity") or market.get("liquidityNum") or 0),
            "yes_book_bid": yes_data.get("book_bid") if yes_data else None,
            "yes_book_ask": yes_data.get("book_ask") if yes_data else None,
            "yes_book_bid_size": yes_data.get("book_bid_size") if yes_data else None,
            "yes_book_ask_size": yes_data.get("book_ask_size") if yes_data else None,
            "yes_tick_size": yes_data.get("tick_size") if yes_data else None,
            "yes_min_order_size": yes_data.get("min_order_size") if yes_data else None,
            "no_book_bid": no_data.get("book_bid") if no_data else None,
            "no_book_ask": no_data.get("book_ask") if no_data else None,
            "no_book_bid_size": no_data.get("book_bid_size") if no_data else None,
            "no_book_ask_size": no_data.get("book_ask_size") if no_data else None,
            "no_tick_size": no_data.get("tick_size") if no_data else None,
            "no_min_order_size": no_data.get("min_order_size") if no_data else None,
            "yes_derived_mid": yes_data.get("derived_mid") if yes_data else None,
            "no_derived_mid": no_data.get("derived_mid") if no_data else None,
        },
    }


def write_snapshot(payload: dict):
    # Atomic write kullaniliyor:
    # once .tmp dosyasina yaz, sonra tek hamlede asil dosyanin yerine koy.
    # Boylece bot yarim yazilmis JSON okumaz.
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    tmp_path = SNAPSHOT_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp_path, SNAPSHOT_PATH)


def scanner_config_hash() -> str:
    config = {
        "gamma_base": GAMMA_BASE,
        "clob_base": CLOB_BASE,
        "scan_interval_sec": SCAN_INTERVAL_SEC,
        "max_book_age_sec": MAX_BOOK_AGE_SEC,
        "max_spread": MAX_SPREAD,
        "max_price_mid_gap": MAX_PRICE_MID_GAP,
        "max_side_mid_deviation": MAX_SIDE_MID_DEVIATION,
        "max_complement_gap": MAX_COMPLEMENT_GAP,
        "min_liquidity": MIN_LIQUIDITY,
        "next_slot_publish_after_sec": NEXT_SLOT_PUBLISH_AFTER_SEC,
        "min_stable_passes": MIN_STABLE_PASSES,
        "snapshot_path": SNAPSHOT_PATH,
        "dataset_db_path": str(resolve_db_path()),
    }
    encoded = json.dumps(config, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def init_dataset_writer():
    global _dataset_conn, _dataset_run_id, _dataset_run_closed, _dataset_atexit_registered
    if _dataset_conn is not None:
        return
    try:
        _dataset_conn = connect_db()
        _dataset_run_id = start_collector_run(
            _dataset_conn,
            collector_name=COLLECTOR_NAME,
            collector_version=COLLECTOR_VERSION,
            config_hash=scanner_config_hash(),
            meta_json={
                "db_path": str(resolve_db_path()),
                "log_path": LOG_PATH,
                "snapshot_path": SNAPSHOT_PATH,
                "source_name": "polymarket_clob",
            },
        )
        _dataset_run_closed = False
        if not _dataset_atexit_registered:
            atexit.register(_shutdown_dataset_writer)
            _dataset_atexit_registered = True
        log(f"DATASET DB enabled | path={resolve_db_path()} | run_id={_dataset_run_id}")
    except Exception as exc:
        _dataset_conn = None
        _dataset_run_id = None
        log(f"WARN dataset_db_disabled | reason={exc}")


def _shutdown_dataset_writer():
    close_dataset_writer(status="STOPPED")


def close_dataset_writer(status: str = "STOPPED"):
    global _dataset_conn, _dataset_run_id, _dataset_run_closed
    if _dataset_run_closed:
        return
    _dataset_run_closed = True
    if _dataset_conn is None or _dataset_run_id is None:
        return
    try:
        finish_collector_run(
            _dataset_conn,
            _dataset_run_id,
            status=status,
            snapshot_count=_dataset_snapshot_count,
            market_count=len(_dataset_market_ids),
            error_count=_dataset_error_count,
            meta_json={
                "db_path": str(resolve_db_path()),
                "log_path": LOG_PATH,
                "snapshot_path": SNAPSHOT_PATH,
            },
        )
    except Exception as exc:
        log(f"WARN collector_run_finish_failed | reason={exc}")
    finally:
        try:
            _dataset_conn.close()
        except Exception:
            pass
        _dataset_conn = None
        _dataset_run_id = None
        _dataset_discovered_markets.clear()


def record_collector_error():
    global _dataset_error_count
    _dataset_error_count += 1
    sync_run_metrics()


def sync_run_metrics():
    if _dataset_conn is None or _dataset_run_id is None or _dataset_run_closed:
        return
    try:
        update_collector_run(
            _dataset_conn,
            _dataset_run_id,
            {
                "snapshot_count": _dataset_snapshot_count,
                "market_count": len(_dataset_market_ids),
                "error_count": _dataset_error_count,
                "status": "RUNNING",
            },
        )
    except Exception as exc:
        log(f"WARN collector_run_update_failed | reason={exc}")


def dataset_market_id(payload: dict) -> str:
    market_id = str(payload.get("market_id") or "").strip()
    return market_id or str(payload.get("market_slug") or "").strip()


def build_market_row(
    market: dict,
    payload: Optional[dict] = None,
    *,
    observed_ts: Optional[int] = None,
    yes_token_id: Optional[str] = None,
    no_token_id: Optional[str] = None,
    market_status: Optional[str] = None,
    orderbook_exists_yes: Optional[bool] = None,
    orderbook_exists_no: Optional[bool] = None,
    last_orderbook_seen_ts: Optional[int] = None,
) -> dict:
    payload = payload or {}
    now_ts = int(payload.get("ts") or observed_ts or time.time())
    slot_start_ts = int(payload.get("slot_ts") or market_slot_ts_from_slug(str(market.get("slug", ""))) or (now_ts // 300) * 300)
    yes_token_id = str(payload.get("yes_token_id") or yes_token_id or "")
    no_token_id = str(payload.get("no_token_id") or no_token_id or "")
    rule_text = (
        market.get("description")
        or market.get("rules")
        or market.get("resolutionSource")
        or market.get("question")
        or ""
    )
    return {
        "market_id": dataset_market_id(payload or {
            "market_id": str(market.get("conditionId") or market.get("id") or ""),
            "market_slug": str(market.get("slug") or ""),
        }),
        "market_slug": str(payload.get("market_slug") or market.get("slug") or ""),
        "question": str(market.get("question") or payload.get("question") or ""),
        "slot_start_ts": slot_start_ts,
        "slot_end_ts": slot_start_ts + 300,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "tick_size": payload.get("tick_size"),
        "min_order_size": payload.get("min_order_size"),
        "resolution_source": "polymarket_official_market",
        "resolution_rule_text": str(rule_text),
        "resolution_rule_version": "gamma_market_snapshot_v1",
        "first_seen_ts": now_ts,
        "last_seen_ts": now_ts,
        "last_orderbook_seen_ts": last_orderbook_seen_ts,
        "created_at_ts": now_ts,
        "market_status": str(market_status or "ACTIVE"),
        "orderbook_exists_yes": bool(orderbook_exists_yes),
        "orderbook_exists_no": bool(orderbook_exists_no),
        "market_resolution_status": "ACTIVE",
    }


def build_candidate_extra_meta(
    candidate_started_at: float,
    *,
    yes_meta: Optional[dict] = None,
    no_meta: Optional[dict] = None,
    state_fields: Optional[dict] = None,
    extra_meta: Optional[dict] = None,
) -> dict:
    payload = dict(extra_meta or {})
    payload["timing_ms"] = {
        "candidate_total": _elapsed_ms(candidate_started_at),
        "yes": (yes_meta or {}).get("timing_ms"),
        "no": (no_meta or {}).get("timing_ms"),
    }
    payload["request_status"] = {
        "yes": (yes_meta or {}).get("request_status"),
        "no": (no_meta or {}).get("request_status"),
    }
    if state_fields:
        payload["state_fields"] = dict(state_fields)
    return payload


def build_orderbook_depth_row(
    payload: dict,
    yes_data: Optional[dict],
    no_data: Optional[dict],
    *,
    extra_meta: Optional[dict] = None,
) -> Optional[dict]:
    depth_row = {
        "market_id": dataset_market_id(payload),
        "collected_ts": int(payload.get("ts") or time.time()),
        "yes_bid_depth_3": yes_data.get("bid_depth_3") if yes_data else None,
        "yes_ask_depth_3": yes_data.get("ask_depth_3") if yes_data else None,
        "no_bid_depth_3": no_data.get("bid_depth_3") if no_data else None,
        "no_ask_depth_3": no_data.get("ask_depth_3") if no_data else None,
        "yes_bid_depth_5": yes_data.get("bid_depth_5") if yes_data else None,
        "yes_ask_depth_5": yes_data.get("ask_depth_5") if yes_data else None,
        "no_bid_depth_5": no_data.get("bid_depth_5") if no_data else None,
        "no_ask_depth_5": no_data.get("ask_depth_5") if no_data else None,
        "yes_bid_depth_within_1c": yes_data.get("bid_depth_within_1c") if yes_data else None,
        "yes_ask_depth_within_1c": yes_data.get("ask_depth_within_1c") if yes_data else None,
        "no_bid_depth_within_1c": no_data.get("bid_depth_within_1c") if no_data else None,
        "no_ask_depth_within_1c": no_data.get("ask_depth_within_1c") if no_data else None,
        "yes_bid_depth_within_2c": yes_data.get("bid_depth_within_2c") if yes_data else None,
        "yes_ask_depth_within_2c": yes_data.get("ask_depth_within_2c") if yes_data else None,
        "no_bid_depth_within_2c": no_data.get("bid_depth_within_2c") if no_data else None,
        "no_ask_depth_within_2c": no_data.get("ask_depth_within_2c") if no_data else None,
        "yes_bid_depth_within_5c": yes_data.get("bid_depth_within_5c") if yes_data else None,
        "yes_ask_depth_within_5c": yes_data.get("ask_depth_within_5c") if yes_data else None,
        "no_bid_depth_within_5c": no_data.get("bid_depth_within_5c") if no_data else None,
        "no_ask_depth_within_5c": no_data.get("ask_depth_within_5c") if no_data else None,
        "source_name": str(payload.get("source") or "clob_price_mid"),
        "meta_json": {
            "scanner": COLLECTOR_NAME,
            "run_id": _dataset_run_id,
            **(extra_meta or {}),
        },
    }
    if not any(
        depth_row.get(key) is not None
        for key in (
            "yes_bid_depth_3",
            "yes_ask_depth_3",
            "no_bid_depth_3",
            "no_ask_depth_3",
            "yes_bid_depth_5",
            "yes_ask_depth_5",
            "no_bid_depth_5",
            "no_ask_depth_5",
        )
    ):
        return None
    return depth_row


def side_orderbook_exists(side_data: Optional[dict], side_meta: Optional[dict]) -> bool:
    if side_data and (side_data.get("book_bid") is not None or side_data.get("book_ask") is not None):
        return True
    book_snapshot = (side_meta or {}).get("book_snapshot") or {}
    return bool(book_snapshot.get("bid") is not None or book_snapshot.get("ask") is not None)


def derive_market_status(market: dict, observed_ts: int, *, slot_start_ts: Optional[int] = None) -> str:
    slot_start_ts = int(slot_start_ts if slot_start_ts is not None else market_slot_ts_from_slug(str(market.get("slug", ""))) or (observed_ts // 300) * 300)
    slot_end_ts = slot_start_ts + 300
    if observed_ts >= slot_end_ts:
        return "PENDING_SETTLEMENT"
    if market.get("closed") is True or market.get("active") is False:
        return "CANCELLED"
    return "ACTIVE"


def derive_market_state_fields(
    market: dict,
    payload: dict,
    *,
    yes_data: Optional[dict] = None,
    no_data: Optional[dict] = None,
    yes_meta: Optional[dict] = None,
    no_meta: Optional[dict] = None,
) -> dict:
    observed_ts = int(payload.get("ts") or time.time())
    slot_start_ts = int(payload.get("slot_ts") or market_slot_ts_from_slug(str(market.get("slug", ""))) or (observed_ts // 300) * 300)
    orderbook_exists_yes = side_orderbook_exists(yes_data, yes_meta)
    orderbook_exists_no = side_orderbook_exists(no_data, no_meta)
    return {
        "market_status": derive_market_status(market, observed_ts, slot_start_ts=slot_start_ts),
        "orderbook_exists_yes": orderbook_exists_yes,
        "orderbook_exists_no": orderbook_exists_no,
        "last_orderbook_seen_ts": observed_ts if (orderbook_exists_yes or orderbook_exists_no) else None,
    }


def build_snapshot_row(
    payload: dict,
    stable_pass_count: int,
    *,
    observation_state: str,
    publish_reason: Optional[str] = None,
    reject_reason: Optional[str] = None,
    market_status: str = "ACTIVE",
    orderbook_exists_yes: Optional[bool] = None,
    orderbook_exists_no: Optional[bool] = None,
    collector_latency_ms: Optional[int] = None,
    extra_meta: Optional[dict] = None,
) -> dict:
    now_ts = int(time.time())
    slot_start_ts = int(payload.get("slot_ts") or (now_ts // 300) * 300)
    slot_end_ts = slot_start_ts + 300
    yes_bid = payload.get("yes_bid")
    yes_ask = payload.get("yes_ask")
    yes_mid = payload.get("yes_mid")
    no_bid = payload.get("no_bid")
    no_ask = payload.get("no_ask")
    no_mid = payload.get("no_mid")
    meta = payload.get("meta") or {}
    return {
        "market_id": dataset_market_id(payload),
        "market_slug": str(payload.get("market_slug") or ""),
        "collected_ts": int(payload.get("ts") or now_ts),
        "written_ts": now_ts,
        "source_ts": int(payload.get("ts") or now_ts),
        "seconds_to_resolution": max(0, slot_end_ts - int(payload.get("ts") or now_ts)),
        "best_bid_yes": yes_bid,
        "best_ask_yes": yes_ask,
        "best_bid_no": no_bid,
        "best_ask_no": no_ask,
        "mid_yes": yes_mid,
        "mid_no": no_mid,
        "spread_yes": payload.get("spread_yes"),
        "spread_no": payload.get("spread_no"),
        "best_bid_size_yes": meta.get("yes_book_bid_size"),
        "best_ask_size_yes": meta.get("yes_book_ask_size"),
        "best_bid_size_no": meta.get("no_book_bid_size"),
        "best_ask_size_no": meta.get("no_book_ask_size"),
        "liquidity_market": meta.get("liquidity"),
        "tick_size": payload.get("tick_size"),
        "min_order_size": payload.get("min_order_size"),
        "complement_gap_mid": abs((float(yes_mid) + float(no_mid)) - 1.0) if yes_mid is not None and no_mid is not None else None,
        "complement_gap_cross": max(
            abs((float(yes_bid) + float(no_ask)) - 1.0) if yes_bid is not None and no_ask is not None else 0.0,
            abs((float(yes_ask) + float(no_bid)) - 1.0) if yes_ask is not None and no_bid is not None else 0.0,
        ) if all(value is not None for value in (yes_bid, yes_ask, no_bid, no_ask)) else None,
        "price_mid_gap_yes_buy": abs(float(yes_bid) - float(yes_mid)) if yes_bid is not None and yes_mid is not None else None,
        "price_mid_gap_yes_sell": abs(float(yes_ask) - float(yes_mid)) if yes_ask is not None and yes_mid is not None else None,
        "price_mid_gap_no_buy": abs(float(no_bid) - float(no_mid)) if no_bid is not None and no_mid is not None else None,
        "price_mid_gap_no_sell": abs(float(no_ask) - float(no_mid)) if no_ask is not None and no_mid is not None else None,
        "quote_stable_pass_count": stable_pass_count,
        "book_valid": bool(payload.get("book_valid")),
        "market_status": str(market_status),
        "orderbook_exists_yes": bool(orderbook_exists_yes if orderbook_exists_yes is not None else (meta.get("yes_book_bid") is not None or meta.get("yes_book_ask") is not None)),
        "orderbook_exists_no": bool(orderbook_exists_no if orderbook_exists_no is not None else (meta.get("no_book_bid") is not None or meta.get("no_book_ask") is not None)),
        "publish_reason": str(publish_reason if publish_reason is not None else meta.get("reason") or ""),
        "reject_reason": reject_reason,
        "source_name": str(payload.get("source") or "clob_price_mid"),
        "collector_latency_ms": collector_latency_ms,
        "snapshot_age_ms": max(0, int((time.time() - float(payload.get("ts") or now_ts)) * 1000)),
        "meta_json": {
            "scanner": COLLECTOR_NAME,
            "run_id": _dataset_run_id,
            "observation_state": observation_state,
            "status_reason": meta.get("reason"),
            "validation_meta": meta,
            **(extra_meta or {}),
        },
    }


def ensure_market_discovered(
    market: dict,
    *,
    payload: Optional[dict],
    pick_reason: str,
    observed_ts: Optional[int] = None,
    yes_token_id: Optional[str] = None,
    no_token_id: Optional[str] = None,
    market_status: Optional[str] = None,
    orderbook_exists_yes: Optional[bool] = None,
    orderbook_exists_no: Optional[bool] = None,
    last_orderbook_seen_ts: Optional[int] = None,
):
    market_row = build_market_row(
        market,
        payload,
        observed_ts=observed_ts,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        market_status=market_status,
        orderbook_exists_yes=orderbook_exists_yes,
        orderbook_exists_no=orderbook_exists_no,
        last_orderbook_seen_ts=last_orderbook_seen_ts,
    )
    upsert_market(_dataset_conn, market_row)
    market_id = str(market_row["market_id"])
    if market_id:
        _dataset_market_ids.add(market_id)
    if market_id and market_id not in _dataset_discovered_markets:
        event_ts = int((payload or {}).get("ts") or observed_ts or time.time())
        insert_lifecycle_event(
            _dataset_conn,
            {
                "market_id": market_id,
                "event_ts": event_ts,
                "event_type": "DISCOVERED",
                "reason": "candidate_discovered",
                "meta_json": {
                    "run_id": _dataset_run_id,
                    "market_slug": market_row["market_slug"],
                    "pick_reason": pick_reason,
                },
            },
        )
        _dataset_discovered_markets.add(market_id)
    return market_row


def write_candidate_observation_to_db(
    market: dict,
    payload: dict,
    *,
    observation_state: str,
    pick_reason: str,
    stable_pass_count: int,
    reject_reason: Optional[str] = None,
    lifecycle_reason: Optional[str] = None,
    yes_data: Optional[dict] = None,
    no_data: Optional[dict] = None,
    yes_meta: Optional[dict] = None,
    no_meta: Optional[dict] = None,
    collector_latency_ms: Optional[int] = None,
    extra_meta: Optional[dict] = None,
):
    global _dataset_snapshot_count
    if _dataset_conn is None or _dataset_run_id is None:
        return
    try:
        state_fields = derive_market_state_fields(
            market,
            payload,
            yes_data=yes_data,
            no_data=no_data,
            yes_meta=yes_meta,
            no_meta=no_meta,
        )
        market_row = ensure_market_discovered(
            market,
            payload=payload,
            pick_reason=pick_reason,
            market_status=state_fields["market_status"],
            orderbook_exists_yes=state_fields["orderbook_exists_yes"],
            orderbook_exists_no=state_fields["orderbook_exists_no"],
            last_orderbook_seen_ts=state_fields["last_orderbook_seen_ts"],
        )
        market_id = str(market_row["market_id"])
        inserted = insert_snapshot(
            _dataset_conn,
            build_snapshot_row(
                payload,
                stable_pass_count,
                observation_state=observation_state,
                publish_reason=pick_reason,
                reject_reason=reject_reason,
                market_status=state_fields["market_status"],
                orderbook_exists_yes=state_fields["orderbook_exists_yes"],
                orderbook_exists_no=state_fields["orderbook_exists_no"],
                collector_latency_ms=collector_latency_ms,
                extra_meta=extra_meta,
            ),
        )
        if inserted:
            _dataset_snapshot_count += inserted
        depth_row = build_orderbook_depth_row(payload, yes_data, no_data, extra_meta=extra_meta)
        if depth_row:
            insert_orderbook_depth(_dataset_conn, depth_row)
        insert_lifecycle_event(
            _dataset_conn,
            {
                "market_id": market_id,
                "event_ts": int(payload.get("ts") or time.time()),
                "event_type": observation_state,
                "reason": str(lifecycle_reason or reject_reason or pick_reason or observation_state.lower()),
                "meta_json": {
                    "run_id": _dataset_run_id,
                    "market_slug": payload.get("market_slug"),
                    "pick_reason": pick_reason,
                    "stable_pass_count": stable_pass_count,
                    "reject_reason": reject_reason,
                    **(extra_meta or {}),
                },
            },
        )
        sync_run_metrics()
    except Exception as exc:
        record_collector_error()
        log(
            f"WARN candidate_db_write_failed | slug={payload.get('market_slug', '')} | "
            f"state={observation_state} | reason={exc}"
        )


def scan_once() -> bool:
    global _last_candidate_slug, _last_candidate_passes
    # Scanner'in tek tur mantigi:
    # 1) market discovery
    # 2) current/next slot secimi
    # 3) YES/NO quote toplama
    # 4) validation
    # 5) yeterince stabilse snapshot publish
    now_ts = int(time.time())
    markets = fetch_btc_5min_markets()
    candidates = pick_target_markets(markets, now_ts)
    if not candidates:
        _last_candidate_slug = None
        _last_candidate_passes = 0
        log(f"SKIP market_not_found_or_not_publishable | scanned={len(markets)}")
        return False

    skip_reasons = []
    for market, status in candidates:
        candidate_started_at = time.perf_counter()
        discovery_payload = build_snapshot(market, None, None, status)
        discovery_state_fields = derive_market_state_fields(market, discovery_payload)
        if _dataset_conn is not None and _dataset_run_id is not None:
            try:
                ensure_market_discovered(
                    market,
                    payload=discovery_payload,
                    pick_reason=status,
                    market_status=discovery_state_fields["market_status"],
                    orderbook_exists_yes=discovery_state_fields["orderbook_exists_yes"],
                    orderbook_exists_no=discovery_state_fields["orderbook_exists_no"],
                    last_orderbook_seen_ts=discovery_state_fields["last_orderbook_seen_ts"],
                )
            except Exception as exc:
                record_collector_error()
                log(f"WARN discovery_db_write_failed | slug={market.get('slug', '')} | reason={exc}")

        yes_token_id, no_token_id = parse_clob_ids(market)
        if not yes_token_id or not no_token_id:
            reject_reason = "token_missing"
            reject_payload = build_snapshot(market, None, None, status)
            reject_state_fields = derive_market_state_fields(market, reject_payload)
            write_candidate_observation_to_db(
                market,
                reject_payload,
                observation_state="REJECTED",
                pick_reason=status,
                stable_pass_count=0,
                reject_reason=reject_reason,
                lifecycle_reason=reject_reason,
                collector_latency_ms=_elapsed_ms(candidate_started_at),
                extra_meta=build_candidate_extra_meta(
                    candidate_started_at,
                    state_fields=reject_state_fields,
                    extra_meta={"reject_detail": "yes_token_id_or_no_token_id_missing"},
                ),
            )
            skip_reasons.append(f"{market.get('slug', '')}:token_missing")
            continue

        yes_data, yes_reason, yes_meta = build_side_snapshot(yes_token_id)
        no_data, no_reason, no_meta = build_side_snapshot(no_token_id)
        if not yes_data or not no_data:
            reject_reason = "side_snapshot_invalid"
            reject_payload = build_snapshot(market, yes_data, no_data, status)
            reject_state_fields = derive_market_state_fields(
                market,
                reject_payload,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
            )
            write_candidate_observation_to_db(
                market,
                reject_payload,
                observation_state="REJECTED",
                pick_reason=status,
                stable_pass_count=0,
                reject_reason=reject_reason,
                lifecycle_reason=reject_reason,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
                collector_latency_ms=_elapsed_ms(candidate_started_at),
                extra_meta=build_candidate_extra_meta(
                    candidate_started_at,
                    yes_meta=yes_meta,
                    no_meta=no_meta,
                    state_fields=reject_state_fields,
                    extra_meta={
                        "reject_detail": {
                            "yes_reason": yes_reason,
                            "no_reason": no_reason,
                        },
                    },
                ),
            )
            skip_reasons.append(f"{market.get('slug', '')}:yes={yes_reason}|no={no_reason}|pick={status}")
            continue

        cross_ok, cross_reason = validate_cross_market(market, yes_data, no_data)
        if not cross_ok:
            reject_reason = "cross_validation_failed"
            reject_payload = build_snapshot(market, yes_data, no_data, status)
            reject_state_fields = derive_market_state_fields(
                market,
                reject_payload,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
            )
            write_candidate_observation_to_db(
                market,
                reject_payload,
                observation_state="REJECTED",
                pick_reason=status,
                stable_pass_count=0,
                reject_reason=reject_reason,
                lifecycle_reason=reject_reason,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
                collector_latency_ms=_elapsed_ms(candidate_started_at),
                extra_meta=build_candidate_extra_meta(
                    candidate_started_at,
                    yes_meta=yes_meta,
                    no_meta=no_meta,
                    state_fields=reject_state_fields,
                    extra_meta={"reject_detail": cross_reason},
                ),
            )
            skip_reasons.append(f"{market.get('slug', '')}:cross={cross_reason}|pick={status}")
            continue

        payload = build_snapshot(market, yes_data, no_data, status)
        observation_state_fields = derive_market_state_fields(
            market,
            payload,
            yes_data=yes_data,
            no_data=no_data,
            yes_meta=yes_meta,
            no_meta=no_meta,
        )
        slug = payload["market_slug"]
        if slug == _last_candidate_slug:
            _last_candidate_passes += 1
        else:
            _last_candidate_slug = slug
            _last_candidate_passes = 1

        # Tek sefer temiz quote gormek yetmez.
        # Ayni aday market birkac taramada ust uste temiz gelirse publish edilir.
        if _last_candidate_passes < max(1, MIN_STABLE_PASSES):
            write_candidate_observation_to_db(
                market,
                payload,
                observation_state="WARMUP",
                pick_reason=status,
                stable_pass_count=_last_candidate_passes,
                lifecycle_reason="warmup_pending_stability",
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
                collector_latency_ms=_elapsed_ms(candidate_started_at),
                extra_meta=build_candidate_extra_meta(
                    candidate_started_at,
                    yes_meta=yes_meta,
                    no_meta=no_meta,
                    state_fields=observation_state_fields,
                    extra_meta={
                        "required_stable_passes": max(1, MIN_STABLE_PASSES),
                    },
                ),
            )
            log(
                "WARMUP | slug=%s | pass=%d/%d | yes=%.3f/%.3f mid=%.3f | no=%.3f/%.3f mid=%.3f"
                % (
                    slug,
                    _last_candidate_passes,
                    max(1, MIN_STABLE_PASSES),
                    payload["yes_bid"], payload["yes_ask"], payload["yes_mid"],
                    payload["no_bid"], payload["no_ask"], payload["no_mid"],
                )
            )
            return False

        write_snapshot(payload)
        age = int(time.time()) - payload["ts"]
        if age > MAX_BOOK_AGE_SEC:
            reject_reason = "stale_data"
            stale_state_fields = derive_market_state_fields(
                market,
                payload,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
            )
            write_candidate_observation_to_db(
                market,
                payload,
                observation_state="REJECTED",
                pick_reason=status,
                stable_pass_count=_last_candidate_passes,
                reject_reason=reject_reason,
                lifecycle_reason=reject_reason,
                yes_data=yes_data,
                no_data=no_data,
                yes_meta=yes_meta,
                no_meta=no_meta,
                collector_latency_ms=_elapsed_ms(candidate_started_at),
                extra_meta=build_candidate_extra_meta(
                    candidate_started_at,
                    yes_meta=yes_meta,
                    no_meta=no_meta,
                    state_fields=stale_state_fields,
                    extra_meta={"reject_detail": {"age_sec": age, "max_book_age_sec": MAX_BOOK_AGE_SEC}},
                ),
            )
            log(f"SKIP stale_data | age={age}s | slug={payload['market_slug']}")
            return False

        write_candidate_observation_to_db(
            market,
            payload,
            observation_state="PUBLISHED",
            pick_reason=status,
            stable_pass_count=_last_candidate_passes,
            lifecycle_reason="published_valid_snapshot",
            yes_data=yes_data,
            no_data=no_data,
            yes_meta=yes_meta,
            no_meta=no_meta,
            collector_latency_ms=_elapsed_ms(candidate_started_at),
            extra_meta=build_candidate_extra_meta(
                candidate_started_at,
                yes_meta=yes_meta,
                no_meta=no_meta,
                state_fields=observation_state_fields,
            ),
        )

        log(
            "OK | slug=%s | yes=%.3f/%.3f mid=%.3f | no=%.3f/%.3f mid=%.3f | spread=%.3f/%.3f | stable=%d | source=price+mid"
            % (
                payload["market_slug"],
                payload["yes_bid"], payload["yes_ask"], payload["yes_mid"],
                payload["no_bid"], payload["no_ask"], payload["no_mid"],
                payload["spread_yes"], payload["spread_no"],
                _last_candidate_passes,
            )
        )
        return True

    _last_candidate_slug = None
    _last_candidate_passes = 0
    log("SKIP no_valid_book | " + "; ".join(skip_reasons))
    return False


def main():
    # main() scanner'i tek instance olarak ayakta tutar.
    # Uzun sure fresh snapshot uretilemezse uyarir, runtime error olursa loglar.
    acquire_single_instance_lock(LOCK_FILE, process_name="btc-5min-clob-scanner", on_log=log, takeover=True)
    init_dataset_writer()
    log("BTC 5MIN CLOB-only scanner started")
    error_count = 0
    last_ok_ts = time.time()
    snapshot_age = snapshot_age_seconds()
    if snapshot_age is not None and snapshot_age <= MAX_BOOK_AGE_SEC:
        last_ok_ts = max(last_ok_ts - snapshot_age, 0.0)
    last_no_data_alert_ts = 0.0
    exit_status = "STOPPED"
    try:
        while True:
            try:
                now = time.time()
                ok = scan_once()
                if ok:
                    error_count = 0
                    last_ok_ts = now
                else:
                    error_count += 1
                    gap = now - last_ok_ts
                    current_slot = (int(now) // 300) * 300
                    sec_in_slot = int(now) - current_slot
                    published_slot = snapshot_slot_ts()
                    in_new_slot_grace = (
                        published_slot is not None
                        and published_slot < current_slot
                        and sec_in_slot < NO_DATA_NEW_SLOT_GRACE_SEC
                    )
                    if (
                        gap >= NO_DATA_ALERT_AFTER_SEC
                        and not in_new_slot_grace
                        and (now - last_no_data_alert_ts) >= NO_DATA_ALERT_COOLDOWN_SEC
                    ):
                        telegram_alert(
                            f"BTC 5MIN CLOB scanner {int(gap)}s boyunca fresh snapshot publish edemedi. "
                            f"Recent state skip/warmup olabilir; validation/discovery kontrol et.",
                            level="WARN",
                        )
                        last_no_data_alert_ts = now
                time.sleep(SCAN_INTERVAL_SEC)
            except KeyboardInterrupt:
                exit_status = "STOPPED"
                raise SystemExit(0)
            except Exception as e:
                error_count += 1
                record_collector_error()
                log(f"Runtime Error: {e}")
                if error_count <= 3:
                    telegram_alert(f"BTC 5MIN CLOB scanner error: {e}")
                time.sleep(SCAN_INTERVAL_SEC)
    finally:
        close_dataset_writer(status=exit_status)


if __name__ == "__main__":
    main()
