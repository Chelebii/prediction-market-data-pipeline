"""
BTC 5MIN CLOB-ONLY Scanner
- Gamma sadece market discovery icin kullanilir.
- Fiyat/spread verisi sadece CLOB /book kaynagindan gelir.
- Fallback fiyat YOKTUR.
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


def fetch_book(token_id: str) -> Tuple[Optional[dict], str]:
    # Book'tan en iyi bid/ask, tick size ve minimum order size bilgileri cekilir.
    try:
        r = session.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=4)
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
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
        return {
            "bid": best_bid,
            "ask": best_ask,
            "spread": spread,
            "bid_size": float(best_bid_level.get("size", 0)) if best_bid_level else None,
            "ask_size": float(best_ask_level.get("size", 0)) if best_ask_level else None,
            "tick_size": float(data.get("tick_size") or data.get("tickSize") or 0) or None,
            "min_order_size": float(data.get("min_order_size") or data.get("minOrderSize") or 0) or None,
        }, "ok"
    except Exception as e:
        return None, f"error:{e}"


def fetch_price(token_id: str, side: str) -> Tuple[Optional[float], str]:
    try:
        r = session.get(f"{CLOB_BASE}/price", params={"token_id": token_id, "side": side}, timeout=4)
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
        data = r.json()
        return float(data.get("price")), "ok"
    except Exception as e:
        return None, f"error:{e}"


def fetch_midpoint(token_id: str) -> Tuple[Optional[float], str]:
    try:
        r = session.get(f"{CLOB_BASE}/midpoint", params={"token_id": token_id}, timeout=4)
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
        data = r.json()
        mid = data.get("mid")
        if mid is None:
            mid = data.get("mid_price")
        return float(mid), "ok"
    except Exception as e:
        return None, f"error:{e}"


def build_side_snapshot(token_id: str) -> Tuple[Optional[dict], str]:
    # Tek bir taraf (YES veya NO) icin gereken tum quote verisini topluyoruz.
    # Buradaki amac su:
    # - price endpoint ile midpoint birbirini dogruluyor mu
    # - spread mantikli mi
    # - book metadata'si mevcut mu
    book_data, book_reason = fetch_book(token_id)
    buy_price, buy_reason = fetch_price(token_id, "BUY")
    sell_price, sell_reason = fetch_price(token_id, "SELL")
    midpoint, mid_reason = fetch_midpoint(token_id)

    if buy_price is None or sell_price is None:
        return None, f"price_missing buy={buy_reason} sell={sell_reason}"
    if midpoint is None:
        return None, f"mid_missing {mid_reason}"

    gap_buy = abs(buy_price - midpoint)
    gap_sell = abs(sell_price - midpoint)
    if gap_buy > MAX_PRICE_MID_GAP or gap_sell > MAX_PRICE_MID_GAP:
        return None, f"price_mid_gap buy={gap_buy:.4f} sell={gap_sell:.4f}"

    derived_mid = (buy_price + sell_price) / 2.0
    if abs(derived_mid - midpoint) > MAX_SIDE_MID_DEVIATION:
        return None, f"mid_deviation midpoint={midpoint:.4f} derived={derived_mid:.4f}"
    spread = sell_price - buy_price
    if spread < 0 or spread > MAX_SPREAD:
        return None, f"spread_invalid spread={spread:.4f}"

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
    }, "ok"


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
        yes_token_id, no_token_id = parse_clob_ids(market)
        if not yes_token_id or not no_token_id:
            skip_reasons.append(f"{market.get('slug', '')}:token_missing")
            continue

        yes_data, yes_reason = build_side_snapshot(yes_token_id)
        no_data, no_reason = build_side_snapshot(no_token_id)
        if not yes_data or not no_data:
            skip_reasons.append(f"{market.get('slug', '')}:yes={yes_reason}|no={no_reason}|pick={status}")
            continue

        cross_ok, cross_reason = validate_cross_market(market, yes_data, no_data)
        if not cross_ok:
            skip_reasons.append(f"{market.get('slug', '')}:cross={cross_reason}|pick={status}")
            continue

        payload = build_snapshot(market, yes_data, no_data, status)
        slug = payload["market_slug"]
        if slug == _last_candidate_slug:
            _last_candidate_passes += 1
        else:
            _last_candidate_slug = slug
            _last_candidate_passes = 1

        # Tek sefer temiz quote gormek yetmez.
        # Ayni aday market birkac taramada ust uste temiz gelirse publish edilir.
        if _last_candidate_passes < max(1, MIN_STABLE_PASSES):
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
            log(f"SKIP stale_data | age={age}s | slug={payload['market_slug']}")
            return False

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
    log("BTC 5MIN CLOB-only scanner started")
    error_count = 0
    last_ok_ts = time.time()
    snapshot_age = snapshot_age_seconds()
    if snapshot_age is not None and snapshot_age <= MAX_BOOK_AGE_SEC:
        last_ok_ts = max(last_ok_ts - snapshot_age, 0.0)
    last_no_data_alert_ts = 0.0
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
            raise SystemExit(0)
        except Exception as e:
            error_count += 1
            log(f"Runtime Error: {e}")
            if error_count <= 3:
                telegram_alert(f"BTC 5MIN CLOB scanner error: {e}")
            time.sleep(SCAN_INTERVAL_SEC)


if __name__ == "__main__":
    main()
