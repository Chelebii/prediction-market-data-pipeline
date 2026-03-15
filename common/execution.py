"""
execution.py -- Paper vs Live Trading Abstraction Layer
======================================================
Tum botlarin open/close islemlerini soyutlar.
Strateji mantigi ICERMEZ -- sadece execution.

Modlar:
  - paper:   Mevcut davranis (sadece SQLite)
  - live:    Gercek CLOB order + SQLite kayit
  - dry-run: Order olustur ama gonderme, logla + SQLite'a DRY-RUN notu ile kaydet
"""

import logging
import os
import requests
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.bot_notify import send_alert

logger = logging.getLogger("execution")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] [EXEC] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)

VALID_MODES = {"paper", "live", "dry-run"}

ORDER_MATCHED_STATUSES = {"matched", "filled", "confirmed", "confirmed_unsettled", "mined"}
ORDER_TERMINAL_FAILURE_STATUSES = {"canceled", "cancelled", "rejected", "expired", "failed", "unmatched"}


# --- DB Migration -------------------------------------------------------------

def migrate_db(conn: sqlite3.Connection) -> None:
    """
    paper_positions tablosuna live trading icin gerekli sutunlari ekler.
    Idempotent -- sutun varsa hata vermez.
    """
    migrations = [
        ("order_id", "TEXT"),
        ("trading_mode", "TEXT DEFAULT 'paper'"),
        ("fill_price", "REAL"),
    ]
    for col_name, col_type in migrations:
        try:
            conn.execute(f"ALTER TABLE paper_positions ADD COLUMN {col_name} {col_type}")
            conn.commit()
            logger.info("DB migration: '%s' sutunu eklendi.", col_name)
        except sqlite3.OperationalError:
            # Sutun zaten var -- sorun yok
            pass


# --- Telegram Helpers ---------------------------------------------------------

def _trade_notifications_enabled() -> bool:
    raw = os.getenv("TRADE_NOTIFICATIONS_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _send_live_notification(bot_label: str, msg: str, level: str = "INFO") -> None:
    """Live/dry-run trade bildirimlerini Telegram'a gonderir."""
    if level.upper() not in {"ERROR", "ALERT"} and not _trade_notifications_enabled():
        return
    send_alert(bot_label=bot_label, msg=msg, level=level, dedupe_seconds=5)


def _fallback_buy_worst_price(entry_price: float, worst_price: Optional[float]) -> float:
    if worst_price is not None and worst_price > 0:
        return min(0.99, max(float(entry_price), float(worst_price)))
    pct = max(0.01, float(os.getenv("DEFAULT_LIVE_BUY_SLIPPAGE_PCT", "0.08")))
    return min(0.99, float(entry_price) * (1.0 + pct))


def _fallback_sell_worst_price(exit_price: float, worst_price: Optional[float]) -> float:
    if worst_price is not None and worst_price > 0:
        return max(0.01, min(float(exit_price), float(worst_price)))
    pct = max(0.01, float(os.getenv("DEFAULT_LIVE_SELL_SLIPPAGE_PCT", "0.10")))
    return max(0.01, float(exit_price) * (1.0 - pct))


def _is_fok_fill_failure(error_text: str) -> bool:
    low = str(error_text or "").lower()
    return (
        "fully filled or killed" in low
        or "couldn't be fully filled" in low
        or "tum retry'lar basarisiz" in low
    )


def _sell_floor_attempts(exit_price: float, worst_price: Optional[float]) -> list[float]:
    """
    Live close icin tek bir fiyat tabanina bagli kalma.
    Likidite inceyse tabani kademeli olarak biraz daha asagi cekip
    cikisi tamamlama sansini artiriyoruz.
    """
    base = _fallback_sell_worst_price(exit_price, worst_price)
    widen_steps = [0.0, 0.03, 0.06]
    floors: list[float] = []
    for step in widen_steps:
        floor = max(0.01, min(float(exit_price), base - step))
        if not floors or abs(floors[-1] - floor) > 1e-9:
            floors.append(floor)
    return floors


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_order_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _request_first_trade_approval(
    bot_label: str, bot_key: str,
    question: str, outcome: str, entry_price: float, size_usd: float,
    timeout_sec: int = 300,
) -> bool:
    """
    Telegram inline button ile ilk live trade onayi iste.
    Onayla/Reddet butonlariyla mesaj gonderir, callback_query bekler.
    timeout_sec icinde onay gelmezse False doner.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        logger.warning("[%s] Telegram token/chat_id yok -- 30s bekleme fallback.", bot_label)
        time.sleep(30)
        return True  # Telegram yoksa fallback olarak izin ver

    text = (
        f" ILK LIVE TRADE ONAYI\n\n"
        f"Bot: {bot_label} ({bot_key})\n"
        f"Market: {question[:80]}\n"
        f"Yon: {outcome} @ {entry_price:.4f}\n"
        f"Tutar: ${size_usd:.2f}\n\n"
        f" {timeout_sec}s icinde onay ver:"
    )

    callback_approve = f"approve_first_{bot_key}"
    callback_reject = f"reject_first_{bot_key}"
    keyboard = {
        "inline_keyboard": [
            [
                {"text": " Onayla", "callback_data": callback_approve},
                {"text": " Reddet", "callback_data": callback_reject},
            ]
        ]
    }

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "reply_markup": keyboard},
            timeout=10,
        )
        msg_data = resp.json()
        if not msg_data.get("ok"):
            logger.warning("[%s] Telegram mesaj gonderilemedi -- fallback 30s.", bot_label)
            time.sleep(30)
            return True
    except Exception as e:
        logger.warning("[%s] Telegram hatasi: %s -- fallback 30s.", bot_label, e)
        time.sleep(30)
        return True

    # Polling for callback_query
    start = time.time()
    last_update_id = 0
    while time.time() - start < timeout_sec:
        try:
            updates = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 10, "allowed_updates": '["callback_query"]'},
                timeout=15,
            ).json()

            for upd in updates.get("result", []):
                last_update_id = upd["update_id"]
                cq = upd.get("callback_query")
                if not cq:
                    continue
                cb_data = cq.get("data", "")

                if cb_data == callback_approve:
                    # Answer callback + edit message
                    requests.post(
                        f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                        json={"callback_query_id": cq["id"], "text": " Onaylandi!"},
                        timeout=5,
                    )
                    requests.post(
                        f"https://api.telegram.org/bot{token}/editMessageText",
                        json={
                            "chat_id": chat_id,
                            "message_id": msg_data["result"]["message_id"],
                            "text": text + "\n\n ONAYLANDI",
                        },
                        timeout=5,
                    )
                    return True

                elif cb_data == callback_reject:
                    requests.post(
                        f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                        json={"callback_query_id": cq["id"], "text": " Reddedildi!"},
                        timeout=5,
                    )
                    requests.post(
                        f"https://api.telegram.org/bot{token}/editMessageText",
                        json={
                            "chat_id": chat_id,
                            "message_id": msg_data["result"]["message_id"],
                            "text": text + "\n\n REDDEDILDI",
                        },
                        timeout=5,
                    )
                    return False

        except Exception as e:
            logger.warning("[%s] Polling hatasi: %s", bot_label, e)
            time.sleep(5)

    # Timeout -- reddet
    logger.warning("[%s] Ilk trade onayi zaman asimi (%ds).", bot_label, timeout_sec)
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={
                "chat_id": chat_id,
                "message_id": msg_data["result"]["message_id"],
                "text": text + "\n\n? ZAMAN ASIMI -- Trade iptal edildi.",
            },
            timeout=5,
        )
    except Exception:
        pass
    return False


# --- Execution Engine ---------------------------------------------------------

class ExecutionEngine:
    """
    Paper / Live / Dry-run execution abstraction.

    paper:   Sadece SQLite INSERT/UPDATE (mevcut davranis, regresyon yok)
    live:    Safety kontrol -> bakiye kontrol -> CLOB order -> SQLite kayit
    dry-run: Order parametrelerini logla + SQLite'a DRY-RUN notu ile kaydet
    """

    def __init__(
        self,
        mode: str = "paper",
        bot_label: str = "",
        bot_key: str = "",
        conn: Optional[sqlite3.Connection] = None,
        clob_client=None,       # ClobClientManager instance (live modda)
        wallet_manager=None,    # WalletManager instance (live modda)
        safety_manager=None,    # SafetyManager instance (live modda)
        daily_loss_limit: float = 20.0,
        max_position_size: float = 25.0,
    ):
        mode = (mode or "paper").strip().lower()
        if mode not in VALID_MODES:
            logger.warning("Gecersiz mode '%s' -- paper'a dusuruldu.", mode)
            mode = "paper"

        self.mode = mode
        self.bot_label = bot_label
        self.bot_key = bot_key
        self.conn = conn
        self.clob_client = clob_client
        self.wallet_manager = wallet_manager
        self.safety_manager = safety_manager
        self.daily_loss_limit = daily_loss_limit
        self.max_position_size = max_position_size

        if mode != "paper":
            logger.info(
                "ExecutionEngine baslatildi: mode=%s, bot=%s, daily_limit=$%.0f, max_size=$%.0f",
                mode, bot_label, daily_loss_limit, max_position_size,
            )

    def extract_live_fill_price(
        self,
        order_resp,
        side: str,
        fallback_price: float,
    ) -> float:
        raw = getattr(order_resp, "raw", None) or {}
        for key in ("avgPrice", "avg_price", "fillPrice", "fill_price", "matchedPrice", "matched_price"):
            value = _safe_float(raw.get(key))
            if value is not None and value > 0:
                return max(0.01, min(0.99, value))

        making_amount = _safe_float(raw.get("makingAmount"))
        if making_amount is None:
            making_amount = _safe_float(raw.get("making_amount"))
        taking_amount = _safe_float(raw.get("takingAmount"))
        if taking_amount is None:
            taking_amount = _safe_float(raw.get("taking_amount"))

        if making_amount and taking_amount and making_amount > 0 and taking_amount > 0:
            if side == "buy":
                return max(0.01, min(0.99, making_amount / taking_amount))
            if side == "sell":
                return max(0.01, min(0.99, taking_amount / making_amount))

        return max(0.01, min(0.99, float(fallback_price)))

    def _order_has_fill(self, payload: Optional[Dict[str, Any]], status: str) -> bool:
        raw = payload or {}
        if status in ORDER_MATCHED_STATUSES:
            return True
        trade_ids = raw.get("tradeIDs")
        if trade_ids is None:
            trade_ids = raw.get("trade_ids")
        if isinstance(trade_ids, list) and len(trade_ids) > 0:
            return True
        size_matched = _safe_float(raw.get("size_matched"))
        if size_matched is None:
            size_matched = _safe_float(raw.get("sizeMatched"))
        return bool(size_matched and size_matched > 0)

    def _resolved_order_payload(self, order_resp) -> Dict[str, Any]:
        raw = getattr(order_resp, "raw", None) or {}
        status = _normalize_order_status(getattr(order_resp, "status", "") or raw.get("status"))
        if self._order_has_fill(raw, status):
            return raw
        if self.clob_client is not None and getattr(order_resp, "order_id", ""):
            detail = self.clob_client.get_order(order_resp.order_id)
            if isinstance(detail, dict):
                return detail
        return raw

    def extract_live_matched_shares(self, order_resp) -> Optional[float]:
        raw = self._resolved_order_payload(order_resp)
        value = _safe_float(raw.get("size_matched"))
        if value is None:
            value = _safe_float(raw.get("sizeMatched"))
        if value is None:
            value = _safe_float(raw.get("matched_size"))
        if value is None:
            value = _safe_float(raw.get("matchedSize"))
        return value if value is not None and value > 0 else None

    def verify_live_order(
        self,
        order_resp,
        *,
        timeout_sec: float = 8.0,
        poll_interval_sec: float = 1.5,
    ) -> Tuple[bool, str]:
        if self.mode != "live":
            return True, "not_live"
        if not getattr(order_resp, "success", False):
            return False, "order_failed"

        raw = getattr(order_resp, "raw", None) or {}
        status = _normalize_order_status(getattr(order_resp, "status", "") or raw.get("status"))
        if self._order_has_fill(raw, status):
            return True, status or "matched"

        if self.clob_client is None or not getattr(order_resp, "order_id", ""):
            return False, status or "unverified"

        deadline = time.time() + max(0.0, float(timeout_sec))
        last_status = status or "pending"
        while time.time() <= deadline:
            detail = self.clob_client.get_order(order_resp.order_id)
            if isinstance(detail, dict):
                last_status = _normalize_order_status(detail.get("status")) or last_status
                if self._order_has_fill(detail, last_status):
                    return True, last_status or "matched"
                if last_status in ORDER_TERMINAL_FAILURE_STATUSES:
                    return False, last_status
            time.sleep(max(0.2, float(poll_interval_sec)))

        return False, last_status or "timeout"

    # --- OPEN -------------------------------------------------------------

    def execute_open(
        self,
        conn: sqlite3.Connection,
        market_id: str,
        market_slug: str,
        question: str,
        outcome: str,
        token_id: str,
        entry_price: float,
        size_usd: float,
        now_ts: int,
        worst_price: Optional[float] = None,
        order_tick_size: Optional[float] = None,
        extra_cols: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Pozisyon acma -- mode'a gore paper/live/dry-run.

        extra_cols: 5MIN gibi farkli sema kullanan botlar icin ek sutunlar
                    ornek: {"coin": "btc", "slot_ts": 123456}

        Returns: {"success": bool, "order_id": str, "error": str, "mode": str}
        """

        result = {
            "success": False,
            "order_id": "",
            "error": "",
            "mode": self.mode,
            "fill_price": entry_price,
        }

        # -- Paper Mode ----------------------------------------------------
        if self.mode == "paper":
            # Paper modda bu fonksiyon CAGRILMAZ -- bot kendi open_position'ini kullanir
            # Ama cagirilirsa da sorunsuz calisir (fallback)
            result["success"] = True
            result["mode"] = "paper"
            return result

        # -- Safety Kontrolleri (live + dry-run) ---------------------------
        if self.safety_manager is not None:
            # Kill switch
            if self.safety_manager.is_kill_switch_active():
                msg = "Kill switch aktif -- yeni pozisyon acilamaz."
                logger.warning("[%s] %s", self.bot_label, msg)
                result["error"] = msg
                return result

            # Position size guard
            if not self.safety_manager.validate_position_size(size_usd, self.max_position_size):
                msg = f"Position size ${size_usd:.2f} limiti (${self.max_position_size:.2f}) asiyor."
                logger.warning("[%s] %s", self.bot_label, msg)
                result["error"] = msg
                return result

            # Daily loss limit kontrolu
            if not self.safety_manager.check_daily_limit(self.bot_key, 0, self.daily_loss_limit):
                msg = "Gunluk kayip limiti asildi -- yeni pozisyon acilamaz."
                logger.warning("[%s] %s", self.bot_label, msg)
                result["error"] = msg
                return result

            # Ilk trade onayi (inline button callback)
            if self.mode == "live" and self.safety_manager.is_first_live_trade(self.bot_key):
                logger.info("[%s] Ilk live trade -- Telegram onayi bekleniyor...", self.bot_label)
                approved = _request_first_trade_approval(
                    self.bot_label, self.bot_key,
                    question, outcome, entry_price, size_usd,
                    timeout_sec=300,
                )
                if not approved:
                    msg = "Ilk live trade onayi reddedildi veya zaman asimi."
                    logger.warning("[%s] %s", self.bot_label, msg)
                    result["error"] = msg
                    return result
                logger.info("[%s] Ilk live trade onaylandi!", self.bot_label)

        # -- Bakiye Kontrolu (live) ----------------------------------------
        if self.mode == "live" and (self.wallet_manager is not None or self.clob_client is not None):
            balance = None
            balance_source = "unknown"
            if self.clob_client is not None and hasattr(self.clob_client, "get_collateral_balance"):
                balance = self.clob_client.get_collateral_balance()
                if balance is not None:
                    balance_source = "clob_collateral"
            if balance is None and self.wallet_manager is not None and hasattr(self.wallet_manager, "get_total_usdc_balance"):
                balance = self.wallet_manager.get_total_usdc_balance()
                if balance is not None:
                    balance_source = "polygon_wallet_total"
            if balance is None and self.wallet_manager is not None:
                balance = self.wallet_manager.get_usdc_balance()
                if balance is not None:
                    balance_source = "polygon_wallet_usdc"
            if balance is not None and balance < size_usd:
                msg = f"Yetersiz collateral bakiye ({balance_source}): ${balance:.2f} < ${size_usd:.2f}"
                logger.warning("[%s] %s", self.bot_label, msg)
                _send_live_notification(self.bot_label, msg, level="ERROR")
                result["error"] = msg
                return result

        # -- Live Mode -- CLOB Order ----------------------------------------
        if self.mode == "live":
            if self.clob_client is None:
                result["error"] = "CLOB client baslatilamadi -- live trade yapilamaz."
                logger.error("[%s] %s", self.bot_label, result["error"])
                return result

            buy_cap = _fallback_buy_worst_price(entry_price, worst_price)
            order_resp = self.clob_client.market_buy(
                token_id=token_id,
                amount_usd=size_usd,
                worst_price=buy_cap,
                tick_size=order_tick_size,
            )

            if not order_resp.success:
                msg = f"CLOB order basarisiz: {order_resp.error}"
                logger.error("[%s] %s", self.bot_label, msg)
                _send_live_notification(self.bot_label, msg, level="ERROR")
                result["error"] = msg
                return result

            verified, verify_reason = self.verify_live_order(order_resp)
            if not verified:
                msg = f"CLOB order verify basarisiz: {verify_reason}"
                logger.error("[%s] %s", self.bot_label, msg)
                _send_live_notification(self.bot_label, msg, level="ERROR")
                if self.safety_manager is not None:
                    self.safety_manager.activate_kill_switch(f"{self.bot_label} live open verify failed: {verify_reason}")
                result["error"] = msg
                return result

            fill_price = self.extract_live_fill_price(order_resp, "buy", entry_price)
            matched_shares = self.extract_live_matched_shares(order_resp)
            actual_size_usd = (matched_shares * fill_price) if matched_shares and fill_price > 0 else size_usd
            result["success"] = True
            result["order_id"] = order_resp.order_id
            result["fill_price"] = fill_price
            result["matched_shares"] = matched_shares or 0.0
            result["actual_size_usd"] = actual_size_usd
            logger.info(
                "[%s]  LIVE OPEN -- %s -- %s @ %.4f ($%.2f, cap=%.4f) -- order_id=%s",
                self.bot_label, question[:40], outcome, fill_price, actual_size_usd, buy_cap, order_resp.order_id,
            )
            _send_live_notification(
                self.bot_label,
                f" LIVE OPEN -- {question[:50]} -- {outcome} @ {fill_price:.4f} (${actual_size_usd:.2f} real) -- order:{order_resp.order_id[:12]}",
                level="SUCCESS",
            )

            # Ilk trade tamamlandi olarak isaretle
            if self.safety_manager is not None:
                self.safety_manager.mark_first_trade_completed(self.bot_key)

        # -- Dry-Run Mode --------------------------------------------------
        elif self.mode == "dry-run":
            result["success"] = True
            result["order_id"] = f"DRY-RUN-{int(time.time())}"
            logger.info(
                "[%s]  DRY-RUN OPEN -- %s -- %s @ %.4f ($%.2f)",
                self.bot_label, question[:40], outcome, entry_price, size_usd,
            )
            _send_live_notification(
                self.bot_label,
                f" DRY-RUN OPEN -- {question[:50]} -- {outcome} @ {entry_price:.4f} (${size_usd:.2f})",
                level="INFO",
            )

        # -- SQLite Kayit (live + dry-run) ---------------------------------
        if result["success"] and conn is not None:
            try:
                row_extra = dict(extra_cols or {})
                if "original_size_usd" in row_extra:
                    row_extra["original_size_usd"] = result.get("actual_size_usd", size_usd)
                _insert_position(
                    conn=conn,
                    market_id=market_id,
                    market_slug=market_slug,
                    question=question,
                    outcome=outcome,
                    token_id=token_id,
                    entry_price=result["fill_price"],
                    size_usd=result.get("actual_size_usd", size_usd),
                    now_ts=now_ts,
                    order_id=result["order_id"],
                    trading_mode=self.mode,
                    extra_cols=row_extra,
                )
            except Exception as e:
                logger.error("[%s] SQLite kayit hatasi: %s", self.bot_label, e)

        return result

    # --- CLOSE ------------------------------------------------------------

    def execute_close(
        self,
        conn: sqlite3.Connection,
        pos_id: int,
        token_id: str,
        entry_price: float,
        size_usd: float,
        exit_price: float,
        now_ts: int,
        reason: str,
        worst_price: Optional[float] = None,
        order_tick_size: Optional[float] = None,
        market_slug: str = "",
        question: str = "",
        outcome: str = "",
    ) -> Dict[str, Any]:
        """
        Pozisyon kapatma -- mode'a gore paper/live/dry-run.

        Returns: {"success": bool, "pnl_usd": float, "pnl_pct": float, "order_id": str, "error": str}
        """

        shares = size_usd / entry_price if entry_price > 0 else 0
        pnl_usd = (exit_price - entry_price) * shares
        pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0

        result = {
            "success": False,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "order_id": "",
            "error": "",
            "mode": self.mode,
            "fill_price": exit_price,
        }

        # -- Paper Mode ----------------------------------------------------
        if self.mode == "paper":
            result["success"] = True
            return result

        # -- Live Mode -- CLOB Sell -----------------------------------------
        if self.mode == "live":
            if self.clob_client is None:
                result["error"] = "CLOB client yok -- live close yapilamaz."
                logger.error("[%s] %s", self.bot_label, result["error"])
                if self.safety_manager is not None:
                    self.safety_manager.activate_kill_switch(f"{self.bot_label} live close failed: missing CLOB client")
                return result

            actual_balance_shares = None
            if hasattr(self.clob_client, "get_conditional_token_balance"):
                actual_balance_shares = self.clob_client.get_conditional_token_balance(token_id)
                if actual_balance_shares is not None:
                    shares = min(shares, max(0.0, float(actual_balance_shares)))

            if shares <= 0:
                result["error"] = "Satilabilir conditional token bakiyesi yok."
                logger.error("[%s] %s", self.bot_label, result["error"])
                return result

            floor_attempts = _sell_floor_attempts(exit_price, worst_price)
            order_resp = None
            sell_floor = floor_attempts[0]
            for idx, attempt_floor in enumerate(floor_attempts, start=1):
                sell_floor = attempt_floor
                order_resp = self.clob_client.market_sell(
                    token_id=token_id,
                    shares=shares,
                    worst_price=attempt_floor,
                    tick_size=order_tick_size,
                )
                if order_resp.success:
                    break
                if not _is_fok_fill_failure(order_resp.error):
                    break
                logger.warning(
                    "[%s] LIVE CLOSE retryable fill failure (%s/%s) -- floor=%.4f error=%s",
                    self.bot_label,
                    idx,
                    len(floor_attempts),
                    attempt_floor,
                    order_resp.error,
                )

            if order_resp.success:
                verified, verify_reason = self.verify_live_order(order_resp)
                if not verified:
                    msg = f"CLOB close verify basarisiz: {verify_reason}"
                    logger.error("[%s] %s", self.bot_label, msg)
                    _send_live_notification(self.bot_label, " LIVE CLOSE VERIFY FAILED", level="ERROR")
                    logger.warning(
                        "[%s] LIVE CLOSE DETAIL -- verify failed | question=%s | outcome=%s | reason=%s",
                        self.bot_label,
                        question[:60],
                        outcome,
                        verify_reason,
                    )
                    result["error"] = msg
                    return result

                fill_price = self.extract_live_fill_price(order_resp, "sell", exit_price)
                matched_shares = self.extract_live_matched_shares(order_resp)
                if matched_shares is not None and matched_shares > 0:
                    shares = min(shares, matched_shares)
                pnl_usd = (fill_price - entry_price) * shares
                pnl_pct = (fill_price - entry_price) / entry_price if entry_price > 0 else 0
                result["success"] = True
                result["order_id"] = order_resp.order_id
                result["fill_price"] = fill_price
                result["pnl_usd"] = pnl_usd
                result["pnl_pct"] = pnl_pct
                result["sold_shares"] = shares
                result["available_shares"] = actual_balance_shares

                logger.info(
                    "[%s]  LIVE CLOSE -- %s -- %s @ %.4f -> %.4f (floor=%.4f, PnL: $%+.2f, %s)",
                    self.bot_label, question[:30], outcome, entry_price, fill_price, sell_floor, pnl_usd, reason,
                )
                _send_live_notification(
                    self.bot_label,
                    f" LIVE CLOSE -- {question[:40]} -- PnL: ${pnl_usd:+.2f} ({pnl_pct*100:+.1f}%) [{reason}]",
                    level="SUCCESS" if pnl_usd >= 0 else "WARN",
                )
            else:
                orderbook_exists = None
                if hasattr(self.clob_client, "token_has_orderbook"):
                    orderbook_exists = self.clob_client.token_has_orderbook(token_id)
                if orderbook_exists is False:
                    result["error"] = "MARKET_CLOSED_NO_ORDERBOOK"
                    result["no_orderbook"] = True
                    logger.warning(
                        "[%s] LIVE CLOSE skip -- token orderbook yok, market expiry/settlement olabilir. token=%s",
                        self.bot_label,
                        token_id[:12],
                    )
                    return result
                logger.error("[%s] CLOB sell basarisiz: %s", self.bot_label, order_resp.error)
                _send_live_notification(
                    self.bot_label,
                    " LIVE CLOSE FAILED",
                    level="ERROR",
                )
                result["error"] = order_resp.error
                logger.warning(
                    "[%s] LIVE CLOSE DETAIL -- question=%s | outcome=%s | target=%.4f | final_floor=%.4f | shares=%.4f | error=%s",
                    self.bot_label,
                    question[:60],
                    outcome,
                    exit_price,
                    sell_floor,
                    shares,
                    order_resp.error,
                )
                return result

        # -- Dry-Run Mode --------------------------------------------------
        elif self.mode == "dry-run":
            result["success"] = True
            result["order_id"] = f"DRY-RUN-{int(time.time())}"
            logger.info(
                "[%s]  DRY-RUN CLOSE -- %s -- PnL: $%+.2f (%s)",
                self.bot_label, question[:30], pnl_usd, reason,
            )
            _send_live_notification(
                self.bot_label,
                f" DRY-RUN CLOSE -- {question[:40]} -- PnL: ${pnl_usd:+.2f} [{reason}]",
                level="INFO",
            )

        # -- SQLite Update (live + dry-run) --------------------------------
        if result["success"] and conn is not None:
            try:
                _update_position_closed(
                    conn, pos_id, result["fill_price"], result["pnl_usd"], result["pnl_pct"],
                    now_ts, reason, result["order_id"], self.mode,
                )
            except Exception as e:
                logger.error("[%s] SQLite close update hatasi: %s", self.bot_label, e)

        # -- Daily PnL kaydi (live) ----------------------------------------
        if self.mode == "live" and result["success"] and self.safety_manager is not None:
            self.safety_manager.record_trade_pnl(self.bot_key, result["pnl_usd"])

        return result


# --- SQLite Helpers -----------------------------------------------------------

def _insert_position(
    conn: sqlite3.Connection,
    market_id: str,
    market_slug: str,
    question: str,
    outcome: str,
    token_id: str,
    entry_price: float,
    size_usd: float,
    now_ts: int,
    order_id: str = "",
    trading_mode: str = "paper",
    extra_cols: Optional[Dict[str, Any]] = None,
) -> None:
    """paper_positions tablosuna yeni pozisyon ekler."""

    row_data = {
        "market_id": market_id,
        "market_slug": market_slug,
        "question": question,
        "outcome": outcome,
        "token_id": token_id,
        "entry_price": entry_price,
        "size_usd": size_usd,
        "opened_ts": now_ts,
        "status": "OPEN",
        "high_price": entry_price,
        "order_id": order_id,
        "trading_mode": trading_mode,
    }

    if extra_cols:
        row_data.update(extra_cols)

    cols = list(row_data.keys())
    vals = [row_data[col] for col in cols]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    conn.execute(f"INSERT INTO paper_positions ({col_names}) VALUES ({placeholders})", vals)
    conn.commit()


def _update_position_closed(
    conn: sqlite3.Connection,
    pos_id: int,
    exit_price: float,
    pnl_usd: float,
    pnl_pct: float,
    now_ts: int,
    reason: str,
    order_id: str = "",
    trading_mode: str = "paper",
) -> None:
    """paper_positions tablosundaki pozisyonu kapatir."""

    # order_id ve fill_price sutunlari varsa guncelle
    try:
        conn.execute(
            "UPDATE paper_positions SET closed_ts=?, exit_price=?, pnl_usd=?, pnl_pct=?, "
            "close_reason=?, status='CLOSED', order_id=COALESCE(order_id, ?), "
            "trading_mode=?, fill_price=? WHERE id=?",
            (now_ts, exit_price, pnl_usd, pnl_pct, reason, order_id, trading_mode, exit_price, pos_id),
        )
    except sqlite3.OperationalError:
        # Eski sema -- order_id/fill_price sutunlari yok
        conn.execute(
            "UPDATE paper_positions SET closed_ts=?, exit_price=?, pnl_usd=?, pnl_pct=?, "
            "close_reason=?, status='CLOSED' WHERE id=?",
            (now_ts, exit_price, pnl_usd, pnl_pct, reason, pos_id),
        )
    conn.commit()
