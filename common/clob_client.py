"""
clob_client.py -- Polymarket CLOB API Wrapper
=============================================
py-clob-client SDK'sini sarmalar. Strateji mantigi YOKTUR.
Sadece order gonderme, iptal, bakiye sorgulama islemleri yapar.

Thread-safe: her bot kendi ClobClientManager instance'ini olusturur.
"""

import logging
import os
import requests
import sys
import time
import threading
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

# SDK imports
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    MarketOrderArgs,
    OrderType,
    OpenOrderParams,
)
from py_clob_client.order_builder.constants import BUY, SELL

# Internal imports
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from common.bot_notify import send_alert

logger = logging.getLogger("clob_client")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] [CLOB] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

# Retry config
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 4, 8]  # saniye


def _coerce_tick_size(value: Any) -> Optional[Decimal]:
    try:
        tick = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if tick <= 0:
        return None
    return tick


def _quantize_price(price: float, tick_size: Optional[float], side: str) -> float:
    tick = _coerce_tick_size(tick_size)
    if tick is None:
        return max(0.0, float(price))

    price_dec = Decimal(str(max(0.0, float(price))))
    steps = price_dec / tick
    rounding = ROUND_CEILING if str(side).lower() == "buy" else ROUND_FLOOR
    quantized = steps.to_integral_value(rounding=rounding) * tick
    return max(0.0, float(quantized))


class OrderResponse:
    """Standart order sonucu."""

    def __init__(self, success: bool, order_id: str = "", error: str = "",
                 status: str = "", raw: Optional[dict] = None):
        self.success = success
        self.order_id = order_id
        self.error = error
        self.status = status
        self.raw = raw or {}

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "order_id": self.order_id,
            "error": self.error,
            "status": self.status,
        }

    def __repr__(self):
        return f"OrderResponse(success={self.success}, order_id={self.order_id!r}, error={self.error!r})"


class ClobClientManager:
    """
    Polymarket CLOB client wrapper.
    Her bot instance'i kendi ClobClientManager'ini olusturur.
    Lazy-init: ilk API cagrisi yapilana kadar baglanti kurulmaz.
    """

    def __init__(
        self,
        private_key: str,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        funder_address: str = "",
        signature_type: int = 0,
        bot_label: str = "",
    ):
        self._private_key = private_key
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._funder_address = funder_address
        self._signature_type = signature_type
        self._bot_label = bot_label

        self._client: Optional[ClobClient] = None
        self._lock = threading.Lock()
        self._initialized = False

    # ------------------------------------------------------------------ init
    def _ensure_client(self) -> ClobClient:
        """Lazy initialization -- ilk kullarimda client'i baslat."""
        if self._initialized and self._client is not None:
            return self._client

        with self._lock:
            if self._initialized and self._client is not None:
                return self._client

            if not self._private_key:
                raise ValueError("CLOB client: private_key bos -- live trading icin gerekli.")

            logger.info("CLOB client baslatiliyor (signature_type=%d)...", self._signature_type)

            kwargs: Dict[str, Any] = {
                "host": CLOB_HOST,
                "chain_id": CHAIN_ID,
                "key": self._private_key,
                "signature_type": self._signature_type,
            }
            if self._funder_address:
                kwargs["funder"] = self._funder_address

            client = ClobClient(**kwargs)

            # API credentials
            if self._api_key and self._api_secret and self._api_passphrase:
                creds = ApiCreds(
                    api_key=self._api_key,
                    api_secret=self._api_secret,
                    api_passphrase=self._api_passphrase,
                )
                client.set_api_creds(creds)
                logger.info("CLOB client: mevcut API credentials kullaniliyor.")
            else:
                logger.info("CLOB client: API credentials turetiliyor (create_or_derive)...")
                try:
                    derived = client.create_or_derive_api_creds()
                    client.set_api_creds(derived)
                    logger.info(
                        "CLOB client: API credentials basariyla turetildi. "
                        "api_key=%s...", str(derived.api_key)[:8] if derived.api_key else "N/A"
                    )
                except Exception as e:
                    logger.error("CLOB client: API credential turetme hatasi: %s", e)
                    raise

            self._client = client
            self._initialized = True
            logger.info("CLOB client hazir.")
            return client

    # --------------------------------------------------------- retry wrapper
    def _retry(self, fn, description: str) -> Any:
        """Exponential backoff ile retry."""
        last_err = None
        for attempt in range(MAX_RETRIES):
            try:
                return fn()
            except Exception as e:
                last_err = e
                err_str = str(e)
                is_rate_limit = "429" in err_str or "rate" in err_str.lower()
                wait = RETRY_BACKOFF[attempt] if attempt < len(RETRY_BACKOFF) else RETRY_BACKOFF[-1]

                if is_rate_limit:
                    logger.warning(
                        "[%s] Rate limit (429) -- %ds bekleyip tekrar deneniyor (attempt %d/%d)",
                        description, wait, attempt + 1, MAX_RETRIES,
                    )
                else:
                    logger.warning(
                        "[%s] Hata: %s -- %ds bekleyip tekrar deneniyor (attempt %d/%d)",
                        description, e, wait, attempt + 1, MAX_RETRIES,
                    )
                time.sleep(wait)

        # Tum denemeler basarisiz
        logger.error("[%s] %d deneme sonrasi basarisiz: %s", description, MAX_RETRIES, last_err)
        send_alert(
            bot_label=self._bot_label or "CLOB",
            msg=f"CLOB API hatasi ({description}): {last_err}",
            level="ERROR",
            dedupe_seconds=300,
        )
        return None

    # --------------------------------------------------------- order methods
    def buy_token(
        self,
        token_id: str,
        amount_usd: float,
        price: float,
        tick_size: str = "0.01",
    ) -> OrderResponse:
        """Limit BUY order gonderir."""
        client = self._ensure_client()
        size = amount_usd / price if price > 0 else 0
        if size <= 0:
            return OrderResponse(success=False, error="Gecersiz size (price=0 veya amount=0)")

        def _do():
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=round(size, 4),
                side=BUY,
            )
            signed = client.create_order(order_args)
            resp = client.post_order(signed, OrderType.GTC)
            return resp

        logger.info("BUY order: token=%s, amount=$%.2f, price=%.4f, size=%.4f",
                     token_id[:12], amount_usd, price, size)
        result = self._retry(_do, f"buy_token({token_id[:12]})")
        return self._parse_order_response(result, "BUY")

    def sell_token(
        self,
        token_id: str,
        amount_usd: float,
        price: float,
        tick_size: str = "0.01",
    ) -> OrderResponse:
        """Limit SELL order gonderir."""
        client = self._ensure_client()
        size = amount_usd / price if price > 0 else 0
        if size <= 0:
            return OrderResponse(success=False, error="Gecersiz size (price=0 veya amount=0)")

        def _do():
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=round(size, 4),
                side=SELL,
            )
            signed = client.create_order(order_args)
            resp = client.post_order(signed, OrderType.GTC)
            return resp

        logger.info("SELL order: token=%s, amount=$%.2f, price=%.4f, size=%.4f",
                     token_id[:12], amount_usd, price, size)
        result = self._retry(_do, f"sell_token({token_id[:12]})")
        return self._parse_order_response(result, "SELL")

    def market_buy(
        self,
        token_id: str,
        amount_usd: float,
        worst_price: float = 0,
        tick_size: Optional[float] = None,
    ) -> OrderResponse:
        """Market BUY order (FOK) with worst-price protection."""
        client = self._ensure_client()
        if amount_usd <= 0:
            return OrderResponse(success=False, error="Gecersiz amount")

        capped_price = _quantize_price(worst_price or 0.0, tick_size, "buy")

        def _do():
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usd,
                side=BUY,
                price=capped_price,
            )
            signed = client.create_market_order(mo)
            resp = client.post_order(signed, OrderType.FOK)
            return resp

        logger.info(
            "MARKET BUY: token=%s, amount=$%.2f, worst_price=%.4f, tick_size=%s",
            token_id[:12],
            amount_usd,
            capped_price,
            tick_size,
        )
        result = self._retry(_do, f"market_buy({token_id[:12]})")
        return self._parse_order_response(result, "MARKET_BUY")

    def market_sell(
        self,
        token_id: str,
        shares: float,
        worst_price: float = 0,
        tick_size: Optional[float] = None,
    ) -> OrderResponse:
        """Market SELL order (FOK). `shares` Polymarket pay adedidir."""
        client = self._ensure_client()
        if shares <= 0:
            return OrderResponse(success=False, error="Gecersiz shares")

        floor_price = _quantize_price(worst_price or 0.0, tick_size, "sell")

        def _do():
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=shares,
                side=SELL,
                price=floor_price,
            )
            signed = client.create_market_order(mo)
            resp = client.post_order(signed, OrderType.FOK)
            return resp

        logger.info(
            "MARKET SELL: token=%s, shares=%.4f, worst_price=%.4f, tick_size=%s",
            token_id[:12],
            shares,
            floor_price,
            tick_size,
        )
        result = self._retry(_do, f"market_sell({token_id[:12]})")
        return self._parse_order_response(result, "MARKET_SELL")

    # ---------------------------------------------------------- query methods
    def get_open_orders(self) -> List[dict]:
        """Acik orderlari listeler."""
        client = self._ensure_client()

        def _do():
            return client.get_orders(OpenOrderParams())

        result = self._retry(_do, "get_open_orders")
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return []

    def get_order(self, order_id: str) -> Optional[dict]:
        """Tek bir orderin guncel durumunu getirir."""
        if not order_id:
            return None
        client = self._ensure_client()

        def _do():
            return client.get_order(order_id)

        result = self._retry(_do, f"get_order({order_id[:12]})")
        if isinstance(result, dict):
            return result
        return None

    def get_collateral_balance(self) -> Optional[float]:
        """
        Polymarket collateral (USDC/USDC.e) bakiyesini CLOB account context'inden okur.
        Browser wallet / proxy wallet modunda on-chain EOA bakiyesinden daha dogrudur.
        """
        client = self._ensure_client()

        def _do():
            return client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=self._signature_type,
                )
            )

        result = self._retry(_do, "get_collateral_balance")
        if not isinstance(result, dict):
            return None

        try:
            balance_raw = int(str(result.get("balance", "0") or "0"))
            return balance_raw / 1_000_000
        except (TypeError, ValueError):
            logger.warning("Collateral balance parse edilemedi: %s", result)
            return None

    def get_conditional_token_balance(self, token_id: str) -> Optional[float]:
        """
        Belirli bir outcome token icin mevcut conditional bakiye.
        Donen deger pay adedidir.
        """
        if not token_id:
            return None
        client = self._ensure_client()

        def _do():
            return client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                    signature_type=self._signature_type,
                )
            )

        result = self._retry(_do, f"get_conditional_balance({token_id[:12]})")
        if not isinstance(result, dict):
            return None

        try:
            balance_raw = int(str(result.get("balance", "0") or "0"))
            return balance_raw / 1_000_000
        except (TypeError, ValueError):
            logger.warning("Conditional balance parse edilemedi: %s", result)
            return None

    def token_has_orderbook(self, token_id: str) -> Optional[bool]:
        """
        Token icin aktif bir CLOB orderbook var mi?
        Expiry sonrasi 404/no-orderbook donuyorsa bot artik market_sell denememelidir.
        """
        if not token_id:
            return None

        try:
            resp = requests.get(
                f"{CLOB_HOST}/midpoint",
                params={"token_id": token_id},
                timeout=10,
            )
        except Exception as e:
            logger.warning("Orderbook kontrolu basarisiz (%s): %s", token_id[:12], e)
            return None

        if resp.status_code == 200:
            return True
        if resp.status_code == 404 and "No orderbook exists" in resp.text:
            return False

        logger.warning(
            "Orderbook kontrolu beklenmeyen cevap verdi (%s): status=%s body=%s",
            token_id[:12],
            resp.status_code,
            resp.text[:200],
        )
        return None

    def cancel_order(self, order_id: str) -> bool:
        """Tek bir orderi iptal eder."""
        client = self._ensure_client()

        def _do():
            return client.cancel(order_id)

        logger.info("Cancel order: %s", order_id)
        result = self._retry(_do, f"cancel_order({order_id[:12]})")
        if result is not None:
            logger.info("Order iptal edildi: %s", order_id)
            return True
        return False

    def cancel_all_orders(self) -> bool:
        """Tum acik orderlari iptal eder."""
        client = self._ensure_client()

        def _do():
            return client.cancel_all()

        logger.info("Cancel ALL orders")
        result = self._retry(_do, "cancel_all_orders")
        if result is not None:
            logger.info("Tum orderlar iptal edildi.")
            return True
        return False

    # -------------------------------------------------------- response parser
    def _parse_order_response(self, raw_resp: Any, op: str) -> OrderResponse:
        """CLOB API yanitini standart OrderResponse'a cevir."""
        if raw_resp is None:
            return OrderResponse(success=False, error=f"{op}: tum retry'lar basarisiz")

        # py-clob-client dict veya object donebilir
        if isinstance(raw_resp, dict):
            success = raw_resp.get("success", False)
            order_id = raw_resp.get("orderID", raw_resp.get("order_id", ""))
            error_msg = raw_resp.get("errorMsg", raw_resp.get("error", ""))
            status = raw_resp.get("status", "")
            return OrderResponse(
                success=bool(success),
                order_id=str(order_id or ""),
                error=str(error_msg or ""),
                status=str(status or ""),
                raw=raw_resp,
            )

        # Bazi versiyonlar string donebilir
        resp_str = str(raw_resp)
        if "error" in resp_str.lower():
            return OrderResponse(success=False, error=resp_str)
        return OrderResponse(success=True, order_id=resp_str, raw={"raw": resp_str})

    # ------------------------------------------------------------ health check
    def health_check(self) -> bool:
        """CLOB API'ye basit bir ping atar."""
        try:
            client = self._ensure_client()
            ok = client.get_ok()
            return ok == "OK" or bool(ok)
        except Exception as e:
            logger.error("Health check basarisiz: %s", e)
            return False
