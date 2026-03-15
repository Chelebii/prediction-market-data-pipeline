"""
safety.py -- Live Trading Guvenlik Katmani
==========================================
Kill switch, gunluk kayip limiti, position size guard, ilk trade onayi.
Bu modul fonlari korur -- tum live islemler buradan gecmek ZORUNDADIR.

State dosyasi: xPolymarketBots/state/safety_state.json
File lock: run_registry.py'deki _FileLock pattern'i takip eder.
"""

import json
import logging
import msvcrt
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.bot_notify import send_alert

logger = logging.getLogger("safety")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] [SAFETY] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)

STATE_DIR = os.path.join(str(ROOT_DIR), "state")
DEFAULT_STATE_PATH = os.path.join(STATE_DIR, "safety_state.json")
LOCK_PATH = os.path.join(STATE_DIR, "safety_state.lock")

BOT_KEYS = ["5min"]

_LOCK_TIMEOUT = 5.0
_LOCK_RETRY_MS = 50


# --- File Lock (run_registry.py pattern) --------------------------------------

class _FileLock:
    """Windows uyumlu basit file lock (msvcrt)."""

    def __init__(self, lock_path: str, timeout: float = _LOCK_TIMEOUT):
        self._path = lock_path
        self._timeout = timeout
        self._fh = None
        self._locked = False

    def acquire(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                fh = open(self._path, "w", encoding="utf-8")
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                self._fh = fh
                self._locked = True
                return
            except OSError:
                try:
                    fh.close()
                except Exception:
                    pass
                if time.monotonic() >= deadline:
                    self._locked = False
                    return
                time.sleep(_LOCK_RETRY_MS / 1000.0)

    def release(self):
        if not self._locked or self._fh is None:
            return
        try:
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None
        self._locked = False
        try:
            os.remove(self._path)
        except Exception:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()


# --- State Helpers ------------------------------------------------------------

def _default_state() -> dict:
    return {
        "kill_switch": False,
        "kill_switch_reason": None,
        "kill_switch_at": None,
        "daily_pnl": {},
        "first_trade_completed": {k: False for k in BOT_KEYS},
    }


def _load_state(path: str) -> dict:
    """State dosyasini lock altinda okur."""
    if not os.path.exists(path):
        return _default_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Eksik alanlari tamamla
        default = _default_state()
        for k, v in default.items():
            if k not in data:
                data[k] = v
        return data
    except Exception as e:
        logger.error("State dosyasi okunamadi: %s -- varsayilan kullaniliyor.", e)
        return _default_state()


def _save_state(data: dict, path: str) -> None:
    """State dosyasini atomic write ile kaydeder."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    dir_ = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix="safety_", suffix=".tmp", dir=dir_)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# --- SafetyManager -----------------------------------------------------------

class SafetyManager:
    """
    Live trading guvenlik yoneticisi.

    - Kill switch: tum live botlari durdurur
    - Gunluk kayip limiti: bot bazinda
    - Position size guard
    - Ilk trade onayi
    """

    def __init__(self, state_path: Optional[str] = None, bot_label: str = ""):
        self._path = state_path or DEFAULT_STATE_PATH
        self._bot_label = bot_label

    def _lock(self) -> _FileLock:
        return _FileLock(LOCK_PATH, timeout=_LOCK_TIMEOUT)

    def _read(self) -> dict:
        with self._lock():
            return _load_state(self._path)

    def _write(self, data: dict) -> None:
        with self._lock():
            _save_state(data, self._path)

    def _update(self, updater_fn) -> None:
        """Lock altinda oku -> guncelle -> yaz."""
        with self._lock():
            data = _load_state(self._path)
            updater_fn(data)
            _save_state(data, self._path)

    # -- Kill Switch -------------------------------------------------------

    def is_kill_switch_active(self) -> bool:
        """Kill switch aktif mi?"""
        data = self._read()
        return bool(data.get("kill_switch", False))

    def activate_kill_switch(self, reason: str = "") -> None:
        """Kill switch'i aktifle. TUM live botlar durur."""
        def _update(data):
            data["kill_switch"] = True
            data["kill_switch_reason"] = reason
            data["kill_switch_at"] = datetime.now(timezone.utc).isoformat()

        self._update(_update)
        logger.warning(" KILL SWITCH AKTIF -- Sebep: %s", reason)

    def deactivate_kill_switch(self) -> None:
        """Kill switch'i deaktifle."""
        def _update(data):
            data["kill_switch"] = False
            data["kill_switch_reason"] = None
            data["kill_switch_at"] = None

        self._update(_update)
        logger.info(" Kill switch deaktif edildi.")

    # -- Gunluk Kayip Limiti -----------------------------------------------

    def check_daily_limit(self, bot_key: str, pending_pnl: float = 0.0,
                          limit: float = 20.0) -> bool:
        """
        Gunluk kayip limitini kontrol eder.
        True = devam edebilir, False = limit asildi.
        """
        data = self._read()
        daily = data.get("daily_pnl", {})
        bot_daily = daily.get(bot_key, {})
        today = _today_str()

        # Farkli gun -- sifirla
        if bot_daily.get("date") != today:
            return True

        total = bot_daily.get("total_pnl", 0.0) + pending_pnl
        if total <= -abs(limit):
            logger.warning(
                "[%s] Gunluk kayip limiti asildi: $%.2f (limit: -$%.2f)",
                bot_key, total, limit,
            )
            return False

        return True

    def record_trade_pnl(self, bot_key: str, pnl_usd: float) -> None:
        """Trade PnL'ini gunluk kayda ekler."""
        today = _today_str()

        def _update(data):
            daily = data.setdefault("daily_pnl", {})
            bot_daily = daily.get(bot_key, {})

            if bot_daily.get("date") != today:
                bot_daily = {"date": today, "total_pnl": 0.0, "trade_count": 0}

            bot_daily["total_pnl"] = bot_daily.get("total_pnl", 0.0) + pnl_usd
            bot_daily["trade_count"] = bot_daily.get("trade_count", 0) + 1
            daily[bot_key] = bot_daily

        self._update(_update)
        logger.info("[%s] Trade PnL kaydedildi: $%+.2f", bot_key, pnl_usd)

    def get_daily_pnl(self, bot_key: str) -> float:
        """Bugunun toplam PnL'ini doner."""
        data = self._read()
        daily = data.get("daily_pnl", {})
        bot_daily = daily.get(bot_key, {})
        today = _today_str()

        if bot_daily.get("date") != today:
            return 0.0
        return bot_daily.get("total_pnl", 0.0)

    # -- Position Size Guard -----------------------------------------------

    def validate_position_size(self, size_usd: float, max_allowed: float) -> bool:
        """Position size limitini kontrol eder."""
        if size_usd > max_allowed:
            logger.warning(
                "Position size reddedildi: $%.2f > max $%.2f",
                size_usd, max_allowed,
            )
            return False
        return True

    # -- Ilk Trade Onayi ---------------------------------------------------

    def is_first_live_trade(self, bot_key: str) -> bool:
        """Bu bot daha once live trade yapti mi?"""
        data = self._read()
        completed = data.get("first_trade_completed", {})
        return not completed.get(bot_key, False)

    def mark_first_trade_completed(self, bot_key: str) -> None:
        """Ilk live trade tamamlandi olarak isaretle."""
        def _update(data):
            completed = data.setdefault("first_trade_completed", {})
            completed[bot_key] = True

        self._update(_update)
        logger.info("[%s] Ilk live trade tamamlandi olarak isaretlendi.", bot_key)

    # -- Durum Raporu ------------------------------------------------------

    def get_status_summary(self) -> dict:
        """Tum guvenlik durumunu ozet olarak doner."""
        data = self._read()
        return {
            "kill_switch": data.get("kill_switch", False),
            "kill_switch_reason": data.get("kill_switch_reason"),
            "daily_pnl": data.get("daily_pnl", {}),
            "first_trade_completed": data.get("first_trade_completed", {}),
        }
