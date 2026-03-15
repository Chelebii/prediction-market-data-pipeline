import json
import msvcrt
import os
import tempfile
import time
import psutil
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_DIR = os.path.join(ROOT_DIR, "state")
REGISTRY_PATH = os.path.join(STATE_DIR, "active_runs.json")
REGISTRY_LOCK_PATH = os.path.join(STATE_DIR, "active_runs.lock")
MANAGER_LOCK_PATH = os.path.join(ROOT_DIR, "polymarket_paper_bot_5min", "manager.lock")

BOT_KEYS = ["5min"]
VALID_STATUSES = {"CREATED", "ACTIVE", "DRAINING", "CLOSED", "ARCHIVED", "STOPPED"}
ALLOWED_TRANSITIONS = {
    "CREATED": {"ACTIVE", "CLOSED"},
    "ACTIVE": {"ACTIVE", "DRAINING", "CLOSED", "STOPPED"},
    "DRAINING": {"DRAINING", "CLOSED", "STOPPED"},
    "CLOSED": {"CLOSED", "ARCHIVED", "ACTIVE", "STOPPED"},
    "ARCHIVED": {"ARCHIVED", "CREATED"},
    # Legacy compatibility: older flows persisted STOPPED in registry.
    "STOPPED": {"STOPPED", "ACTIVE", "CLOSED", "ARCHIVED"},
}

_LOCK_TIMEOUT = 5.0   # saniye -- bu sureyi asarsa uyar ve devam et
_LOCK_RETRY_MS = 50   # ms -- lock beklerken yeniden deneme araligi


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _default_registry() -> dict:
    return {
        "version": 1,
        "updated_at": _now(),
        "bots": {
            k: {
                "active_run_id": None,
                "run_dir": None,
                "status": "CREATED",
                "last_switch_at": None,
                "last_heartbeat_at": None,
            }
            for k in BOT_KEYS
        },
    }


# --- File Lock (Windows msvcrt, stdlib only) ----------------------------------

class _FileLock:
    """
    Windows uyumlu basit file lock.
    Harici paket gerektirmez -- sadece msvcrt ve stdlib kullanir.

    Timeout durumunda kilidi alamasa bile context'e girer ve
    release() guvenli sekilde calisir (self._locked flag'i ile kontrol edilir).
    """

    def __init__(self, lock_path: str, timeout: float = _LOCK_TIMEOUT):
        self._path = lock_path
        self._timeout = timeout
        self._fh = None
        self._locked = False  # Gercekten lock alindi mi?

    def acquire(self):
        import logging
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        deadline = time.monotonic() + self._timeout

        while True:
            try:
                fh = open(self._path, "w", encoding="utf-8")
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                self._fh = fh
                self._locked = True
                return  # basarili
            except OSError:
                try:
                    fh.close()
                except Exception:
                    pass

                if time.monotonic() >= deadline:
                    logging.warning(
                        "[run_registry] Lock timeout (%.1fs) -- "
                        "devam ediliyor, race condition riski var.", self._timeout
                    )
                    self._locked = False
                    return  # lock alinamadi ama devam et

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


def _get_lock() -> _FileLock:
    return _FileLock(REGISTRY_LOCK_PATH, timeout=_LOCK_TIMEOUT)


# --- Registry Okuma / Yazma ---------------------------------------------------

def _raw_load(path: str) -> dict:
    """Lock almadan dosyayi oku. Sadece lock icinden cagir."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _raw_save(data: dict, path: str):
    """
    Lock altinda tum registry'yi yazar (atomic write).
    global updated_at guncellenir.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data["updated_at"] = _now()
    dir_ = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix="active_runs_", suffix=".tmp", dir=dir_)
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


def ensure_registry(path: str = REGISTRY_PATH):
    """Registry dosyasi yoksa olustur. Lock altinda calisir."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with _get_lock():
            # Tekrar kontrol et (double-checked locking)
            if not os.path.exists(path):
                _raw_save(_default_registry(), path)


def load_registry(path: str = REGISTRY_PATH) -> dict:
    """
    Registry'yi lock altinda okur.
    Yazma sirasinda okuma yapilmasini engeller.
    """
    ensure_registry(path)
    with _get_lock():
        return _raw_load(path)


def save_registry(data: dict, path: str = REGISTRY_PATH):
    """
    Tum registry'yi lock altinda yazar.
    Disaridan dogrudan cagirilmasi ONERILMEZ; _update_bot_field kullan.
    """
    with _get_lock():
        _raw_save(data, path)


def _update_bot_field(bot_key: str, updater_fn, path: str = REGISTRY_PATH):
    """
    Lock altinda yalnizca bot_key'e ait alanlari gunceller.
    Diger botlara KESINLIKLE DOKUNMAZ.

    updater_fn(bot_dict) seklinde cagrilir; don?? degeri beklenmez,
    bot_dict in-place guncellenir.
    """
    ensure_registry(path)
    with _get_lock():
        data = _raw_load(path)
        bot = get_bot(data, bot_key)
        updater_fn(bot)
        _raw_save(data, path)


# --- Bot Dict Yardimcisi ------------------------------------------------------

def get_bot(data: dict, bot_key: str) -> dict:
    """
    data icinden bot_key sozlugunu dondurur.
    Yoksa varsayilan degerlerle olusturur.
    NOT: Bu fonksiyon lock ALMAZ; lock altinda cagrilmasi gereken
    ic yardimci bir fonksiyondur.
    """
    return data.setdefault("bots", {}).setdefault(
        bot_key,
        {
            "active_run_id": None,
            "run_dir": None,
            "status": "CREATED",
            "last_switch_at": None,
            "last_heartbeat_at": None,
        },
    )


# --- Public API (imzalar degismedi -- geriye uyumlu) --------------------------

def get_active_run(bot_key: str, path: str = REGISTRY_PATH) -> str | None:
    """Bot'un aktif run dizinini dondurur. Lock altinda okur."""
    ensure_registry(path)
    with _get_lock():
        data = _raw_load(path)
    bot = get_bot(data, bot_key)
    return bot.get("run_dir")


def get_status(bot_key: str, path: str = REGISTRY_PATH) -> str:
    """Bot'un guncel statusunu dondurur. Lock altinda okur."""
    ensure_registry(path)
    with _get_lock():
        data = _raw_load(path)
    bot = get_bot(data, bot_key)
    return bot.get("status") or "CREATED"


def _validate_transition(from_status: str, to_status: str):
    if to_status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {to_status}")
    allowed = ALLOWED_TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise ValueError(f"invalid transition: {from_status} -> {to_status}")


def set_active_run(
    bot_key: str,
    run_dir: str,
    status: str = "ACTIVE",
    path: str = REGISTRY_PATH,
):
    """
    Sadece bot_key'in run_dir, status ve last_switch_at alanlarini gunceller.
    Diger botlara DOKUNMAZ. Lock altinda calisir.
    """
    def _update(bot):
        current = bot.get("status") or "CREATED"
        _validate_transition(current, status)
        run_id = os.path.basename(run_dir.rstrip("\\/")) if run_dir else None
        bot["active_run_id"] = run_id
        bot["run_dir"] = run_dir
        bot["status"] = status
        bot["last_switch_at"] = _now()

    _update_bot_field(bot_key, _update, path)


def set_status(bot_key: str, status: str, path: str = REGISTRY_PATH):
    """
    Sadece bot_key'in status alanini gunceller.
    Diger botlara DOKUNMAZ. Lock altinda calisir.
    """
    def _update(bot):
        current = bot.get("status") or "CREATED"
        _validate_transition(current, status)
        bot["status"] = status

    _update_bot_field(bot_key, _update, path)


def touch_heartbeat(bot_key: str, path: str = REGISTRY_PATH):
    """
    Sadece bot_key'in last_heartbeat_at alanini gunceller.
    Diger botlarin last_heartbeat_at degeri DEGISMEZ. Lock altinda calisir.
    """
    def _update(bot):
        bot["last_heartbeat_at"] = _now()

    _update_bot_field(bot_key, _update, path)


def set_trading_mode(bot_key: str, mode: str, path: str = REGISTRY_PATH):
    """
    Sadece bot_key'in trading_mode alanini gunceller.
    Diger botlara DOKUNMAZ. Lock altinda calisir.
    """
    def _update(bot):
        bot["trading_mode"] = mode

    _update_bot_field(bot_key, _update, path)


def get_trading_mode(bot_key: str, path: str = REGISTRY_PATH) -> str:
    """Bot'un trading mode'unu dondurur. Lock altinda okur."""
    ensure_registry(path)
    with _get_lock():
        data = _raw_load(path)
    bot = get_bot(data, bot_key)
    return bot.get("trading_mode") or "paper"


def status_rows(path: str = REGISTRY_PATH) -> list[dict]:
    """Tum botlarin ozet satirlarini dondurur. Lock altinda okur."""
    ensure_registry(path)
    with _get_lock():
        data = _raw_load(path)
    rows = []
    for k in BOT_KEYS:
        b = get_bot(data, k)
        rows.append(
            {
                "bot": k,
                "status": b.get("status"),
                "active_run_id": b.get("active_run_id"),
                "run_dir": b.get("run_dir"),
                "last_switch_at": b.get("last_switch_at"),
                "last_heartbeat_at": b.get("last_heartbeat_at"),
                "trading_mode": b.get("trading_mode") or "paper",
            }
        )
    return rows


# --- Manager Lock & PID Control -----------------------------------------------

def check_manager_lock(lock_path: str = MANAGER_LOCK_PATH) -> bool:
    """
    Manager kilidini kontrol eder. 
    Eger kilitteki PID calismiyorsa kilidi siler ve True (baslatilabilir) doner.
    Calisiyorsa False (zaten aktif) doner.
    """
    if not os.path.exists(lock_path):
        return True
    
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                # Bos dosya ise sil ve devam et
                os.remove(lock_path)
                return True
            pid = int(content)
            
        if psutil.pid_exists(pid):
            # PID hala hayatta
            return False
        else:
            # PID olu, kilidi temizle
            os.remove(lock_path)
            return True
            
    except (ValueError, OSError):
        # Okunamazsa veya hataliysa silmeyi dene
        try:
            os.remove(lock_path)
        except Exception:
            pass
        return True


# --- Cleanup & Heartbeat Maintenance ------------------------------------------

def cleanup_stale_runs(timeout_seconds: int = 300, path: str = REGISTRY_PATH):
    """
    Son heartbeat degeri timeout suresinden eski olan botlari 
    DRAINING veya CLOSED statusune gecirir ve active_run_id'yi temizler.
    """
    ensure_registry(path)
    now_dt = datetime.now()
    
    with _get_lock():
        data = _raw_load(path)
        changed = False
        
        for bot_key, bot in data.get("bots", {}).items():
            hb_str = bot.get("last_heartbeat_at")
            if not hb_str or not bot.get("active_run_id"):
                continue
                
            try:
                hb_dt = datetime.fromisoformat(hb_str)
                diff = (now_dt - hb_dt).total_seconds()
                
                if diff > timeout_seconds:
                    # Bot bayatlamis (stale)
                    bot["status"] = "CLOSED"  # Veya DRAINING? CLOSED daha kesin temizlik saglar.
                    bot["active_run_id"] = None
                    bot["run_dir"] = None
                    bot["last_switch_at"] = _now()
                    changed = True
            except (ValueError, TypeError):
                continue
                
        if changed:
            _raw_save(data, path)
