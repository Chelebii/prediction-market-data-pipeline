"""Create rolling SQLite backups for the BTC5M dataset."""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.btc5m_dataset_db import resolve_db_path

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

BACKUP_DIR = Path(os.getenv("BTC5M_BACKUP_DIR", ROOT_DIR / "runtime" / "backups"))
BACKUP_LOG_PATH = Path(os.getenv("BTC5M_BACKUP_LOG_PATH", ROOT_DIR / "runtime" / "logs" / "btc5m_backup_dataset.log"))
KEEP_COUNT = max(1, int(os.getenv("BTC5M_BACKUP_KEEP_COUNT", "72")))

LOGGER = logging.getLogger("btc5m_backup_dataset")
LOGGER.setLevel(logging.INFO)
LOGGER.handlers.clear()
BACKUP_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-BACKUP | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
LOGGER.addHandler(_console)
_file_handler = RotatingFileHandler(BACKUP_LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("[%(asctime)s] BTC5M-BACKUP | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
LOGGER.addHandler(_file_handler)


def log(message: str) -> None:
    LOGGER.info(message)


def prune_old_backups() -> None:
    backups = sorted(BACKUP_DIR.glob("btc5m_dataset_*.db"))
    if len(backups) <= KEEP_COUNT:
        return
    for path in backups[: len(backups) - KEEP_COUNT]:
        try:
            path.unlink()
            log(f"PRUNE | removed={path.name}")
        except OSError as exc:
            log(f"WARN prune_failed | file={path.name} | reason={exc}")


def main() -> None:
    db_path = resolve_db_path()
    if not db_path.exists():
        log(f"WARN source_db_missing | db={db_path}")
        return

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    backup_path = BACKUP_DIR / f"btc5m_dataset_{ts}.db"

    src = sqlite3.connect(db_path)
    dest = sqlite3.connect(backup_path)
    try:
        src.backup(dest)
    finally:
        dest.close()
        src.close()

    size_bytes = backup_path.stat().st_size if backup_path.exists() else 0
    log(f"BACKUP | file={backup_path.name} | size={size_bytes} | source={db_path}")
    prune_old_backups()


if __name__ == "__main__":
    main()
