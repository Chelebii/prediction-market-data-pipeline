"""Create rolling SQLite backups for the BTC5M dataset."""

from __future__ import annotations

import json
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

from common.btc5m_dataset_db import resolve_db_path, resolve_repo_path

load_dotenv(ROOT_DIR / "polymarket_scanner" / ".env")
load_dotenv()

BACKUP_DIR = resolve_repo_path(
    os.getenv("BTC5M_BACKUP_DIR"),
    default_path=ROOT_DIR / "runtime" / "backups",
)
BACKUP_LOG_PATH = resolve_repo_path(
    os.getenv("BTC5M_BACKUP_LOG_PATH"),
    default_path=ROOT_DIR / "runtime" / "logs" / "btc5m_backup_dataset.log",
)
KEEP_COUNT = max(1, int(os.getenv("BTC5M_BACKUP_KEEP_COUNT", "72")))
VALIDATE_MODE = str(os.getenv("BTC5M_BACKUP_VALIDATE_MODE", "quick_check")).strip().lower() or "quick_check"
LATEST_METADATA_PATH = resolve_repo_path(
    os.getenv("BTC5M_BACKUP_LATEST_METADATA_PATH"),
    default_path=ROOT_DIR / "runtime" / "backups" / "btc5m_backup_latest.json",
)

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


def atomic_write_text(path: Path, payload: str) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(path)


def backup_meta_path(backup_path: Path) -> Path:
    return backup_path.with_suffix(".meta.json")


def validate_backup(backup_path: Path) -> tuple[bool, str]:
    if not backup_path.exists():
        return False, "backup_file_missing"
    if VALIDATE_MODE == "none":
        return True, "validation_skipped"

    conn = sqlite3.connect(backup_path)
    try:
        pragma_name = "quick_check" if VALIDATE_MODE == "quick_check" else "integrity_check"
        row = conn.execute(f"PRAGMA {pragma_name}(1)").fetchone()
        result = str(row[0]).strip() if row and row[0] is not None else ""
    finally:
        conn.close()

    if result.lower() == "ok":
        return True, f"{pragma_name}:ok"
    return False, f"{pragma_name}:{result or 'empty_result'}"


def write_backup_metadata(backup_path: Path, metadata: dict[str, object]) -> None:
    atomic_write_text(
        backup_meta_path(backup_path),
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
    )
    atomic_write_text(
        LATEST_METADATA_PATH,
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
    )


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
        meta_path = backup_meta_path(path)
        if meta_path.exists():
            try:
                meta_path.unlink()
                log(f"PRUNE | removed={meta_path.name}")
            except OSError as exc:
                log(f"WARN prune_failed | file={meta_path.name} | reason={exc}")


def main() -> None:
    db_path = resolve_db_path()
    if not db_path.exists():
        log(f"WARN source_db_missing | db={db_path}")
        return

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%SZ", time.gmtime())
    backup_path = BACKUP_DIR / f"btc5m_dataset_{ts}.db"
    temp_backup_path = BACKUP_DIR / f"btc5m_dataset_{ts}.tmp.db"
    if temp_backup_path.exists():
        temp_backup_path.unlink(missing_ok=True)

    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    dest = sqlite3.connect(temp_backup_path)
    try:
        src.backup(dest)
    finally:
        dest.close()
        src.close()

    ok, validation_result = validate_backup(temp_backup_path)
    if not ok:
        temp_backup_path.unlink(missing_ok=True)
        log(f"ERROR validation_failed | file={temp_backup_path.name} | result={validation_result}")
        raise SystemExit(1)

    temp_backup_path.replace(backup_path)
    size_bytes = backup_path.stat().st_size if backup_path.exists() else 0
    metadata = {
        "backup_name": backup_path.name,
        "backup_path": str(backup_path),
        "created_ts_utc": int(time.time()),
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "size_bytes": int(size_bytes),
        "source_db_path": str(db_path),
        "source_db_size_bytes": int(db_path.stat().st_size) if db_path.exists() else None,
        "retention_keep_count": KEEP_COUNT,
        "validation_mode": VALIDATE_MODE,
        "validation_result": validation_result,
    }
    write_backup_metadata(backup_path, metadata)
    log(
        "BACKUP | file=%s | size=%s | validation=%s | latest_pointer=%s | source=%s"
        % (backup_path.name, size_bytes, validation_result, LATEST_METADATA_PATH.name, db_path)
    )
    prune_old_backups()


if __name__ == "__main__":
    main()
