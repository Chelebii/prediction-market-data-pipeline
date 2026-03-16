"""SQLite helpers for the BTC 5MIN dataset."""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_DB_PATH = "BTC5M_DATASET_DB_PATH"
DEFAULT_DB_PATH = ROOT_DIR / "runtime" / "data" / "btc5m_dataset.db"

PRAGMA_STATEMENTS = (
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA foreign_keys=ON;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA cache_size=-20000;",
    "PRAGMA busy_timeout=5000;",
)

TABLE_SPECS: dict[str, dict[str, Any]] = {
    "btc5m_markets": {
        "columns": {
            "market_id": "TEXT PRIMARY KEY",
            "market_slug": "TEXT NOT NULL UNIQUE",
            "question": "TEXT NOT NULL",
            "slot_start_ts": "INTEGER NOT NULL",
            "slot_end_ts": "INTEGER NOT NULL",
            "yes_token_id": "TEXT NOT NULL",
            "no_token_id": "TEXT NOT NULL",
            "tick_size": "REAL",
            "min_order_size": "REAL",
            "resolution_source": "TEXT",
            "resolution_rule_text": "TEXT",
            "resolution_rule_version": "TEXT",
            "first_seen_ts": "INTEGER NOT NULL",
            "last_seen_ts": "INTEGER NOT NULL",
            "last_orderbook_seen_ts": "INTEGER",
            "created_at_ts": "INTEGER NOT NULL",
            "market_status": "TEXT NOT NULL DEFAULT 'ACTIVE'",
            "orderbook_exists_yes": "INTEGER NOT NULL DEFAULT 0",
            "orderbook_exists_no": "INTEGER NOT NULL DEFAULT 0",
            "market_resolution_status": "TEXT NOT NULL DEFAULT 'ACTIVE'",
            "resolved_outcome": "TEXT",
            "resolved_yes_price": "REAL",
            "resolved_no_price": "REAL",
            "resolved_ts": "INTEGER",
            "settled_ts": "INTEGER",
            "slot_start_reference_price": "REAL",
            "slot_end_reference_price": "REAL",
            "slot_start_reference_ts": "INTEGER",
            "slot_end_reference_ts": "INTEGER",
            "label_quality_flag": "TEXT",
            "notes": "TEXT",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_start_ts ON btc5m_markets(slot_start_ts)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_end_ts ON btc5m_markets(slot_end_ts)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_markets_status ON btc5m_markets(market_resolution_status)",
        ),
    },
    "btc5m_snapshots": {
        "columns": {
            "snapshot_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "market_slug": "TEXT NOT NULL",
            "collected_ts": "INTEGER NOT NULL",
            "written_ts": "INTEGER NOT NULL",
            "source_ts": "INTEGER",
            "seconds_to_resolution": "INTEGER NOT NULL",
            "best_bid_yes": "REAL",
            "best_ask_yes": "REAL",
            "best_bid_no": "REAL",
            "best_ask_no": "REAL",
            "mid_yes": "REAL",
            "mid_no": "REAL",
            "spread_yes": "REAL",
            "spread_no": "REAL",
            "best_bid_size_yes": "REAL",
            "best_ask_size_yes": "REAL",
            "best_bid_size_no": "REAL",
            "best_ask_size_no": "REAL",
            "liquidity_market": "REAL",
            "tick_size": "REAL",
            "min_order_size": "REAL",
            "complement_gap_mid": "REAL",
            "complement_gap_cross": "REAL",
            "price_mid_gap_yes_buy": "REAL",
            "price_mid_gap_yes_sell": "REAL",
            "price_mid_gap_no_buy": "REAL",
            "price_mid_gap_no_sell": "REAL",
            "quote_stable_pass_count": "INTEGER",
            "book_valid": "INTEGER NOT NULL",
            "market_status": "TEXT NOT NULL",
            "orderbook_exists_yes": "INTEGER NOT NULL",
            "orderbook_exists_no": "INTEGER NOT NULL",
            "publish_reason": "TEXT",
            "reject_reason": "TEXT",
            "source_name": "TEXT NOT NULL",
            "collector_latency_ms": "INTEGER",
            "reference_sync_gap_ms": "INTEGER",
            "snapshot_age_ms": "INTEGER",
            "meta_json": "TEXT",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_ts ON btc5m_snapshots(market_id, collected_ts)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_collected_ts ON btc5m_snapshots(collected_ts)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_status ON btc5m_snapshots(market_status)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_book_valid ON btc5m_snapshots(book_valid)",
        ),
    },
    "btc5m_orderbook_depth": {
        "columns": {
            "depth_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "collected_ts": "INTEGER NOT NULL",
            "yes_bid_depth_3": "REAL",
            "yes_ask_depth_3": "REAL",
            "no_bid_depth_3": "REAL",
            "no_ask_depth_3": "REAL",
            "yes_bid_depth_5": "REAL",
            "yes_ask_depth_5": "REAL",
            "no_bid_depth_5": "REAL",
            "no_ask_depth_5": "REAL",
            "yes_bid_depth_within_1c": "REAL",
            "yes_ask_depth_within_1c": "REAL",
            "no_bid_depth_within_1c": "REAL",
            "no_ask_depth_within_1c": "REAL",
            "yes_bid_depth_within_2c": "REAL",
            "yes_ask_depth_within_2c": "REAL",
            "no_bid_depth_within_2c": "REAL",
            "no_ask_depth_within_2c": "REAL",
            "yes_bid_depth_within_5c": "REAL",
            "yes_ask_depth_within_5c": "REAL",
            "no_bid_depth_within_5c": "REAL",
            "no_ask_depth_within_5c": "REAL",
            "source_name": "TEXT NOT NULL",
            "meta_json": "TEXT",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_orderbook_depth_market_ts ON btc5m_orderbook_depth(market_id, collected_ts)",
        ),
    },
    "btc5m_reference_ticks": {
        "columns": {
            "ref_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "ts_utc": "INTEGER NOT NULL",
            "source_name": "TEXT NOT NULL",
            "symbol": "TEXT NOT NULL",
            "btc_price": "REAL NOT NULL",
            "btc_bid": "REAL",
            "btc_ask": "REAL",
            "btc_mark_price": "REAL",
            "btc_index_price": "REAL",
            "volume_1s": "REAL",
            "latency_ms": "INTEGER",
            "meta_json": "TEXT",
        },
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_source_ts ON btc5m_reference_ticks(source_name, symbol, ts_utc)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_ts ON btc5m_reference_ticks(ts_utc)",
        ),
    },
    "btc5m_reference_1m_ohlcv": {
        "columns": {
            "candle_ts": "INTEGER PRIMARY KEY",
            "source_name": "TEXT NOT NULL",
            "symbol": "TEXT NOT NULL",
            "open": "REAL NOT NULL",
            "high": "REAL NOT NULL",
            "low": "REAL NOT NULL",
            "close": "REAL NOT NULL",
            "volume": "REAL",
            "trade_count": "INTEGER",
            "meta_json": "TEXT",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_btc5m_reference_1m_ohlcv_symbol_ts ON btc5m_reference_1m_ohlcv(symbol, candle_ts)",
        ),
    },
    "btc5m_lifecycle_events": {
        "columns": {
            "event_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "event_ts": "INTEGER NOT NULL",
            "event_type": "TEXT NOT NULL",
            "reason": "TEXT",
            "meta_json": "TEXT",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_market_ts ON btc5m_lifecycle_events(market_id, event_ts)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_event_type ON btc5m_lifecycle_events(event_type)",
        ),
    },
    "collector_runs": {
        "columns": {
            "run_id": "TEXT PRIMARY KEY",
            "started_ts": "INTEGER NOT NULL",
            "ended_ts": "INTEGER",
            "collector_name": "TEXT NOT NULL",
            "collector_version": "TEXT NOT NULL",
            "config_hash": "TEXT NOT NULL",
            "snapshot_count": "INTEGER NOT NULL DEFAULT 0",
            "market_count": "INTEGER NOT NULL DEFAULT 0",
            "reference_tick_count": "INTEGER NOT NULL DEFAULT 0",
            "error_count": "INTEGER NOT NULL DEFAULT 0",
            "status": "TEXT NOT NULL",
            "meta_json": "TEXT",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_collector_runs_status ON collector_runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_collector_runs_started_ts ON collector_runs(started_ts)",
        ),
    },
    "quality_audits": {
        "columns": {
            "audit_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "audit_ts": "INTEGER NOT NULL",
            "audit_date": "TEXT NOT NULL",
            "market_id": "TEXT",
            "run_id": "TEXT",
            "expected_snapshot_count": "INTEGER",
            "actual_snapshot_count": "INTEGER",
            "slot_coverage_ratio": "REAL",
            "max_gap_sec": "REAL",
            "invalid_book_ratio": "REAL",
            "structural_invalid_ratio": "REAL",
            "semantic_reject_ratio": "REAL",
            "duplicate_snapshot_ratio": "REAL",
            "missing_reference_ratio": "REAL",
            "missing_resolution_flag": "INTEGER NOT NULL DEFAULT 0",
            "reference_sync_gap_sec": "REAL",
            "audit_status": "TEXT NOT NULL",
            "notes": "TEXT",
        },
        "indexes": (
            "CREATE INDEX IF NOT EXISTS idx_quality_audits_audit_ts ON quality_audits(audit_ts)",
            "CREATE INDEX IF NOT EXISTS idx_quality_audits_market_id ON quality_audits(market_id)",
            "CREATE INDEX IF NOT EXISTS idx_quality_audits_run_id ON quality_audits(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_quality_audits_status ON quality_audits(audit_status)",
        ),
    },
    "btc5m_features": {
        "columns": {
            "feature_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "ts_utc": "INTEGER NOT NULL",
            "seconds_to_resolution": "INTEGER NOT NULL",
            "return_15s": "REAL",
            "return_30s": "REAL",
            "return_60s": "REAL",
            "return_120s": "REAL",
            "volatility_30s": "REAL",
            "volatility_60s": "REAL",
            "volatility_180s": "REAL",
            "microprice_yes": "REAL",
            "microprice_no": "REAL",
            "order_imbalance_yes": "REAL",
            "order_imbalance_no": "REAL",
            "complement_gap": "REAL",
            "spread_sum": "REAL",
            "depth_ratio_yes": "REAL",
            "depth_ratio_no": "REAL",
            "quote_stability_score": "REAL",
            "feature_version": "TEXT NOT NULL",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_features_market_ts_version ON btc5m_features(market_id, ts_utc, feature_version)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_features_version ON btc5m_features(feature_version)",
        ),
    },
    "btc5m_labels": {
        "columns": {
            "label_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "decision_ts": "INTEGER NOT NULL",
            "label_horizon_sec": "INTEGER NOT NULL",
            "terminal_outcome": "TEXT NOT NULL",
            "resolved_yes_price": "REAL",
            "resolved_no_price": "REAL",
            "mtm_return_if_buy_yes_hold_to_resolution": "REAL",
            "mtm_return_if_buy_no_hold_to_resolution": "REAL",
            "best_exit_yes_before_expiry": "REAL",
            "best_exit_no_before_expiry": "REAL",
            "would_hit_tp_5c": "INTEGER",
            "would_hit_tp_10c": "INTEGER",
            "would_hit_sl_5c": "INTEGER",
            "would_hit_sl_10c": "INTEGER",
            "time_to_best_yes_sec": "REAL",
            "time_to_best_no_sec": "REAL",
            "label_quality_flag": "TEXT NOT NULL",
            "label_version": "TEXT NOT NULL",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_labels_market_ts_version ON btc5m_labels(market_id, decision_ts, label_horizon_sec, label_version)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_labels_version ON btc5m_labels(label_version)",
        ),
    },
    "btc5m_decision_dataset": {
        "columns": {
            "row_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "market_id": "TEXT NOT NULL",
            "decision_ts": "INTEGER NOT NULL",
            "seconds_to_resolution": "INTEGER NOT NULL",
            "market_slug": "TEXT NOT NULL",
            "mid_yes": "REAL",
            "mid_no": "REAL",
            "spread_yes": "REAL",
            "spread_no": "REAL",
            "btc_price": "REAL",
            "quote_stability_score": "REAL",
            "terminal_outcome": "TEXT",
            "target_yes_hold": "REAL",
            "target_no_hold": "REAL",
            "label_quality_flag": "TEXT",
            "is_trainable": "INTEGER NOT NULL",
            "split_bucket": "TEXT NOT NULL",
            "dataset_version": "TEXT NOT NULL",
        },
        "table_constraints": (
            "FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)",
        ),
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_decision_dataset_market_ts_version ON btc5m_decision_dataset(market_id, decision_ts, dataset_version)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_decision_dataset_version ON btc5m_decision_dataset(dataset_version)",
            "CREATE INDEX IF NOT EXISTS idx_btc5m_decision_dataset_split_bucket ON btc5m_decision_dataset(split_bucket)",
        ),
    },
}


def default_db_path() -> Path:
    return DEFAULT_DB_PATH


def resolve_repo_path(
    path_value: Optional[os.PathLike[str] | str] = None,
    *,
    default_path: os.PathLike[str] | str,
    root_dir: Optional[os.PathLike[str] | str] = None,
) -> Path:
    raw_value = str(path_value).strip() if path_value is not None else ""
    candidate = Path(raw_value or str(default_path)).expanduser()
    base_dir = Path(root_dir).expanduser() if root_dir is not None else ROOT_DIR
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate


def resolve_db_path(db_path: Optional[os.PathLike[str] | str] = None) -> Path:
    if db_path is not None:
        raw_value = str(db_path)
    else:
        raw_value = str(os.getenv(ENV_DB_PATH, "")).strip() or str(DEFAULT_DB_PATH)
    raw_path = Path(raw_value)
    if not raw_path.is_absolute():
        raw_path = ROOT_DIR / raw_path
    return raw_path.expanduser()


def connect_db(db_path: Optional[os.PathLike[str] | str] = None) -> sqlite3.Connection:
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    for pragma in PRAGMA_STATEMENTS:
        conn.execute(pragma)
    migrate_schema(conn)
    return conn


def migrate_schema(conn: sqlite3.Connection) -> None:
    for table_name, spec in TABLE_SPECS.items():
        conn.execute(_build_create_table_sql(table_name, spec))
        _ensure_missing_columns(conn, table_name, spec["columns"])
        for index_sql in spec.get("indexes", ()):
            conn.execute(index_sql)
    conn.commit()


def create_run_id(collector_name: str = "btc5m-collector") -> str:
    ts = int(time.time())
    suffix = uuid.uuid4().hex[:8]
    return f"{collector_name}-{ts}-{suffix}"


def upsert_market(conn: sqlite3.Connection, market_row: Mapping[str, Any]) -> int:
    row = _prepare_row("btc5m_markets", market_row)
    columns = list(row.keys())
    if columns == ["market_id"]:
        cursor = conn.execute("INSERT OR IGNORE INTO btc5m_markets (market_id) VALUES (?)", (row["market_id"],))
        conn.commit()
        return cursor.rowcount
    placeholders = ", ".join("?" for _ in columns)
    update_sql = ", ".join(
        _market_upsert_assignment(column)
        for column in columns
        if column != "market_id"
    )
    sql = (
        f"INSERT INTO btc5m_markets ({', '.join(columns)}) VALUES ({placeholders}) "
        "ON CONFLICT(market_id) DO UPDATE SET "
        f"{update_sql}"
    )
    cursor = conn.execute(sql, tuple(row[column] for column in columns))
    conn.commit()
    return cursor.rowcount


def update_market(conn: sqlite3.Connection, market_id: str, updates: Mapping[str, Any]) -> int:
    prepared = {
        key: _normalize_value(value)
        for key, value in updates.items()
        if key in TABLE_SPECS["btc5m_markets"]["columns"] and key != "market_id"
    }
    if not prepared:
        return 0
    assignments = ", ".join(f"{column}=?" for column in prepared)
    values = list(prepared.values()) + [market_id]
    cursor = conn.execute(f"UPDATE btc5m_markets SET {assignments} WHERE market_id=?", values)
    conn.commit()
    return cursor.rowcount


def insert_snapshot(conn: sqlite3.Connection, snapshot_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "btc5m_snapshots", snapshot_row, or_ignore=True)


def insert_orderbook_depth(conn: sqlite3.Connection, depth_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "btc5m_orderbook_depth", depth_row, or_ignore=True)


def insert_reference_tick(conn: sqlite3.Connection, reference_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "btc5m_reference_ticks", reference_row, or_ignore=True)


def insert_reference_ohlcv(conn: sqlite3.Connection, candle_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "btc5m_reference_1m_ohlcv", candle_row, or_ignore=True)


def insert_lifecycle_event(conn: sqlite3.Connection, event_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "btc5m_lifecycle_events", event_row, or_ignore=False)


def insert_quality_audit(conn: sqlite3.Connection, audit_row: Mapping[str, Any]) -> int:
    return _insert_row(conn, "quality_audits", audit_row, or_ignore=False)


def start_collector_run(
    conn: sqlite3.Connection,
    run_row: Optional[Mapping[str, Any]] = None,
    **overrides: Any,
) -> str:
    payload = dict(run_row or {})
    payload.update(overrides)
    payload.setdefault("run_id", create_run_id(str(payload.get("collector_name", "btc5m-collector"))))
    payload.setdefault("started_ts", int(time.time()))
    payload.setdefault("collector_name", "btc5m-collector")
    payload.setdefault("collector_version", "dev")
    payload.setdefault("config_hash", "")
    payload.setdefault("snapshot_count", 0)
    payload.setdefault("market_count", 0)
    payload.setdefault("reference_tick_count", 0)
    payload.setdefault("error_count", 0)
    payload.setdefault("status", "RUNNING")
    _insert_row(conn, "collector_runs", payload, or_ignore=False)
    return str(payload["run_id"])


def finish_collector_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    ended_ts: Optional[int] = None,
    status: str = "COMPLETED",
    snapshot_count: Optional[int] = None,
    market_count: Optional[int] = None,
    reference_tick_count: Optional[int] = None,
    error_count: Optional[int] = None,
    meta_json: Any = None,
) -> int:
    updates: dict[str, Any] = {
        "ended_ts": ended_ts if ended_ts is not None else int(time.time()),
        "status": status,
    }
    if snapshot_count is not None:
        updates["snapshot_count"] = snapshot_count
    if market_count is not None:
        updates["market_count"] = market_count
    if reference_tick_count is not None:
        updates["reference_tick_count"] = reference_tick_count
    if error_count is not None:
        updates["error_count"] = error_count
    if meta_json is not None:
        updates["meta_json"] = meta_json
    return update_collector_run(conn, run_id, updates)


def update_collector_run(conn: sqlite3.Connection, run_id: str, updates: Mapping[str, Any]) -> int:
    prepared = {key: _normalize_value(value) for key, value in updates.items() if key in TABLE_SPECS["collector_runs"]["columns"]}
    if not prepared:
        return 0
    assignments = ", ".join(f"{column}=?" for column in prepared)
    values = list(prepared.values()) + [run_id]
    cursor = conn.execute(f"UPDATE collector_runs SET {assignments} WHERE run_id=?", values)
    conn.commit()
    return cursor.rowcount


def _build_create_table_sql(table_name: str, spec: Mapping[str, Any]) -> str:
    column_sql = [f"{column} {ddl}" for column, ddl in spec["columns"].items()]
    column_sql.extend(spec.get("table_constraints", ()))
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_sql)})"


def _ensure_missing_columns(conn: sqlite3.Connection, table_name: str, columns: Mapping[str, str]) -> None:
    existing = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column, ddl in columns.items():
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {ddl}")


def _insert_row(
    conn: sqlite3.Connection,
    table_name: str,
    row: Mapping[str, Any],
    *,
    or_ignore: bool,
) -> int:
    prepared = _prepare_row(table_name, row)
    columns = list(prepared.keys())
    placeholders = ", ".join("?" for _ in columns)
    modifier = " OR IGNORE" if or_ignore else ""
    sql = f"INSERT{modifier} INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor = conn.execute(sql, tuple(prepared[column] for column in columns))
    conn.commit()
    return cursor.rowcount


def _prepare_row(table_name: str, row: Mapping[str, Any]) -> dict[str, Any]:
    allowed = TABLE_SPECS[table_name]["columns"]
    prepared = {
        key: _normalize_value(value)
        for key, value in row.items()
        if key in allowed
    }
    if not prepared:
        raise ValueError(f"No valid columns supplied for table {table_name}")
    return prepared


def _market_upsert_assignment(column: str) -> str:
    if column == "first_seen_ts":
        return "first_seen_ts=MIN(btc5m_markets.first_seen_ts, excluded.first_seen_ts)"
    if column == "last_seen_ts":
        return "last_seen_ts=MAX(btc5m_markets.last_seen_ts, excluded.last_seen_ts)"
    if column == "last_orderbook_seen_ts":
        return (
            "last_orderbook_seen_ts="
            "CASE "
            "WHEN excluded.last_orderbook_seen_ts IS NULL THEN btc5m_markets.last_orderbook_seen_ts "
            "WHEN btc5m_markets.last_orderbook_seen_ts IS NULL THEN excluded.last_orderbook_seen_ts "
            "ELSE MAX(btc5m_markets.last_orderbook_seen_ts, excluded.last_orderbook_seen_ts) "
            "END"
        )
    if column == "created_at_ts":
        return "created_at_ts=btc5m_markets.created_at_ts"
    return f"{column}=COALESCE(excluded.{column}, btc5m_markets.{column})"


def _normalize_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return value
