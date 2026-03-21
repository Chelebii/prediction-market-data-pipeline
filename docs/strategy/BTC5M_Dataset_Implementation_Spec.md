# BTC5M Dataset Implementation Spec

## 1. Objective

This document turns the dataset target defined in [Backtest_Data_Collection_Plan.md](Backtest_Data_Collection_Plan.md) into an implementable plan.

This document covers:

- concrete SQLite schema
- collector implementation task list
- label ETL checklist
- validation and acceptance criteria

Focus:

- BTC 5-minute up/down only
- raw-first dataset
- reusable pipeline for backtesting and ML

---

## 2. Design Principles

### 2.1 Raw Data Must Remain Immutable

Raw tables written by the collector must not be mutated later.
If incorrect data must be corrected:

- a new audit record should be created
- a new derived table should be generated
- the raw table should not be overwritten

### 2.2 Official Resolution Must Not Be Separated from the Label

`resolved_outcome` and settlement status must come from the official market result.
The reference exchange price should only be used for:

- features
- debugging
- reconciliation

### 2.3 There Must Be No State Loss

The dataset must store not only "good quote" moments, but also state transitions:

- discovered
- publishable
- rejected
- expired
- pending settlement
- resolved

### 2.4 Derived Data Must Always Be Rebuildable from Raw Data

Feature and label tables should be treated as disposable.

---

## 3. Recommended File / Module Layout

The following distribution fits the current repository structure:

- `polymarket_scanner/btc_5min_clob_scanner.py`
  Collector main loop. Discovery, quote fetch, validation, DB write trigger.

- `common/btc5m_dataset_db.py`
  SQLite connection, schema migration, insert helpers, upsert helpers.

- `common/btc5m_reference_feed.py`
  BTC reference tick fetch and normalization helper.

- `scripts/btc5m_build_labels.py`
  Label generation from the resolution table.

- `scripts/btc5m_build_features.py`
  Feature generation from raw snapshots and reference data.

- `scripts/btc5m_build_decision_dataset.py`
  Produces the final research dataset by joining features and labels.

- `scripts/btc5m_audit_dataset.py`
  Coverage, gap, missing-label, and invalid-ratio audit.

This separation keeps the scanner code simple and separates ETL logic from the runtime collector.

---

## 4. SQLite Schema

## 4.1 Recommended DB File

Start with a single DB:

```text
runtime/data/btc5m_dataset.db
```

WAL should be enabled.
The collector and offline ETL should be able to read at the same time.

## 4.2 PRAGMA Settings

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -20000;
```

## 4.3 Raw Layer DDL

### `btc5m_markets`

```sql
CREATE TABLE IF NOT EXISTS btc5m_markets (
    market_id TEXT PRIMARY KEY,
    market_slug TEXT NOT NULL UNIQUE,
    question TEXT NOT NULL,
    slot_start_ts INTEGER NOT NULL,
    slot_end_ts INTEGER NOT NULL,
    yes_token_id TEXT NOT NULL,
    no_token_id TEXT NOT NULL,
    tick_size REAL,
    min_order_size REAL,
    resolution_source TEXT,
    resolution_rule_text TEXT,
    resolution_rule_version TEXT,
    first_seen_ts INTEGER NOT NULL,
    last_seen_ts INTEGER NOT NULL,
    created_at_ts INTEGER NOT NULL,
    market_resolution_status TEXT NOT NULL DEFAULT 'ACTIVE',
    resolved_outcome TEXT,
    resolved_yes_price REAL,
    resolved_no_price REAL,
    resolved_ts INTEGER,
    settled_ts INTEGER,
    slot_start_reference_price REAL,
    slot_end_reference_price REAL,
    slot_start_reference_ts INTEGER,
    slot_end_reference_ts INTEGER,
    label_quality_flag TEXT,
    notes TEXT
);
```

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_start_ts ON btc5m_markets(slot_start_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_end_ts ON btc5m_markets(slot_end_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_status ON btc5m_markets(market_resolution_status);
```

### `btc5m_snapshots`

```sql
CREATE TABLE IF NOT EXISTS btc5m_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    collected_ts INTEGER NOT NULL,
    written_ts INTEGER NOT NULL,
    source_ts INTEGER,
    seconds_to_resolution INTEGER NOT NULL,
    best_bid_yes REAL,
    best_ask_yes REAL,
    best_bid_no REAL,
    best_ask_no REAL,
    mid_yes REAL,
    mid_no REAL,
    spread_yes REAL,
    spread_no REAL,
    best_bid_size_yes REAL,
    best_ask_size_yes REAL,
    best_bid_size_no REAL,
    best_ask_size_no REAL,
    liquidity_market REAL,
    tick_size REAL,
    min_order_size REAL,
    complement_gap_mid REAL,
    complement_gap_cross REAL,
    price_mid_gap_yes_buy REAL,
    price_mid_gap_yes_sell REAL,
    price_mid_gap_no_buy REAL,
    price_mid_gap_no_sell REAL,
    quote_stable_pass_count INTEGER,
    book_valid INTEGER NOT NULL,
    market_status TEXT NOT NULL,
    orderbook_exists_yes INTEGER NOT NULL,
    orderbook_exists_no INTEGER NOT NULL,
    publish_reason TEXT,
    reject_reason TEXT,
    source_name TEXT NOT NULL,
    collector_latency_ms INTEGER,
    reference_sync_gap_ms INTEGER,
    snapshot_age_ms INTEGER,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_ts
    ON btc5m_snapshots(market_id, collected_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_collected_ts
    ON btc5m_snapshots(collected_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_status
    ON btc5m_snapshots(market_status);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_book_valid
    ON btc5m_snapshots(book_valid);
```

### `btc5m_orderbook_depth`

```sql
CREATE TABLE IF NOT EXISTS btc5m_orderbook_depth (
    depth_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    collected_ts INTEGER NOT NULL,
    yes_bid_depth_3 REAL,
    yes_ask_depth_3 REAL,
    no_bid_depth_3 REAL,
    no_ask_depth_3 REAL,
    yes_bid_depth_5 REAL,
    yes_ask_depth_5 REAL,
    no_bid_depth_5 REAL,
    no_ask_depth_5 REAL,
    yes_bid_depth_within_1c REAL,
    yes_ask_depth_within_1c REAL,
    no_bid_depth_within_1c REAL,
    no_ask_depth_within_1c REAL,
    yes_bid_depth_within_2c REAL,
    yes_ask_depth_within_2c REAL,
    no_bid_depth_within_2c REAL,
    no_ask_depth_within_2c REAL,
    yes_bid_depth_within_5c REAL,
    yes_ask_depth_within_5c REAL,
    no_bid_depth_within_5c REAL,
    no_ask_depth_within_5c REAL,
    source_name TEXT NOT NULL,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_orderbook_depth_market_ts
    ON btc5m_orderbook_depth(market_id, collected_ts);
```

### `btc5m_reference_ticks`

```sql
CREATE TABLE IF NOT EXISTS btc5m_reference_ticks (
    ref_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc INTEGER NOT NULL,
    source_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    btc_price REAL NOT NULL,
    btc_bid REAL,
    btc_ask REAL,
    btc_mark_price REAL,
    btc_index_price REAL,
    volume_1s REAL,
    latency_ms INTEGER,
    meta_json TEXT
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_source_ts
    ON btc5m_reference_ticks(source_name, symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_ts
    ON btc5m_reference_ticks(ts_utc);
```

### `btc5m_reference_1m_ohlcv`

```sql
CREATE TABLE IF NOT EXISTS btc5m_reference_1m_ohlcv (
    candle_ts INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    trade_count INTEGER,
    meta_json TEXT
);
```

### `btc5m_lifecycle_events`

```sql
CREATE TABLE IF NOT EXISTS btc5m_lifecycle_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_market_ts
    ON btc5m_lifecycle_events(market_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_event_type
    ON btc5m_lifecycle_events(event_type);
```

### `collector_runs`

```sql
CREATE TABLE IF NOT EXISTS collector_runs (
    run_id TEXT PRIMARY KEY,
    started_ts INTEGER NOT NULL,
    ended_ts INTEGER,
    collector_name TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    snapshot_count INTEGER NOT NULL DEFAULT 0,
    market_count INTEGER NOT NULL DEFAULT 0,
    reference_tick_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    meta_json TEXT
);
```

### `quality_audits`

```sql
CREATE TABLE IF NOT EXISTS quality_audits (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_ts INTEGER NOT NULL,
    audit_date TEXT NOT NULL,
    market_id TEXT,
    run_id TEXT,
    expected_snapshot_count INTEGER,
    actual_snapshot_count INTEGER,
    slot_coverage_ratio REAL,
    max_gap_sec REAL,
    invalid_book_ratio REAL,
    duplicate_snapshot_ratio REAL,
    missing_reference_ratio REAL,
    missing_resolution_flag INTEGER NOT NULL DEFAULT 0,
    reference_sync_gap_sec REAL,
    audit_status TEXT NOT NULL,
    notes TEXT
);
```

## 4.4 Derived Layer DDL

### `btc5m_features`

```sql
CREATE TABLE IF NOT EXISTS btc5m_features (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    ts_utc INTEGER NOT NULL,
    seconds_to_resolution INTEGER NOT NULL,
    return_15s REAL,
    return_30s REAL,
    return_60s REAL,
    return_120s REAL,
    volatility_30s REAL,
    volatility_60s REAL,
    volatility_180s REAL,
    microprice_yes REAL,
    microprice_no REAL,
    order_imbalance_yes REAL,
    order_imbalance_no REAL,
    complement_gap REAL,
    spread_sum REAL,
    depth_ratio_yes REAL,
    depth_ratio_no REAL,
    quote_stability_score REAL,
    feature_version TEXT NOT NULL,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_features_market_ts_version
    ON btc5m_features(market_id, ts_utc, feature_version);
```

### `btc5m_labels`

```sql
CREATE TABLE IF NOT EXISTS btc5m_labels (
    label_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    decision_ts INTEGER NOT NULL,
    label_horizon_sec INTEGER NOT NULL,
    terminal_outcome TEXT NOT NULL,
    resolved_yes_price REAL,
    resolved_no_price REAL,
    mtm_return_if_buy_yes_hold_to_resolution REAL,
    mtm_return_if_buy_no_hold_to_resolution REAL,
    best_exit_yes_before_expiry REAL,
    best_exit_no_before_expiry REAL,
    would_hit_tp_5c INTEGER,
    would_hit_tp_10c INTEGER,
    would_hit_sl_5c INTEGER,
    would_hit_sl_10c INTEGER,
    time_to_best_yes_sec REAL,
    time_to_best_no_sec REAL,
    label_quality_flag TEXT NOT NULL,
    label_version TEXT NOT NULL,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_labels_market_ts_version
    ON btc5m_labels(market_id, decision_ts, label_horizon_sec, label_version);
```

### `btc5m_decision_dataset`

```sql
CREATE TABLE IF NOT EXISTS btc5m_decision_dataset (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    decision_ts INTEGER NOT NULL,
    seconds_to_resolution INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    mid_yes REAL,
    mid_no REAL,
    spread_yes REAL,
    spread_no REAL,
    btc_price REAL,
    quote_stability_score REAL,
    terminal_outcome TEXT,
    target_yes_hold REAL,
    target_no_hold REAL,
    label_quality_flag TEXT,
    is_trainable INTEGER NOT NULL,
    split_bucket TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    UNIQUE (market_id, decision_ts, dataset_version)
);
```

---

## 5. Schema Notes

### 5.1 Integer Timestamp Standard

All runtime timestamp fields should be stored as Unix epoch seconds.
If milliseconds are needed:

- separate `*_ms` fields can be added
- but the primary join key should remain second-based

### 5.2 Boolean Field Standard

For SQLite:

- `0 = false`
- `1 = true`

### 5.3 Versioned ETL

The following derived fields must carry versions:

- `feature_version`
- `label_version`
- `dataset_version`

This makes formula changes traceable.

---

## 6. Collector Task List

## 6.1 Phase 1 - DB Foundation Layer

### Task 1

Add a new module:

- `common/btc5m_dataset_db.py`

Contents:

- `connect_db(db_path)`
- `migrate_schema(conn)`
- `upsert_market(...)`
- `insert_snapshot(...)`
- `insert_orderbook_depth(...)`
- `insert_reference_tick(...)`
- `insert_lifecycle_event(...)`
- `start_collector_run(...)`
- `finish_collector_run(...)`

Definition of done:

- schema is created from a single function
- running it a second time does not break anything
- WAL is enabled

### Task 2

Make the DB path configurable through `.env`.

New env:

- `BTC5M_DATASET_DB_PATH`

Default:

- `runtime/data/btc5m_dataset.db`

## 6.2 Phase 2 - Scanner Raw Write

### Task 3

Add market metadata writes inside `polymarket_scanner/btc_5min_clob_scanner.py`.

To do:

- upsert into `btc5m_markets` when a market is discovered
- write a `DISCOVERED` lifecycle event
- optionally store the reason for current-slot / next-slot selection as metadata

### Task 4

Add snapshot writes after every valid scan.

To do:

- map the current `build_snapshot(...)` payload into a DB row
- separate `collected_ts` and `written_ts`
- store `publish_reason` and `reject_reason`
- store `book_valid`, `complement_gap`, `price_mid_gap`

### Task 5

Store rejected candidates as well.

This is critical.
Not only published snapshots, but also:

- rejected candidate rows
- reject reasons
- state events

must be stored.

Minimum path:

- write to `btc5m_snapshots` with `book_valid=0` and `reject_reason`
- additionally write `REJECTED` into `btc5m_lifecycle_events`

### Task 6

Add order book depth summaries.

The current scanner only fetches top-level values.
Upgrade:

- compute first-3-level and first-5-level notional totals from the `/book` response
- write them to the table

Note:
Storing the full raw order book is not required for the MVP.

## 6.3 Phase 3 - Reference Feed

### Task 7

Add a new helper module:

- `common/btc5m_reference_feed.py`

To do:

- fetch ticks from Binance or the chosen source
- normalize field names
- measure latency
- write at 1-second cadence

### Task 8

Add the reference collector loop.

Implementation options:

- a lightweight loop inside the scanner
- or a separate process

Recommendation:

- keep it inside the scanner initially
- split it later if needed

### Task 9

Lock down the rule for matching a snapshot to the nearest reference tick.

Rule:

- the difference between snapshot ts and reference ts must be `<= 1 sec`
- if the difference is larger, write `reference_sync_gap_ms`
- downgrade the quality flag

## 6.4 Phase 4 - Lifecycle and Resolution

### Task 10

Make market status transitions explicit.

Status / event list:

- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`
- `CANCELLED`

### Task 11

Add a resolution collector.

To do:

- check the official outcome when the market closes
- write resolution fields into `btc5m_markets` and, if needed, a separate resolution row
- add a `RESOLVED` lifecycle event

### Task 12

Capture the post-expiry no-orderbook condition as an explicit state.

Why?
This condition is real in runtime incidents.
The backtest side must be able to see it.

To do:

- `orderbook_exists_yes/no`
- `last_orderbook_seen_ts`
- `PENDING_SETTLEMENT` event

## 6.5 Phase 5 - Audit Script

### Task 13

Add a new script:

- `scripts/btc5m_audit_dataset.py`

Checks:

- slot coverage
- duplicate rows
- missing reference
- missing resolution
- invalid ratio
- max snapshot gap

### Task 14

Write the daily audit report both to the table and to stdout.

MVP:

- console summary
- `quality_audits` insert

---

## 7. Label ETL Checklist

## 7.1 Label ETL Inputs

The label ETL uses these tables:

- `btc5m_markets`
- `btc5m_snapshots`
- `btc5m_reference_ticks`
- `btc5m_lifecycle_events`

## 7.2 Label ETL Steps

### Step 1

Select only markets with a clear terminal state.

Include:

- `RESOLVED`

Exclude:

- `ACTIVE`
- `PENDING_SETTLEMENT`
- `CANCELLED` unless a separate label strategy is defined

### Step 2

Validate the official terminal fields for each market.

Check:

- `resolved_outcome`
- `resolved_yes_price`
- `resolved_no_price`
- `resolved_ts`

If missing:

- `label_quality_flag = MISSING_OFFICIAL_RESOLUTION`
- market excluded from the training set

### Step 3

Select decision timestamp candidates.

Recommendation:

- every snapshot can be a candidate
- but the last 3 seconds or stale snapshots can optionally be excluded

Pragmatic MVP:

- `seconds_to_resolution >= 5`
- `book_valid = 1`
- acceptable reference sync

### Step 4

Fix the entry price rule for each decision row.

Recommendation:

- for YES long, use `best_ask_yes`
- for NO long, use `best_ask_no`

Hold-to-resolution return:

- YES: `resolved_yes_price - entry_price`
- NO: `resolved_no_price - entry_price`

### Step 5

Compute path-dependent labels.

Examples:

- `best_exit_yes_before_expiry`
- `best_exit_no_before_expiry`
- `would_hit_tp_5c`
- `would_hit_sl_5c`
- `time_to_best_yes_sec`

During this computation:

- only snapshots after the decision ts should be used
- if there is no order book after expiry, the path should be cut accordingly or processed as explicit state

### Step 6

Apply leakage controls.

Rules:

- feature computation cannot use data after decision ts
- different rows from the same slot cannot go to different splits
- label computation may use the future, feature computation may not

### Step 7

Assign the trainability flag.

Conditions for `is_trainable = 1`:

- official resolution exists
- snapshot is valid
- reference sync is acceptable
- not duplicate
- not stale
- not cancelled

Otherwise:

- `is_trainable = 0`
- write a reason note or quality flag

### Step 8

Produce versioned label output.

Output:

- `btc5m_labels`
- `btc5m_decision_dataset`

Versions:

- `label_version`
- `dataset_version`

---

## 8. Feature ETL Checklist

### Minimum Feature Set

For the first version, the following are sufficient:

- `return_15s`
- `return_30s`
- `return_60s`
- `volatility_30s`
- `volatility_60s`
- `spread_sum`
- `complement_gap`
- `order_imbalance_yes`
- `order_imbalance_no`
- `depth_ratio_yes`
- `depth_ratio_no`
- `quote_stability_score`
- `seconds_to_resolution`

### Feature Rules

- only look backward
- forward fill should be limited
- if reference data is missing, write a flag
- exact formulas should be versioned

---

## 9. Acceptance Criteria

For the collector and dataset to be considered "usable":

- `slot_coverage_ratio >= 0.90`
- `max_snapshot_gap_sec <= 10`
- `missing_resolution_count = 0` for completed markets
- `duplicate_snapshot_ratio < 0.01`
- `reference_sync_gap_sec <= 1` median
- `invalid_book_ratio < 0.20`
- `trainable_row_ratio >= 0.70` after the first stable data window

These thresholds can be optimized later, but they are sufficient for initial reporting.

---

## 10. Immediate Coding Order

1. Create `common/btc5m_dataset_db.py`.
2. Add `btc5m_markets` and `btc5m_snapshots` writes to the scanner.
3. Save rejected / invalid candidates as well.
4. Add the reference tick writer.
5. Add lifecycle event writes.
6. Write the audit script.
7. Write resolution ingestion.
8. Write feature ETL.
9. Write label ETL.
10. Write the decision dataset builder.

If this order is preserved:

- first, data loss is prevented
- then quality becomes visible
- then the modeling / backtest layer is built

---

## 11. Short Technical Note

For the MVP, the biggest mistake would be this:

Treating the bot's snapshot JSON archive alone as the dataset.

That is insufficient because:

- there are no rejected candidates
- there is no lifecycle state
- evidence of reference synchronization is weak
- resolution metadata is missing
- ETL reproducibility is low

The correct MVP is:

- raw DB writes
- audit
- then ETL

---

**Status:** Implementation spec ready
**Related plan:** [Backtest_Data_Collection_Plan.md](Backtest_Data_Collection_Plan.md)
**Last updated:** 2026-03-14
