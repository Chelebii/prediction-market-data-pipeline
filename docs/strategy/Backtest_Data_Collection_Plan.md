# BTC 5MIN Up/Down Backtest Data Collection and Dataset Plan

## 1. Objective

The objective is to build, for Polymarket BTC 5-minute up/down markets, a dataset that is:

- backtest-ready
- execution-aware
- ML-ready
- and later, if useful, LLM-friendly

This dataset must be able to answer these four questions:

1. Which market was actually tradable at this moment?
2. At what price could I realistically enter and exit?
3. How did the market resolve?
4. Is this snapshot or decision point reliable enough for model training?

If the dataset cannot answer any one of these four questions, it should be considered incomplete.

---

## 2. Clear Scope

The scope is strictly limited to:

- BTC 5-minute up/down markets
- raw market data collection
- label generation
- backtesting and strategy research
- later supervised ML / ranking / policy learning

Out of scope for now:

- ETH / SOL / other coins
- other market types
- a general-purpose scanner platform
- distributed architecture from day one
- LLM fine-tuning from day one

---

## 3. What Is Already Correct in the Current Plan

The following aspects should be preserved:

- focus on a single target: BTC 5-minute markets
- the idea of 2-5 second snapshot resolution
- mandatory collection of bid/ask and size
- a BTC reference price layer
- a label / outcome layer
- starting with SQLite
- the quality control and audit concept

However, these alone are not sufficient.
In particular, label definition, lifecycle states, and execution realism need to be specified more clearly.

---

## 4. Critical Items That Must Be Added

### 4.1 Resolution Rule Metadata Is Mandatory

Storing only `resolved_outcome=Up/Down` is not enough.

The following fields must be recorded for every market:

- `resolution_source`
- `resolution_rule_text`
- `resolution_rule_version`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `slot_start_reference_ts`
- `slot_end_reference_ts`
- `resolved_ts`
- `settled_ts`
- `market_resolution_status` -> `ACTIVE`, `EXPIRED`, `PENDING_SETTLEMENT`, `RESOLVED`, `CANCELLED`

Why?

Using Binance as a BTC reference is useful, but inferring the label from Binance is dangerous.
The label source must come from the market's own official resolution data.

### 4.2 All Slots Must Be Collected, Not Only Traded Ones

If the dataset only records moments when the bot actually traded, selection bias will be introduced.

Therefore:

- every active slot must be recorded
- snapshots with no trade signal must also be recorded
- invalid / stale / rejected snapshots must be stored with separate flags

This is mandatory for both realistic backtesting and ML.

### 4.3 Market Lifecycle and Order Book States Must Be Added

As seen in runtime incidents, the order book may disappear after expiry.
For this reason, the data model must carry market state, not just price.

Required fields:

- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`
- `last_orderbook_seen_ts`
- `first_seen_ts`
- `last_seen_ts`
- `publish_reason`
- `reject_reason`

The backtest engine must be able to distinguish between:

- quote exists
- quote missing
- market expired
- settlement pending
- market resolved

### 4.4 Depth / Marketability Data Must Be Added for Execution

Top-of-book alone is sometimes not enough.
At minimum, the following fields should be added:

- `best_bid_yes`, `best_ask_yes`, `best_bid_no`, `best_ask_no`
- `best_bid_size_yes`, `best_ask_size_yes`, `best_bid_size_no`, `best_ask_size_no`
- `depth_3_levels_yes_bid_notional`
- `depth_3_levels_yes_ask_notional`
- `depth_3_levels_no_bid_notional`
- `depth_3_levels_no_ask_notional`
- `depth_5_levels_*` or alternatively `depth_within_1c`, `depth_within_2c`, `depth_within_5c`

If full order book storage is too expensive, at least aggregated depth should be retained.
This is critical for slippage simulation.

### 4.5 Source Timestamps and Latency Fields Must Be Added

Collector time and source time are not the same thing.

The following fields should be added:

- `collected_ts_utc`
- `written_ts_utc`
- `source_ts_utc` if available
- `collector_latency_ms`
- `reference_latency_ms`
- `snapshot_age_ms`

This makes stale data analysis measurable and numerical.

### 4.6 Raw and Derived Layers Must Be Separated

It may be practical to write features during collection time, but the canonical dataset should remain raw.

Correct separation:

- `raw layer`: market, snapshot, order book, reference, resolution
- `derived layer`: features, labels, decision rows

This ensures that:

- raw data is not corrupted when feature formulas change
- new experiments can be generated from the same raw data
- auditability and reproducibility are preserved

### 4.7 A Decision Dataset Table Must Be Added

A raw snapshot table alone is not sufficient for model training.
An additional "decision row" dataset is required.

Each row represents:

- one market
- one timestamp
- one decision point

Fields:

- `market_id`
- `decision_ts`
- `seconds_to_resolution`
- raw quote summary
- reference summary
- feature set
- label set
- quality flags
- `is_trainable`

This table becomes the main table later for supervised ML, ranking models, and policy testing.

### 4.8 Time-Based Split and Leakage Rules Must Be Defined

For ML training, the rule should already be written down now:

- random split is FORBIDDEN
- splitting must be time-based only
- data from the same slot must not appear in both train and test
- future information close to the label must not leak into the features

This rule should be explicitly stated in the plan.

### 4.9 Quality SLO / Acceptance Criteria Must Be Added

It is not enough to say "there should be a report."
Acceptance criteria must be numeric.

Example:

- `slot_coverage_ratio >= 0.90`
- `max_snapshot_gap_sec <= 10`
- `reference_sync_gap_sec <= 1`
- `missing_resolution_count = 0`
- `invalid_book_ratio < 0.20`
- `duplicate_snapshot_ratio < 0.01`

These values can change later, but the threshold-based logic must be explicit in the plan.

### 4.10 The LLM Note Must Be Positioned Correctly

The first goal of this dataset should not be LLM usage.
The first goal should be:

- backtesting
- tabular / time-series ML

LLM usage can come later in forms such as:

- snapshot window -> textual market summary
- anomaly explanation
- strategy diary / run analysis

In other words, the dataset schema should be built for numeric research, not for LLMs.

---

## 5. What Should Be Removed or Deprioritized

### 5.1 Too Much Feature Computation Inside the Collector

The following items are not required in the collector MVP:

- complex indicators such as RSI
- regime classifier
- a large number of handcrafted features

These should move to the offline ETL layer.
On the collector side, only cheap metrics that are critical for debugging may remain:

- `complement_gap`
- `price_mid_gap`
- `quote_stable_pass_count`

### 5.2 The "Resolved Outcome Alone Is Enough" Approach

This approach should be removed.
It should be replaced with:

- official label information
- start/end reference price
- resolution timestamps
- lifecycle status

### 5.3 The Assumption That "Top-of-Book Alone Is Enough"

This should also be removed or marked as a weak assumption.
Even if the minimum level starts with top-of-book, a depth summary should be considered mandatory in the medium term.

### 5.4 Early Model Training

Model training should not be the day-one goal.
The correct sequence is:

1. raw collection
2. audit
3. label generation
4. backtest
5. baseline ML
6. LLM only if needed

---

## 6. Recommended Data Architecture

## 6.1 Raw Layer Tables

### `btc5m_markets`

Metadata for each slot / market.

Required columns:

- `market_id`
- `market_slug`
- `question`
- `slot_start_ts`
- `slot_end_ts`
- `yes_token_id`
- `no_token_id`
- `tick_size`
- `min_order_size`
- `resolution_source`
- `resolution_rule_text`
- `created_at`
- `first_seen_ts`
- `last_seen_ts`
- `market_resolution_status`

### `btc5m_snapshots`

The main time-series table.

Required columns:

- `snapshot_id`
- `market_id`
- `collected_ts_utc`
- `written_ts_utc`
- `seconds_to_resolution`
- `best_bid_yes`
- `best_ask_yes`
- `best_bid_no`
- `best_ask_no`
- `mid_yes`
- `mid_no`
- `spread_yes`
- `spread_no`
- `best_bid_size_yes`
- `best_ask_size_yes`
- `best_bid_size_no`
- `best_ask_size_no`
- `liquidity_market`
- `complement_gap_mid`
- `complement_gap_cross`
- `book_valid`
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`
- `reject_reason`
- `source_name`
- `collector_latency_ms`

### `btc5m_orderbook_depth`

A summary depth table for execution simulation.

Required columns:

- `market_id`
- `collected_ts_utc`
- `yes_bid_depth_3`
- `yes_ask_depth_3`
- `no_bid_depth_3`
- `no_ask_depth_3`
- `yes_bid_depth_5`
- `yes_ask_depth_5`
- `no_bid_depth_5`
- `no_ask_depth_5`

Alternative:

Instead of full depth, notional fields such as `within_1c`, `within_2c`, and `within_5c` may be stored.

### `btc5m_reference_ticks`

BTC reference time series.

Required columns:

- `ts_utc`
- `source_name`
- `btc_price`
- `btc_bid`
- `btc_ask`
- `btc_mark_price`
- `btc_index_price`
- `volume_1s`
- `latency_ms`

Additionally, a 1-minute OHLCV table or view may be added:

- `btc_1m_ohlcv`

### `btc5m_resolution`

Market outcome and official label table.

Required columns:

- `market_id`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `resolved_outcome`
- `resolved_yes_price`
- `resolved_no_price`
- `resolved_ts`
- `settled_ts`
- `resolution_source`
- `market_resolution_status`
- `label_quality_flag`

### `btc5m_lifecycle_events`

Market state transitions.

Example events:

- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`

Columns:

- `event_id`
- `market_id`
- `event_ts`
- `event_type`
- `reason`
- `meta_json`

### `collector_runs`

Used for ingestion and audit.

Columns:

- `run_id`
- `started_ts`
- `ended_ts`
- `collector_version`
- `config_hash`
- `snapshot_count`
- `error_count`

### `quality_audits`

Daily / slot-level quality report.

Columns:

- `audit_id`
- `audit_date`
- `market_id`
- `expected_snapshot_count`
- `actual_snapshot_count`
- `slot_coverage_ratio`
- `max_gap_sec`
- `invalid_book_ratio`
- `missing_reference_ratio`
- `missing_resolution_flag`
- `notes`

---

## 6.2 Derived Layer Tables

### `btc5m_features`

A feature layer that can be regenerated from raw data.

Example columns:

- `market_id`
- `ts_utc`
- `seconds_to_resolution`
- `return_15s`
- `return_30s`
- `return_60s`
- `return_120s`
- `volatility_30s`
- `volatility_60s`
- `volatility_180s`
- `microprice_yes`
- `microprice_no`
- `order_imbalance_yes`
- `order_imbalance_no`
- `complement_gap`
- `distance_to_0_5`
- `distance_to_recent_high`
- `distance_to_recent_low`
- `quote_stability_score`

### `btc5m_labels`

A label layer that stores more than a single market outcome.

Example columns:

- `market_id`
- `decision_ts`
- `terminal_outcome`
- `mtm_return_if_buy_yes_hold_to_resolution`
- `mtm_return_if_buy_no_hold_to_resolution`
- `best_exit_yes_before_expiry`
- `best_exit_no_before_expiry`
- `would_hit_tp_5c`
- `would_hit_tp_10c`
- `would_hit_sl_5c`
- `time_to_best_yes`
- `time_to_best_no`
- `label_horizon_sec`

### `btc5m_decision_dataset`

The main table for model training and research.

Each row contains:

- `market_id`
- `decision_ts`
- feature set
- quality set
- label set
- `is_trainable`
- `split_bucket`

This table should be regenerable from raw data.

---

## 7. Collection Frequency and Coverage

### Recommended Frequency

- market snapshot: every `2-3 seconds`
- reference tick: every `1 second`
- order book depth summary: every `2-5 seconds`
- resolution: when the market closes and when settlement completes

### Why?

In a 5-minute market, the last 30-90 seconds of microstructure are very important.
A 60-second recording resolution is insufficient.

### Coverage Rule

For each market, data collection should:

- start at `DISCOVERED`
- continue until `RESOLVED` or `CANCELLED`

Not only when active quotes exist:

- state records should exist even when there are no quotes
- rejection reasons should be recorded

---

## 8. Label Generation Rule

This is the most critical topic.

Label generation should follow this principle:

- `resolved_outcome` must come from the official market result
- `slot_start_reference_price` and `slot_end_reference_price` must be stored separately
- `final_btc_price` must not stand in as a substitute label by itself

Why?

Because the final price alone can generate incorrect labels if:

- the source is unclear
- the exact timestamp is unclear
- it does not match the market rule exactly

Therefore, the label table should store the following separately:

- source
- timestamp
- quality flag
- status

---

## 9. Quality Controls

The following checks are mandatory:

- expected snapshot count vs. actual snapshot count for each slot
- are there duplicate snapshots?
- what is the maximum gap in seconds?
- what is the stale snapshot ratio?
- what is the invalid book ratio?
- how large is the complement gap inconsistency?
- is the market shown as active while the order book is missing?
- are reference ticks missing?
- are there resolved markets with no label?
- are there cancelled / ambiguous markets?

The following alert / SLO logic should be added:

- `slot_coverage_ratio < threshold`
- `missing_resolution_count > 0`
- `max_gap_sec > threshold`
- `invalid_book_ratio > threshold`
- `reference_sync_gap_sec > threshold`

---

## 10. Mandatory Simulation Assumptions for Backtesting

The simulation model should be defined clearly together with the data collection plan.

At minimum, the engine should support:

- fee model
- spread crossing
- order size vs. available depth
- partial fill
- slippage
- forced exit before expiry
- no-order-book conditions after expiry
- pending settlement state
- max trade count per slot
- cooldown
- time-stop
- stop-loss / take-profit / trailing

Especially:

The distinction between `EXPIRED` and `PENDING_SETTLEMENT` must be explicit in simulation.

---

## 11. Data Preparation for ML and LLM

### For ML

The priority should be:

- tabular baseline
- time-series baseline
- ranking / classification

Preparation rules:

- do not use random split
- split train / validation / test by time
- do not let the same slot fall into two splits
- feature computation must not create label leakage

### For LLM

LLM usage can be considered, but only as a second stage.
If needed later, formats such as the following can be generated:

- `window_summary_text`
- `market_context_text`
- `anomaly_report_text`

But the core dataset must remain numeric and structured.

---

## 12. Recommended Phase Plan

### Phase 1 - Raw Collector

Goal:

Collect raw and immutable data for BTC 5-minute markets.

Deliverables:

- `btc5m_markets`
- `btc5m_snapshots`
- `btc5m_reference_ticks`
- `btc5m_resolution`
- `btc5m_lifecycle_events`

### Phase 2 - Quality / Audit

Goal:

Prove that the data is usable.

Deliverables:

- `quality_audits`
- audit script
- stale / gap / missing-label report

### Phase 3 - Execution Realism

Goal:

Bring the backtest closer to fill reality.

Deliverables:

- `btc5m_orderbook_depth`
- slippage model inputs
- edge-case logs for order book / market state

### Phase 4 - Feature + Label ETL

Goal:

Produce the derived layer for research and modeling.

Deliverables:

- `btc5m_features`
- `btc5m_labels`
- `btc5m_decision_dataset`

### Phase 5 - Backtest + Baseline ML

Goal:

Compare strategies on the same dataset.

Deliverables:

- parameterized backtest engine
- baseline strategy comparison
- baseline ML benchmark

---

## 13. Minimum Initial Scope (Real MVP)

Without the following fields, it is hard to call the dataset "useful."

### Minimum Market Metadata

- `market_id`
- `market_slug`
- `question`
- `slot_start_ts`
- `slot_end_ts`
- `yes_token_id`
- `no_token_id`

### Minimum Snapshot

- `collected_ts_utc`
- `market_id`
- `seconds_to_resolution`
- `best_bid_yes`
- `best_ask_yes`
- `best_bid_no`
- `best_ask_no`
- `mid_yes`
- `mid_no`
- `spread_yes`
- `spread_no`
- `best_bid_size_yes`
- `best_ask_size_yes`
- `best_bid_size_no`
- `best_ask_size_no`
- `liquidity_market`
- `book_valid`
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`

### Minimum Reference

- `ts_utc`
- `source_name`
- `btc_price`

### Minimum Label

- `resolved_outcome`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `resolved_ts`
- `market_resolution_status`

If these four groups are missing, the dataset will remain weak for both backtesting and ML.

---

## 14. Final Decision

The correct target for this project is:

**To build a raw + quality-scored + resolution-safe dataset for BTC 5-minute up/down markets.**

This dataset should be suitable for:

1. strategy backtesting
2. execution simulation
3. supervised ML
4. later, if useful, LLM-assisted research

Core principles:

- raw data should remain immutable
- labels must come from the official source
- lifecycle state loss must not happen
- not only traded moments, but all slots should be collected

---

**Status:** Revised  
**Focus:** BTC 5-minute up/down dataset  
**Last Updated:** 2026-03-14
