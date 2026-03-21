# BTC5M Scanner Gap Analysis and Upgrade Plan

## 1. Objective

The purpose of this document is to analyze the current scanner file:

- [btc_5min_clob_scanner.py](../../polymarket_scanner/btc_5min_clob_scanner.py)

in its current state and, relative to the dataset plan, make clear:

- what is already correct
- what is missing
- what should remain inside the scanner
- what should be moved into separate components

The output of this document is:

- the scanner will require changes
- but the entire dataset system must not be piled into the scanner

---

## 2. What Is Already Correct in the Current Scanner

The following parts should be preserved:

### 2.1 CLOB-Only Pricing Logic Is Correct

The scanner does not use fallback prices.
That is correct.
It should also remain that way for dataset quality.

Relevant location:

- `btc_5min_clob_scanner.py` opening description

### 2.2 Current-Slot-First Selection Is Correct

The scanner prefers the current slot first and only publishes the next slot near the end.
This is sensible for trading behavior.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L171`

### 2.3 The Validation Layer Is Solid

The current scanner:

- fetches BUY/SELL prices
- fetches midpoint
- checks spread
- checks midpoint deviation
- checks complement gap
- filters liquidity

This part is also valuable for the dataset.

Relevant locations:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L270`
- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L316`

### 2.4 Stable Publish Gate Is Correct

It does not immediately publish a clean quote from a single scan.
That is also good.
It reduces transient noise.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L445`

### 2.5 Atomic Snapshot Write Is Correct

For the bot side, atomically writing the JSON snapshot file is correct.
That should be preserved.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L391`

### 2.6 No-Data Alert Logic Is Useful

Operationally, this is necessary in order to understand when the scanner is no longer producing fresh snapshots.
This should also be preserved.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L484`

Conclusion:

The current scanner is broadly correct at its job of producing clean live snapshots for the bot.
The problem is that this alone does not satisfy the needs of a dataset collector.

---

## 3. What Is Missing in the Scanner Relative to the Dataset Plan

## 3.1 The Biggest Gap: The Scanner Publishes, But Does Not Truly Collect

The current scanner's main output is:

- a single JSON snapshot file
- a log file

But what the dataset requires is:

- all observations
- rejected candidates
- warmup states
- lifecycle events
- raw DB rows

In its current form, these do not exist.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L402`

Impact:

- selection bias is introduced
- only "historically clean quotes" are retained
- invalid states are lost

## 3.2 Reject and Warmup States Are Not Structured

The scanner logs `WARMUP` and `SKIP`, but does not write them to the database.

Relevant locations:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L447`
- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L478`

This is insufficient from a dataset perspective.
Because one of the most important strategy questions is:

- why were certain markets filtered out?

## 3.3 The Snapshot Schema Is Too Narrow for the Dataset

The current payload does not include:

- `slot_end_ts`
- `seconds_to_resolution`
- `publish_reason` in structured form
- `reject_reason`
- `quote_stable_pass_count`
- `complement_gap` as an explicit field
- `orderbook_exists_yes/no`
- `market_status`
- separate `collected_ts` and `written_ts`

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L335`

## 3.4 The Stale Check Is Practically Ineffective

Right now the scanner writes `payload["ts"]` itself, and then immediately checks the age of that value.
This does not measure true source staleness.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L460`

Because of this:

- `MAX_BOOK_AGE_SEC` is not a reliable dataset-quality metric
- it only checks local processing age

## 3.5 There Is No Order Book Depth

The scanner only extracts best bid/ask and size from the `/book` endpoint.
There is no first-3-level or first-5-level depth.

Relevant location:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L200`

Impact:

- slippage simulation remains weak
- execution realism remains incomplete

## 3.6 There Is No Reference Price Integration

In the dataset plan, a BTC reference tick is mandatory.
The scanner does not collect this at all.

Impact:

- market movement and underlying movement cannot be connected on the same timeline
- feature engineering remains weaker

## 3.7 There Is No Lifecycle State Capture

The scanner only deals with markets that are active / publishable.
It does not explicitly store these states:

- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`
- `CANCELLED`

Relevant locations:

- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L122`
- `polymarket_scanner/btc_5min_clob_scanner.py` line reference `L171`

Impact:

- it becomes impossible to see what happened after expiry
- the no-orderbook state does not reach the dataset

## 3.8 There Is No Resolution Ingestion

The scanner does not collect official market outcomes.

Impact:

- there is no official source for the label pipeline
- the dataset remains incomplete for supervised training

## 3.9 There Is No Audit / Collector Run Record

The current scanner does not keep structured information such as:

- snapshot count
- reject count
- run config hash
- duplicate count
- gap metrics

Impact:

- data quality cannot be measured
- run-by-run comparison becomes harder

---

## 4. Which Requirements Should Stay Inside the Scanner, and Which Should Be Separate?

This section is critical.
It is not correct to load the scanner with all dataset responsibilities.

## 4.1 What Should Stay Inside the Scanner

These should stay inside the scanner:

- market discovery
- YES/NO quote fetch
- top-of-book validation
- complement / liquidity checks
- stable pass logic
- JSON snapshot write for the bot
- raw DB snapshot write
- rejected candidate write
- order book depth summary write
- lifecycle event write
- lightweight run metrics

## 4.2 What Should Be Moved Outside the Scanner

These should become separate components:

- official resolution collector
- BTC reference collector
- audit script
- feature ETL
- label ETL
- decision dataset builder

Why?

The scanner's main responsibility should be:

- fast and reliable observation

The scanner's main responsibility should not be:

- ETL
- label generation
- training dataset construction

If this separation is not preserved, the scanner becomes both more complex and more risky for live snapshot publishing.

---

## 5. What Must Change in the Current Scanner

## 5.1 Non-Breaking Rule

The most important rule:

**The existing bot snapshot JSON contract must not break.**

That means:

- `write_snapshot(...)` must be preserved
- the core fields used by the bot must not change
- new fields may be added, but old fields must not break

## 5.2 The Scanner Must Become Dual-Output

Instead of a single output, the scanner must produce two outputs:

1. For the bot:
- the existing JSON snapshot

2. For the dataset:
- raw SQLite writes

This is the correct transition model.

## 5.3 Every Candidate Observation Must Be Stored

Not only valid publishes:

- warmup pass
- reject
- token missing
- cross-validation fail
- market not publishable

and similar conditions must also be stored in structured form.

The new concept here is:

- `candidate observation`

For each scan and for each candidate market, an observation row should be created.

## 5.4 Snapshot Rows and Event Rows Must Be Separated

Instead of forcing everything into one table:

- `btc5m_snapshots`
- `btc5m_lifecycle_events`

should remain separate.

This is cleaner for the scanner as well.

## 5.5 Depth Aggregation Must Be Added

`fetch_book(...)` should compute not only the best price, but also first-3-level and first-5-level notional totals.

This change should happen inside the scanner.
Because the `/book` response is already in the scanner's hands.

## 5.6 An Explicit `market_status` Field Must Be Added

The following must be explicit in both the snapshot payload and the DB row:

- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`

Without these fields, the dataset cannot understand path-dependent execution failures.

## 5.7 Local Latency Must Be Measured

Even if no source timestamp exists, the scanner should write at least these fields:

- request start ts
- request end ts
- local fetch latency ms
- snapshot write ts

This is the minimum quality foundation.

## 5.8 The Stale Check Must Be Redesigned

The current stale check should not be removed, but it should be made correct.

New rule:

- snapshot file freshness is separate
- source data staleness is separate

Minimum scanner implementation:

- measure fetch latency
- measure scan cycle duration
- measure previous publish age
- if there is no `source_ts`, write `source_ts=NULL` and a `staleness_unknown` flag

## 5.9 Resolution Should Go to a Sidecar, Not Into the Scanner

Official outcome collection should not live inside the scanner.
It should be a separate collector / script.

This is more correct both operationally and logically.

---

## 6. Recommended Upgrade Phases

## Phase 0 - Refactor Without Behavior Change

Goal:

Prepare the internal structure without breaking the current scanner behavior.

To do:

- add the DB helper module
- add the snapshot row mapper function
- add the candidate result object structure
- make line-level log reasons structured

Deliverable:

- behavior stays the same
- only the codebase becomes ready for upgrade

## Phase 1 - Raw DB Write

Goal:

Write valid snapshots to SQLite as well.

To do:

- `btc5m_markets` upsert
- `btc5m_snapshots` insert
- `collector_runs` start / finish

Deliverable:

- JSON snapshot continues
- DB side writes begin

## Phase 2 - Rejected Observation Capture

Goal:

Reduce selection bias.

To do:

- warmup observation recording
- reject_reason recording
- separate token missing / cross fail / price fail categories
- write `REJECTED` and `PUBLISHED` into `btc5m_lifecycle_events`

Deliverable:

- we become able to see why observations were filtered out in the dataset

## Phase 3 - Depth and Latency

Goal:

Add execution realism.

To do:

- depth 3 / depth 5 aggregation
- local request latency
- scan duration
- write duration

Deliverable:

- the minimum data for slippage simulation becomes available

## Phase 4 - Explicit State and No-Orderbook Support

Goal:

Make lifecycle state explicit.

To do:

- `market_status`
- `orderbook_exists_yes/no`
- `first_seen_ts`
- `last_seen_ts`
- `last_orderbook_seen_ts`

Deliverable:

- expiry / no-orderbook transitions become observable

## Phase 5 - Reference and Resolution Integration

Goal:

Connect the scanner to the dataset ecosystem.

Two paths exist here:

Do not place them directly into the scanner:

- reference collector as a separate script / process
- resolution collector as a separate script / process

The scanner's job:

- raw market observation only

Deliverable:

- the scanner remains safer
- dataset requirements become complete

## Phase 6 - Audit Readiness

Goal:

See numerically how well the collector is gathering data.

To do:

- per-slot expected vs. actual snapshot count
- duplicate snapshot audit
- missing reference audit
- missing resolution audit

Deliverable:

- it becomes measurable whether the scanner data is actually useful

---

## 7. Scanner-Specific Task List

1. Add the `common/btc5m_dataset_db.py` module.
2. Add a config-driven DB write path to the scanner.
3. Add `build_snapshot_row(...)` next to `build_snapshot(...)`.
4. Generate a structured result object for each candidate inside `scan_once()`.
5. Write valid observations into `btc5m_snapshots`.
6. Write rejected and warmup observations as well.
7. Add depth aggregation inside `fetch_book(...)`.
8. Write `market_status` and `orderbook_exists_*` explicitly.
9. Add local latency metrics.
10. Keep the JSON snapshot contract backward-compatible without breaking it.

---

## 8. Risks and Points of Attention

### 8.1 The Biggest Risk: Breaking the Live Bot

The scanner is currently critical for the bot.
Therefore all changes must be:

- additive
- backward-compatible
- must not break the JSON snapshot path

### 8.2 DB Writes Must Not Block the Scanner

The scan loop must not be delayed because of SQLite writes.

Recommendation:

- simple transactions
- short insert path
- if an error happens, log it but do not stop publishing

### 8.3 Too Much Responsibility Must Not Be Loaded Into the Scanner

If reference feed and resolution logic are fully embedded into the scanner:

- debugging becomes harder
- outage risk increases

For this reason, separate collectors are more correct.

---

## 9. Final Decision

Yes, there are things in the current 5MIN BTC scanner that must change relative to the dataset plan.

But the conclusion is:

- the current validation / publish logic should be preserved
- the scanner should become a `snapshot publisher + raw collector`
- reference, resolution, audit, and ETL should remain outside the scanner

So the scanner will not be completely rewritten.
The correct path is:

- extend the current scanner without crippling it
- add structured raw writes for the dataset
- build the rest of the pipeline with separate components

---

**Related documents:**
- [Backtest_Data_Collection_Plan.md](Backtest_Data_Collection_Plan.md)
- [BTC5M_Dataset_Implementation_Spec.md](BTC5M_Dataset_Implementation_Spec.md)
- [BTC5M_Dataset_Architecture_Diagram.md](../architecture/BTC5M_Dataset_Architecture_Diagram.md)
