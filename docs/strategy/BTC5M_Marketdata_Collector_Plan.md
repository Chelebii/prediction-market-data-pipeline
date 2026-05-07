# BTC5M Trade Tick Collector Plan

## Purpose

Add Polymarket public trade-fill / tick collection to the existing BTC5M dataset. The new collector follows the same pattern as `btc5m_resolution_collector.py`: single Python script, env-driven config, lock + `collector_runs`, registered in `btc5m_collection_control.ps1`.

Two phases:

1. **Live integration:** start collecting forward-only. Record the wall-clock timestamp when live ingestion begins (`T_live_start`).
2. **Historical fill:** after live is stable, run the same script in one-shot mode to walk every market in `btc5m_markets` from `MIN(slot_start_ts)` up to `T_live_start`. `INSERT OR IGNORE` + a UNIQUE index guarantee no duplicates against the live writer.

This plan is documentation only.

## Source

| Source | Endpoint |
|--------|----------|
| Polymarket Data API | `GET https://data-api.polymarket.com/trades` |

Relevant parameters:

- `market` = `btc5m_markets.market_id` (conditionId).
- `side` = `BUY` or `SELL`.
- `limit` up to 1000.
- `offset` capped at 3000 per market+side (observed). 5-minute BTC slots virtually never hit this cap.
- `takerOnly=true` for v1.

## Schema

Add one table to `common/btc5m_dataset_db.py` `TABLE_SPECS`. `connect_db()` migrates it on the next run.

### `btc5m_trade_ticks`

```text
trade_id INTEGER PRIMARY KEY AUTOINCREMENT
market_id TEXT NOT NULL
market_slug TEXT NOT NULL
ts_utc INTEGER NOT NULL
asset_token_id TEXT NOT NULL
outcome TEXT
side TEXT NOT NULL
price REAL NOT NULL
size REAL NOT NULL
notional REAL NOT NULL
transaction_hash TEXT NOT NULL
proxy_wallet TEXT
source_name TEXT NOT NULL
collected_ts INTEGER NOT NULL
meta_json TEXT
```

Indexes:

```text
UNIQUE(transaction_hash, asset_token_id, side, price, size)
INDEX(market_id, ts_utc)
INDEX(ts_utc)
```

Normalization:

- `conditionId` → `market_id`, `slug` → `market_slug`, `timestamp` (Unix seconds) → `ts_utc`, `asset` → `asset_token_id`.
- `outcome`, `side`, `price`, `size`, `transactionHash`, `proxyWallet` map directly.
- `notional = price * size`.
- Reject rows whose `asset_token_id` is neither `btc5m_markets.yes_token_id` nor `no_token_id` for the same `market_id`.
- Reject rows whose `ts_utc` is outside `[slot_start_ts, slot_end_ts)`.
- `source_name = 'polymarket_data_api_trades'`.

## Collector Script

`scripts/btc5m_trade_tick_collector.py` — same skeleton as `btc5m_resolution_collector.py`.

Identity:

```text
COLLECTOR_NAME = "btc5m-trade-tick-collector"
COLLECTOR_VERSION = <implementation date>
```

### Live mode (default)

Loop:

1. Load `polymarket_scanner/.env`, `connect_db()`, acquire single-instance lock.
2. `start_collector_run(...)`.
3. Every `INTERVAL_SEC` seconds:
   - Select markets from `btc5m_markets` where `slot_end_ts >= now - LIVE_LOOKBACK_SEC` and `slot_start_ts <= now + 300`. (Default 2-hour lookback so late-arriving fills are captured.)
   - For each market, fetch `/trades?market=<market_id>&side=BUY&limit=1000&offset=0&takerOnly=true`, then `side=SELL`. Iterate `offset` by 1000 until response is empty or 3000 cap hit.
   - Filter to `slot_start_ts <= ts_utc < slot_end_ts`. Validate `asset_token_id`. `INSERT OR IGNORE` into `btc5m_trade_ticks`.
   - `update_collector_run(...)` with row counts in `meta_json`.
4. On HTTP 429/5xx: backoff + retry per existing pattern; never crash the loop.
5. `finish_collector_run(...)` on shutdown.

### Historical fill mode

`--historical` flag changes the market selection only:

- Iterate `btc5m_markets` ordered by `slot_start_ts ASC` where `slot_end_ts < HISTORICAL_CUTOFF_TS` (default: timestamp recorded at first live-collector start).
- Optional `--from-ts` / `--to-ts` for partial passes.
- Same fetch + filter + insert path as live. UNIQUE index handles overlap with live writes.
- Exits when all markets processed.

CLI:

```text
--once               Run a single sweep and exit (live mode).
--historical         Switch to historical mode (one-shot, oldest → newest).
--max-markets N      Limit per-invocation market count.
--market-slug SLUG   Restrict to one market.
--from-ts T          Lower bound for historical mode.
--to-ts T            Upper bound for historical mode.
```

## Environment Variables

Add to `polymarket_scanner/.env.example`:

```text
BTC5M_TRADE_TICK_INTERVAL_SEC=15
BTC5M_TRADE_TICK_LIVE_LOOKBACK_SEC=7200
BTC5M_TRADE_TICK_TIMEOUT_SEC=10
BTC5M_TRADE_TICK_RETRY_COUNT=2
BTC5M_TRADE_TICK_RETRY_BACKOFF_SEC=1.0
BTC5M_TRADE_TICK_REQUEST_SLEEP_SEC=0.1
BTC5M_TRADE_TICK_TAKER_ONLY=true
BTC5M_TRADE_TICK_HISTORICAL_CUTOFF_TS=
BTC5M_TRADE_TICK_LOG_PATH=runtime/logs/btc5m_trade_tick_collector.log
BTC5M_TRADE_TICK_LOCK_PATH=runtime/locks/btc5m_trade_tick_collector.lock
```

`BTC5M_TRADE_TICK_HISTORICAL_CUTOFF_TS` is filled in after Phase 1 starts (the timestamp of the first successful live sweep, recorded in `collector_runs.meta_json`). Phase 2 reads this value as its default `--to-ts`.

## Control Integration

`control/scripts/btc5m_collection_control.ps1` — add to `$collectorMap`:

```powershell
'tradetick' = @{
    Script = 'scripts\btc5m_trade_tick_collector.py'
    WorkingDir = '.'
    Lock = 'runtime\locks\btc5m_trade_tick_collector.lock'
    Pattern = 'btc5m_trade_tick_collector\.py'
    ExeKey = 'tradetick'
}
```

Update default `-Targets` from `'scanner,reference,resolution'` to `'scanner,reference,resolution,tradetick'`.

`control/scripts/ensure_btc5m_process_exes.ps1` — add to `$processDefs`:

```powershell
tradetick = @{
    env_var = 'BTC5M_TRADE_TICK_EXE_PATH'
    file_name = 'btc5m-trade-tick.exe'
}
```

`scripts/btc5m_collection_summary.py` — add `TRADETICK_LOCK` and a `COLLECTOR_CONFIG` entry:

```python
"tradetick": {
    "collector_name": "btc5m-trade-tick-collector",
    "lock_path": TRADETICK_LOCK,
    "command_fragment": "btc5m_trade_tick_collector.py",
},
```

Plus a row count + freshness metric (`latest_trade_tick_age_sec`) on summary output.

## Phase 1 — Live Rollout

1. Add `btc5m_trade_ticks` to `TABLE_SPECS`. Run `python scripts/btc5m_verify_setup.py` to confirm migration.
2. Implement `scripts/btc5m_trade_tick_collector.py`.
3. Smoke test: `python scripts/btc5m_trade_tick_collector.py --once --max-markets 1`.
4. Add the control / summary entries above.
5. Start: `powershell control/scripts/btc5m_collection_control.ps1 -Action start -Targets tradetick`.
6. Verify `btc5m_collection_summary.py` reports it healthy and row count grows.
7. Capture `T_live_start = MIN(started_ts)` from `collector_runs WHERE collector_name='btc5m-trade-tick-collector'` and store it as `BTC5M_TRADE_TICK_HISTORICAL_CUTOFF_TS` in `.env` (or pass via CLI in Phase 2).

## Phase 2 — Historical Fill

After Phase 1 has been running stably for at least a few hours:

```powershell
python scripts\btc5m_backup_dataset.py
python scripts\btc5m_trade_tick_collector.py --historical
```

This walks `btc5m_markets` from the oldest slot to `HISTORICAL_CUTOFF_TS`, fetching trades per market. Live collector keeps running; UNIQUE index dedupes the small overlap window.

If the run is too long for one invocation, use `--from-ts` / `--to-ts` to chunk it:

```powershell
python scripts\btc5m_trade_tick_collector.py --historical --from-ts 1740000000 --to-ts 1742500000
python scripts\btc5m_trade_tick_collector.py --historical --from-ts 1742500000 --to-ts 1745000000
```

Resume is implicit: re-running with the same window is safe because of `INSERT OR IGNORE` on the UNIQUE index. No separate progress table.

## Acceptance

- `btc5m_trade_ticks` exists, populated by both live and historical runs without duplicates.
- Live collector runs alongside scanner / reference / resolution without affecting their loops.
- `btc5m_collection_summary.py` shows the new collector's process state, row count, and freshness.
- After Phase 2, `btc5m_trade_ticks` covers every market from `MIN(btc5m_markets.slot_start_ts)` to current.
- Existing `v1` features, labels, and decision dataset behavior unchanged.

## References

- [Polymarket Data API - Trades](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets)
- [Polymarket API rate limits](https://docs.polymarket.com/api-reference/rate-limits)
