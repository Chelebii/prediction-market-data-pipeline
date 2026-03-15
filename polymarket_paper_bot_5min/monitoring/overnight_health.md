# Overnight Health

## 2026-03-14 03:41 UTC

- Active run: `Run_2026-03-14_03-12-01`
- Dashboard state: 5MIN bot `ACTIVE` (heartbeat age 4s), BTC5 scanner `ACTIVE` (heartbeat age 2s), current snapshot age 2s
- Trades: `trade_events.csv` rows=0 (`OPEN`=0, `CLOSE`=0), `closed_trades.csv` rows=0, dashboard open_count=0, closed_count=0
- New live events since previous automation check: baseline initialized on this first run; no live `OPEN` or `CLOSE` observed
- Relaxation: step 1 was already persisted in `live_relaxation_state.json` at `2026-03-14T03:35:00Z` (`ENTRY_MAX_PRICE=0.50`, `FALLBACK_ENTRY_MAX_PRICE=0.44`); no further loosening applied this check
- Runtime errors: dashboard still surfaces historical `cannot access local variable 'market'` errors from `03:20:55` and `03:21:44`, but no new runtime error appeared after the later restarts and the current bot process is healthy
- Scanner behavior: scanner stayed fresh, but there was a temporary no-valid-book / low-liquidity stretch after `03:38:21 UTC` that let the shared snapshot go stale for roughly 80-90s before recovery in the `03:40` slot
- Remaining risk: current run still has no first live `OPEN`; next checks should continue health monitoring and only consider step 2 if there is still no `OPEN` and no newer relaxation has already been applied
