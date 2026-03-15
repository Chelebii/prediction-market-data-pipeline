# 5MIN Live Runbook

Last updated: 2026-03-13

## Purpose

This document records the work done to move the BTC 5MIN scanner + 5MIN bot from paper-only behavior toward safe live-trading readiness, including the browser-wallet / proxy-wallet configuration now used for the small-wallet live test profile.

File location:
- `polymarket_paper_bot_5min/5MIN_LIVE_RUNBOOK.md`

## What Changed

### 1. 5MIN bot live execution wiring

Updated:
- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Changes:
- Added explicit `open_position()` and `close_position()` wrappers for `paper`, `dry-run`, and `live`.
- Connected the 5MIN strategy to `common.execution.ExecutionEngine`.
- Expanded the local SQLite schema so 5MIN positions can store:
  - `market_id`
  - `question`
  - `token_id`
  - `pnl_pct`
  - `high_price`
  - migrated `order_id`
  - migrated `trading_mode`
  - migrated `fill_price`
- Added signal journal writes for both OPEN and CLOSE in live-compatible format.
- Added live startup checks:
  - credential validation
  - minimum collateral balance check

### 2. Down-side token semantics fix

Updated:
- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Problem that existed before:
- `Down` positions were being evaluated with inconsistent PnL logic.
- That was acceptable neither for real-money tracking nor for live exits.

Fix:
- `Down` is now treated as the actual `NO token`.
- Equity and PnL are calculated as token-price appreciation on the selected token, same as `Up`.
- Legacy `TARGET_EXIT_PRICE_DOWN=0.15` values are normalized to the actual `NO-token` target, but the `.env` should now store the explicit `NO-side` target directly.

Operational note:
- Old paper results for historical `Down` trades should not be used as reliable live-quality evidence.

### 3. Execution layer safety changes

Updated:
- `common/execution.py`
- `common/clob_client.py`

Changes:
- Added missing `requests` import required by first-live-trade approval flow.
- Switched live opens from resting limit-style path to `market_buy`.
- Switched live closes from resting limit-style path to `market_sell`.
- `market_sell` now accepts `shares`, which matches Polymarket market-sell semantics.
- Live orders now use immediate-execution `FOK` path instead of leaving a `GTC` order resting on the book.
- Live orders must now pass a verification gate before local DB state is updated.
- If a live open/close cannot be verified, the bot activates the kill switch instead of writing an assumed fill.
- Daily loss limit enforcement now uses the configured bot limit instead of a hard-coded default.
- Entry/exit accounting now derives fill price from the live order response whenever the CLOB response includes matched amount fields.

### 4. Proxy-wallet live support

Updated:
- `common/clob_client.py`
- `common/execution.py`
- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Changes:
- Added direct CLOB `collateral balance` reads through `get_balance_allowance`.
- Live execution now prefers Polymarket account collateral balance over raw Polygon EOA USDC balance.
- Live startup no longer incorrectly requires `POLY_FUNDER_ADDRESS == signer wallet` when `POLY_SIGNATURE_TYPE != 0`.
- Browser-wallet / proxy-wallet mode is now supported with:
  - `POLY_FUNDER_ADDRESS=<proxy wallet>`
  - `POLY_SIGNATURE_TYPE=2`
- Added explicit `LIVE_ALLOW_UNPROVEN=1` override for the first micro live test profile.

Operational note:
- For Phantom / Polymarket web accounts, the live bot should use the Polymarket `proxy wallet` as funder.
- The signer private key can still be the exported EVM key from the browser wallet account.

### 5. 5MIN manager hardening

Updated:
- `polymarket_paper_bot_5min/manager.py`

Changes:
- Added registry trading-mode updates for 5MIN.
- Added config-driven run start balance.
- Added live crash/bankrupt kill-switch activation path.
- Added `BOT_LABEL` propagation and consistent live/dry-run STARTED notification flow.

### 6. Scanner snapshot enrichment

Updated:
- `polymarket_scanner/btc_5min_clob_scanner.py`

Changes:
- Snapshot now publishes top-level `question`.
- This gives the 5MIN bot cleaner market context for journaling and live execution metadata.

## Current Live Profile

The repo is now configured for the current small-wallet live profile:

- `TRADING_MODE=live`
- `INITIAL_BALANCE=19.84`
- `POSITION_SIZE_USD=5`
- `ENTRY_MIN_PRICE=0.08`
- `MAX_OPEN_POSITIONS=1`
- `MAX_TRADES_PER_SLOT=1`
- `MAX_POSITION_SIZE_USD=5`
- `DAILY_LOSS_LIMIT_USD=18`
- `ENTRY_MAX_PRICE=0.60`
- `ENTRY_CUTOFF_SEC=180`
- `MIN_ENTRY_SEC=10`
- `MOMENTUM_MIN_PCT=0.08`
- `FALLBACK_SIGNAL_MIN_PCT=0.06`
- `SHORT_MOMENTUM_TOLERANCE_PCT=0.02`
- `FALLBACK_ENTRY_MAX_PRICE=0.60`
- `FALLBACK_MAX_ENTRY_SPREAD=0.08`
- `FALLBACK_MIN_QUOTE_STABLE_PASSES=1`
- `MAX_LOSS_USD_PER_TRADE=2`
- `PRINCIPAL_TAKE_MULTIPLIER=1.6`
- `RUNNER_FINAL_TARGET_PRICE=0.97`
- `RUNNER_TRAILING_STOP_PCT=0.18`
- `TRADE_NOTIFICATIONS_ENABLED=0`
- `MAX_TOTAL_DRAWDOWN_USD=18`
- `LIVE_MIN_CLOSED_TRADES=30`
- `LIVE_ALLOW_UNPROVEN=1`
- `POLY_SIGNATURE_TYPE=2`
- `POLY_FUNDER_ADDRESS=<proxy wallet>`

## Latest Pre-Live Hardening

Before the first live funding, three additional runtime issues were fixed:

- Live CLOB `worst_price` caps/floors are now aligned to market `tick_size` before orders are posted.
  This reduces avoidable live rejects from invalid price increments.
- 5MIN live open checks now use total Polygon USDC visibility when available, not only a single USDC contract path.
  This keeps the startup/live-open balance checks consistent.
- 5MIN manager now re-reads `.env` dynamically for `TRADING_MODE` and `INITIAL_BALANCE`.
  This prevents stale manager state after config changes without a full manual process cleanup.
- 5MIN now respects `min_order_size` from the scanner snapshot.
  If a tiny `$1` profile cannot satisfy venue minimum share size, the bot skips the entry.
  If a partial principal-take would fall below venue minimum size, the bot closes the full position at the principal trigger instead of sending a doomed partial reduce.
- `LIVE_MIN_NET_PNL_USD=5`
- `MIN_QUOTE_STABLE_PASSES=2`
- `CONSECUTIVE_LOSS_COOLDOWN_TRIGGER=3`
- `LOSS_COOLDOWN_SLOTS=2`

This profile is for the current constrained live run using the Polymarket proxy-wallet balance.

Operational note:
- For the small-wallet live profile, `ENTRY_MIN_PRICE` remains `0.08`.
- The old `0.20` floor conflicted with the venue `min_order_size=5` constraint and made entries practically unreachable once preview slippage was applied.
- The live logic still relies on cheap entries, but it must allow sub-`0.20` tokens so the venue minimum share size can be satisfied.

Notification policy:
- 5MIN sends Telegram messages for process lifecycle (`STARTED`, crash/stop paths) and actual errors.
- Trade entry/exit execution messages are suppressed for 5MIN to avoid noise.

Strategy model:
- Only low-price entries are allowed so a real `2x` move can recover principal before expiry.
- Entry signal is now hybrid:
  - primary: last 2 closed `1m` candles in the same direction
  - fallback: last closed `1m` candle direction plus lower-but-still-confirmed momentum
- Fallback entries are intentionally stricter on execution quality:
  - cheaper entry cap
  - tighter spread cap
  - more quote-stability passes
- Hard stop is sized from dollars, not percent: `$5` entry size with approximately `$2` max intended loss.
- At `2x` token price, the bot sells enough size to recover principal and leaves a smaller runner.
- Runner then exits either near resolution (`0.97`) or on an `18%` trailing stop from the post-entry high.
- Quotes must remain stable across multiple scans before a new entry is allowed.
- After repeated losses, the bot enters a slot-based cooldown instead of forcing more entries into a weak regime.
- Once enough paper sample exists, recent performance must stay healthy or new entries are paused.

Capital protection:
- Live market orders now use explicit worst-price caps instead of uncapped market execution.
- If total equity drawdown reaches `$18` from the configured start balance, 5MIN stops and the manager does not auto-restart it.
- Live mode is blocked until there is enough paper evidence: at least `30` closed paper trades, at least `$5` net paper PnL, acceptable profit factor, and acceptable force-exit/stop-loss rates.
- The current small-wallet live profile explicitly overrides the paper-evidence gate with `LIVE_ALLOW_UNPROVEN=1`.

## Activation Sequence

### Phase 1: Paper

Use this first.

Requirements:
- scanner must be running
- 5MIN bot must be running
- Telegram alerts should be working

Validation goals:
- scanner snapshot age remains fresh
- OPEN/CLOSE events make sense
- signal journal and DB entries are consistent
- no unexpected `NOSNAP` or frequent forced exits caused by stale data
- total paper sample reaches the live gate (`30` closed trades, positive net edge)

### Phase 2: Dry-run

Only after paper results look sane.

Switch:
- change `TRADING_MODE=paper` to `TRADING_MODE=dry-run`

Goal:
- validate live execution wiring without sending real orders

### Phase 3: Live with very small size

Only after both paper and dry-run look correct.

Before switching:
- fill all `POLY_*` credentials in `polymarket_paper_bot_5min/.env`
- confirm wallet has enough USDC on Polygon
- keep `POSITION_SIZE_USD=1`
- keep `MAX_OPEN_POSITIONS=1`
- keep `DAILY_LOSS_LIMIT_USD=18`

Switch:
- change `TRADING_MODE=dry-run` to `TRADING_MODE=live`

Expected runtime behavior:
- first live trade requires Telegram approval
- if the bot reports bankrupt exit code in live mode, manager activates kill switch

## Remaining Risks

These are still open and should be understood before real money:

1. Live ledger precision is improved but still not broker-grade.
- The bot now avoids assumed closes on failed live sells and uses response-derived fill prices when available.
- It still does not run a full exchange reconciliation pipeline against post-trade settlements.

2. Snapshot dependency remains critical.
- If scanner data stalls, the bot can only act on what it last saw.
- The architecture is still file-based snapshot polling, not event-driven orderbook streaming.

3. 5MIN markets are short-duration.
- Fast expiry means delayed or stale data hurts this bot more than slower strategies.

## Worst-Case Live Scenarios

These are the realistic worst cases once `TRADING_MODE=live`.

### 1. Worst single-trade scenario

Scenario:
- The bot opens a live position near the top of its allowed entry band.
- The market moves sharply against the bot.
- The live close path is delayed, rejected, or cannot be verified before expiry.
- The contract resolves at or near zero for the selected token.

Impact:
- One trade can lose almost the full notional allocated to that position.
- With the current config, that means roughly the `$1` position size, plus fees/slippage.

Current protection:
- `MAX_OPEN_POSITIONS=1`
- `POSITION_SIZE_USD=1`
- worst-price caps on live market orders
- kill switch activation on live close/open verification failure
- no synthetic "successful" close is written on failed live exits

Best solution:
- Keep live size at `$1` until reconciliation is proven.
- Add post-trade reconciliation against actual exchange fills before increasing size.
- Keep a dedicated low-balance live wallet instead of a general-purpose wallet.

### 2. Worst automated session scenario under the current guardrails

Scenario:
- The strategy has a bad regime day.
- Scanner data is valid enough to trade, but market behavior does not match the strategy edge.
- Several trades lose before the runtime health gate shuts entries down.

Impact:
- Multiple small losses can accumulate before the bot stops itself.
- With the current config, the intended automated stop point is around:
  - `DAILY_LOSS_LIMIT_USD=2`
  - `MAX_TOTAL_DRAWDOWN_USD=5`

Current protection:
- runtime health gate
- consecutive-loss cooldown
- daily loss limit
- total drawdown stop with manager no-restart behavior

Best solution:
- Do not raise `POSITION_SIZE_USD`, `DAILY_LOSS_LIMIT_USD`, or `MAX_TOTAL_DRAWDOWN_USD` until paper and dry-run evidence is strong.
- Treat the current `$5` total drawdown stop as a hard capital preservation boundary.
- Review every block/stop event before re-enabling live trading.

### 3. Worst system-state scenario

Scenario:
- A live order reaches the venue, but the bot cannot confidently confirm its fill state because of API inconsistency, timeout, or partial-response ambiguity.
- Local bot state and actual venue state diverge.

Impact:
- The bot may correctly stop itself, but a manual operator may still need to inspect or flatten exposure.
- This is operationally more dangerous than a normal losing trade because it creates state uncertainty.

Current protection:
- live order verification gate
- kill switch on unverified live open/close
- first-trade approval
- single-position mode

Best solution:
- Add a dedicated reconciliation command that compares:
  - local `paper_positions`
  - open CLOB orders
  - recent account trades/fills
- Require manual review after any kill-switch event before restarting live mode.
- Keep dashboard and logs open during early live sessions.

### 4. Absolute worst account outcome

Scenario:
- The strategy has no live edge over time, and the operator repeatedly keeps the bot running or restarts it after protection triggers.
- Or the operator raises size/limits too early.

Impact:
- Over enough time, the account can still be drained.
- Automation reduces loss speed; it does not make the account impossible to lose.

Current protection:
- paper evidence gate before live
- small size
- drawdown stop
- health gate

Best solution:
- Only fund the amount you are willing to ring-fence for this system.
- Do not override stop conditions to "give it one more chance".
- Require a new paper sample after any meaningful strategy or scanner change.

## Practical Rule

If live trading starts now with the current profile, the bot should not be able to burn the full `$20` test wallet quickly by design.
The main ways to lose much more than intended are:
- increasing size or limits too early
- manually restarting after protective stops
- trading live without a verified positive paper sample
- ignoring a kill-switch or reconciliation event

Operational recommendation:
- first live funding should stay small
- first live session should be supervised
- size should only increase after both paper and live logs show stable behavior

## Recommended Files To Check During Operations

- `runtime/snapshots/btc_5min_clob_snapshot.json`
- `polymarket_paper_bot_5min/.env`
- `polymarket_paper_bot_5min/manager.py`
- `polymarket_paper_bot_5min/polymarket_paper_bot.py`
- `polymarket_paper_bot_5min/runs/<active_run>/bot.log`
- `polymarket_paper_bot_5min/runs/<active_run>/paper_trades.db`
- `polymarket_paper_bot_5min/runs/<active_run>/trade_events.csv`
- `polymarket_paper_bot_5min/runs/<active_run>/closed_trades.csv`

## CSV Trade Export

Each run now writes CSV trade records automatically:

- `trade_events.csv`
  - appends every `OPEN`, `REDUCE`, and `CLOSE` event
  - includes price, size, order id, mode, signal metadata, and event-level PnL
- `closed_trades.csv`
  - appends one summary row per fully closed trade
  - includes entry price, exit price, original size, realized PnL, percent PnL, close reason, and timestamps

These files are written in both paper and live modes, so the live session will already have the same export format.

## Quick Commands

Compile check:

```powershell
python scripts\verify_compile.py
```

Start stack from existing control flow:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\bots_control.ps1 start
```

Stop only 5MIN from dashboard or control flow before changing mode.

## Notes For Future Changes

- If `TARGET_EXIT_PRICE_DOWN` is touched again, keep it in actual `NO token` terms, not complementary `YES` terms.
- Do not revert live execution back to `GTC` unless the strategy is explicitly redesigned for resting orders.
- If broker-grade live accounting is required, add a post-order reconciliation layer against actual fills before increasing size.

## Incident Notes

- Expiry sonrasi `no orderbook` kalan runner incident'i icin ayri dokuman:
  - `polymarket_paper_bot_5min/INCIDENT_2026-03-14_LIVE_CLOSE_NO_ORDERBOOK.md`
