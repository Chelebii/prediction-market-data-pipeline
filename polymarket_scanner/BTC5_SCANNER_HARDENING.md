# BTC5 Scanner Hardening

Last updated: 2026-03-14

## Purpose

This note records the scanner-side hardening added for the BTC 5MIN flow before any real-money activation.

File location:
- `polymarket_scanner/BTC5_SCANNER_HARDENING.md`

## What Changed

Updated:
- `polymarket_scanner/btc_5min_clob_scanner.py`
- `polymarket_scanner/.env`

### 1. Current-slot-first publication

The scanner now prefers the current 5-minute market and does not publish the next-slot market early.

Rule:
- publish current slot when valid
- only consider next slot very near rollover (`BTC_5MIN_NEXT_SLOT_PUBLISH_AFTER_SEC`)

Reason:
- the 5MIN bot trades the current slot
- publishing next-slot data too early can make the system look "ready" while the bot still cannot act on it

### 2. Stable publish gate

The scanner now requires the same candidate market to pass validation multiple times before publishing.

Rule:
- `BTC_5MIN_MIN_STABLE_PASSES=2`

Reason:
- filters single-scan spikes and transient quote glitches

### 3. Side quote sanity checks

Each side must now pass all of these:
- `BUY` and `SELL` prices must exist
- midpoint must exist
- `BUY <= SELL`
- spread must be positive and within the configured max
- midpoint must be close to the derived half-spread midpoint
- `price-mid gap` must stay within threshold

### 4. Cross-market consistency checks

YES and NO quotes must also agree with each other:
- `yes_mid + no_mid ~= 1`
- `yes_bid + no_ask ~= 1`
- `yes_ask + no_bid ~= 1`
- market liquidity must clear the configured minimum

Reason:
- a binary market should remain internally complementary
- if these sums drift too far, the quote is not safe enough to publish

### 5. Corrected `/book` best-price extraction

The scanner now derives:
- best bid as the maximum bid price
- best ask as the minimum ask price

Reason:
- raw `/book` arrays were not safe to trust by position alone in observed runtime data

Operational note:
- the published snapshot still uses `/price` + `/midpoint` as the primary tradeable quote
- `/book` values are retained in metadata for diagnostics and additional confidence checks

### 6. Slot-aware no-data alerting

The scanner no-data alert now understands slot rollover.

Rule:
- if the last published snapshot belongs to the previous slot
- and the new slot is still within a short grace period
- do not send a "fresh snapshot publish edemedi" alert yet

Reason:
- the old logic measured only wall-clock time since the last valid publish
- this produced noisy alerts when the previous slot ended with validation-heavy rejects and the new slot was still warming up

Additional adjustment:
- `spread=0.0000` is no longer rejected as invalid by itself
- only negative spread or spread above the configured max is rejected

## Active Thresholds

Current `.env` profile:
- `BTC_5MIN_SCAN_INTERVAL_SEC=3`
- `BTC_5MIN_MAX_SIDE_MID_DEVIATION=0.01`
- `BTC_5MIN_MAX_COMPLEMENT_GAP=0.03`
- `BTC_5MIN_MIN_LIQUIDITY=5000`
- `BTC_5MIN_NEXT_SLOT_PUBLISH_AFTER_SEC=295`
- `BTC_5MIN_MIN_STABLE_PASSES=2`
- `BTC_5MIN_NO_DATA_NEW_SLOT_GRACE_SEC=20`

## Runtime Result

After hardening:
- scanner restarts cleanly
- snapshot remains fresh
- direct checks against live CLOB `price` and `midpoint` match the published snapshot
- malformed or unstable passes now log `WARMUP` or `SKIP` instead of publishing immediately

## Residual Risk

This significantly improves scanner trustworthiness, but does not remove all external risk:
- Polymarket API behavior can still change
- exchange-side outages or temporary quote anomalies are still possible
- strategy profitability is a separate problem from data correctness
