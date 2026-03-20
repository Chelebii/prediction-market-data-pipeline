# BTC5M Scanner Overview

## Purpose

The BTC5M scanner continuously discovers and validates the active Polymarket BTC 5-minute market, then publishes the freshest usable snapshot for the dataset pipeline.

## Inputs

- Polymarket Gamma market discovery responses
- Polymarket CLOB price, midpoint, and order book data
- local scanner configuration from `polymarket_scanner/.env`

## Outputs

- `runtime/snapshots/btc_5min_clob_snapshot.json`
- dataset rows in `runtime/data/btc5m_dataset.db`
- scanner logs and lock files

## Why It Matters

- the snapshot is the real-time view used by operational monitoring
- the dataset DB depends on the scanner for market discovery and quote coverage
- stale or invalid scanner output reduces downstream dataset quality

## Main Responsibilities

- discover the current BTC 5-minute market
- validate quote structure and complement consistency
- publish only stable candidate markets
- write snapshot, order book depth, and lifecycle rows into the dataset
- surface actionable network and freshness problems through logs and alerts

## Quick Operator Checks

1. Confirm `runtime/snapshots/btc_5min_clob_snapshot.json` exists.
2. Confirm the snapshot timestamp is fresh.
3. Confirm the active market slug and quote fields are populated.
4. Confirm the collector status shows `RUNNING`.

## Fail-Safes

- single-instance lock protection
- warm-up gate before publishing a new candidate market
- no-data alerting with slot-aware grace logic
- structured invalid vs semantic reject separation for diagnostics

## Role In The Pipeline

- The scanner is the market-ingestion backbone of the BTC5M dataset pipeline.
- Reference and resolution collectors enrich the dataset around it.
- Audit, health, backup, and ETL jobs consume the resulting dataset state.
