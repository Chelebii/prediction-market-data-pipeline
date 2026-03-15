# BTC5M Live Data Collection Runbook

Last updated: 2026-03-15

## Goal

Run the BTC5M dataset collectors unattended so the machine can keep collecting usable research data overnight without starting the trading bot.

## Required Processes

Long-running:
- `polymarket_scanner/btc_5min_clob_scanner.py`
- `scripts/btc5m_reference_collector.py`
- `scripts/btc5m_resolution_collector.py`

Periodic:
- `scripts/btc5m_healthcheck.py`
- `scripts/btc5m_audit_dataset.py`
- `scripts/btc5m_backup_dataset.py`

## Control Scripts

Collector control:
- `control/scripts/btc5m_collection_control.ps1`

Task Scheduler registration:
- `control/scripts/register_btc5m_collection_tasks.ps1`

## Startup + Task Scheduler Layout

- `control/scripts/start_btc5m_collectors.cmd`
  User logon startup entry. Starts `scanner + reference + resolution`.
- `5minbots BTC5M Health Check`
  Runs every 5 minutes.
- `5minbots BTC5M Dataset Audit`
  Runs every 15 minutes.
- `5minbots BTC5M Dataset Backup`
  Runs every hour.

## Health Outputs

Logs:
- `runtime/logs/btc5m_healthcheck.log`
- `runtime/logs/btc5m_audit_dataset.log`
- `runtime/logs/btc5m_backup_dataset.log`

Machine-readable status:
- `runtime/monitoring/btc5m_collection_health.json`

## Freshness Gates

Default thresholds:
- snapshot freshness: `45s`
- reference freshness: `10s`
- audit freshness: `30m`

Config source:
- `polymarket_scanner/.env`

## Backup Policy

Hourly SQLite backup:
- output dir: `runtime/backups`
- keep count: `72`

## Manual Commands

Start collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action start
```

Stop collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action stop
```

Collector status:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action status
```

Register scheduler tasks:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\register_btc5m_collection_tasks.ps1 -Action register
```

Task status:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\register_btc5m_collection_tasks.ps1 -Action status
```

## Operational Notes

- The machine must stay awake and online. If Windows sleep triggers, collectors stop.
- This setup is only for data collection. `polymarket_paper_bot_5min/polymarket_paper_bot.py` is not required.
- First success criterion is not PnL. First success criterion is:
  - DB growing
  - health checks green
  - audit moving toward `PASS`
  - no long freshness gaps
