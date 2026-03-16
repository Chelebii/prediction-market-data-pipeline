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
- `scripts/btc5m_collection_summary.py`

## Control Scripts

Collector control:
- `control/scripts/btc5m_collection_control.ps1`

Task Scheduler registration:
- `control/scripts/register_btc5m_collection_tasks.ps1`
- periodic task wrappers:
  - `control/scripts/run_btc5m_healthcheck.cmd`
  - `control/scripts/run_btc5m_dataset_audit.cmd`
  - `control/scripts/run_btc5m_backup_dataset.cmd`
  - hidden scheduler wrappers:
    - `control/scripts/run_btc5m_healthcheck_hidden.vbs`
    - `control/scripts/run_btc5m_dataset_audit_hidden.vbs`
    - `control/scripts/run_btc5m_backup_dataset_hidden.vbs`

## Startup + Task Scheduler Layout

- `control/scripts/start_btc5m_collectors.cmd`
  User logon startup entry. Opens one persistent monitor console and best-effort starts `scanner + reference + resolution`.
- `5minbots BTC5M Health Check`
  Runs every 5 minutes.
- `5minbots BTC5M Dataset Audit`
  Runs every 15 minutes.
- `5minbots BTC5M Dataset Backup`
  Runs every hour.

Important:
- Periodic tasks use wrapper `.cmd` files that first `cd` into the repo root.
- Python scripts also normalize relative env paths against repo root, so they no longer depend on Task Scheduler working directory.
- Task Scheduler tarafinda periodic jobs `wscript.exe` + hidden `.vbs` wrapper ile calisir; bu sayede console flash yapmaz.
- Startup tarafinda tek gorunen console `control/scripts/btc5m_console_monitor.ps1` olur.
- Monitor pencere title ile durum gosterir; stdout'a sadece state degisince veya problem olunca yazar.

## Health Outputs

Logs:
- `runtime/logs/btc5m_healthcheck.log`
- `runtime/logs/btc5m_audit_dataset.log`
- `runtime/logs/btc5m_backup_dataset.log`

Machine-readable status:
- `runtime/monitoring/btc5m_collection_health.json`

Morning summary:
- `scripts/btc5m_collection_summary.py`
  Shows collector status, DB counts, latest audit, latest backup, freshness, and warnings.

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
- filename format: `btc5m_dataset_YYYYMMDD_HHMMSSZ.db`
- validation: backup-copy uzerinde `PRAGMA quick_check(1)`
- latest pointer: `runtime/backups/btc5m_backup_latest.json`
- sidecar metadata: her backup icin `*.meta.json`

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

Morning ops summary:

```powershell
python scripts\btc5m_collection_summary.py
```

JSON summary:

```powershell
python scripts\btc5m_collection_summary.py --json
```

Manual backup:

```powershell
python scripts\btc5m_backup_dataset.py
```

Latest backup metadata:

```powershell
Get-Content runtime\backups\btc5m_backup_latest.json
```

## Restore / Recovery

Kural:
- Collector'lar calisirken live DB overwrite etme.
- Restore once her zaman `scanner + reference + resolution` durdur.

1. Collector'lari durdur:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action stop
```

2. Gerekirse mevcut live DB'yi quarantine kopyasi olarak ayir:

```powershell
Copy-Item runtime\data\btc5m_dataset.db runtime\data\btc5m_dataset_pre_restore.db
```

3. Restore edilecek backup'i sec ve live path'e kopyala:

```powershell
Copy-Item runtime\backups\btc5m_dataset_YYYYMMDD_HHMMSSZ.db runtime\data\btc5m_dataset.db -Force
```

4. Eski WAL/SHM kalintilari varsa temizle:

```powershell
Remove-Item runtime\data\btc5m_dataset.db-wal -ErrorAction SilentlyContinue
Remove-Item runtime\data\btc5m_dataset.db-shm -ErrorAction SilentlyContinue
```

5. Restore edilen DB'yi hizli dogrula:

```powershell
@'
import sqlite3
conn = sqlite3.connect(r"runtime\data\btc5m_dataset.db")
print(conn.execute("PRAGMA quick_check(1)").fetchone()[0])
conn.close()
'@ | python -
```

6. Collector'lari yeniden baslat:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action start
```

## Operational Notes

- The machine must stay awake and online. If Windows sleep triggers, collectors stop.
- This setup is only for data collection. `polymarket_paper_bot_5min/polymarket_paper_bot.py` is not required.
- First success criterion is not PnL. First success criterion is:
  - DB growing
  - health checks green
  - audit moving toward `PASS`
  - no long freshness gaps
