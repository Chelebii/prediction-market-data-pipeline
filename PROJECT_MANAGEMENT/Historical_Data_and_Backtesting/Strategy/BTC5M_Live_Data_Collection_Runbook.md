# BTC5M Live Data Collection Runbook

Last updated: 2026-03-18

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

## Collector Executables

The long-running collectors now start with collector-specific executable image names.
This is required for VPN Split Tunnel so only BTC5M data collection traffic is forced through the VPN.

Expected process image names:
- `btc5m-scanner.exe`
- `btc5m-reference.exe`
- `btc5m-resolution.exe`

Default executable paths on this machine:
- `C:\Users\mavia\AppData\Local\Programs\Python\Python311\btc5m-scanner.exe`
- `C:\Users\mavia\AppData\Local\Programs\Python\Python311\btc5m-reference.exe`
- `C:\Users\mavia\AppData\Local\Programs\Python\Python311\btc5m-resolution.exe`

Collector purpose:
- `btc5m-scanner.exe`
  - Runs `polymarket_scanner/btc_5min_clob_scanner.py`
  - Collects BTC 5MIN Polymarket orderbook/snapshot/lifecycle rows
- `btc5m-reference.exe`
  - Runs `scripts/btc5m_reference_collector.py`
  - Collects BTC spot reference ticks
- `btc5m-resolution.exe`
  - Runs `scripts/btc5m_resolution_collector.py`
  - Collects official Polymarket market resolution updates

The process image names are created by:
- `control/scripts/ensure_btc5m_process_exes.ps1`

Notes:
- These are renamed Python interpreter executables, not wrapper-only launchers.
- Command lines still include the `.py` script path for debugging.
- If the renamed executables are missing, the control script can fall back to plain `python.exe`, but Split Tunnel selection should use the renamed executables.

## Control Scripts

Collector control:
- `control/scripts/btc5m_collection_control.ps1`
- `control/scripts/ensure_btc5m_process_exes.ps1`

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
  User logon startup entry. Ensures collector-specific executables exist, opens one persistent monitor console, and best-effort starts `scanner + reference + resolution`.
- `5minbots BTC5M Health Check`
  Runs every 5 minutes.
- `5minbots BTC5M Dataset Audit`
  Runs every 15 minutes.
- `5minbots BTC5M Dataset Backup`
  Runs every 6 hours.

Important:
- Periodic tasks use wrapper `.cmd` files that first `cd` into the repo root.
- Python scripts also normalize relative env paths against repo root, so they no longer depend on Task Scheduler working directory.
- Task Scheduler tarafinda periodic jobs `wscript.exe` + hidden `.vbs` wrapper ile calisir; bu sayede console flash yapmaz.
- Startup tarafinda tek gorunen console `control/scripts/btc5m_console_monitor.ps1` olur.
- Monitor pencere title ile durum gosterir; stdout'a sadece state degisince veya problem olunca yazar.

## NordVPN Split Tunnel Guidance

Split Tunnel tarafinda secilecek uygulamalar sadece long-running BTC5M collectors olmalidir:
- `btc5m-scanner.exe`
- `btc5m-reference.exe`
- `btc5m-resolution.exe`

Do not select these for Split Tunnel:
- `python.exe`
- `powershell.exe`
- `wscript.exe`
- `cmd.exe`

Reason:
- `python.exe` cok genis olur ve repo disindaki baska Python araclarini da etkiler.
- `powershell.exe`, `wscript.exe`, `cmd.exe` sadece orchestration/task wrapper gorevi gorur.
- Asil network traffic yapan collector process'leri `btc5m-*.exe` isimleriyle calisir.

Recommended operator flow:
1. Register/start the BTC5M collection stack normally.
2. Open Task Manager or run collector status command to confirm the 3 image names gorunuyor:
   - `btc5m-scanner.exe`
   - `btc5m-reference.exe`
   - `btc5m-resolution.exe`
3. NordVPN Split Tunnel listesinde bu 3 executable'i sec.
4. VPN on/off sonrasi morning summary ve monitor ile collectors'in hala `RUNNING` oldugunu dogrula.

Verification commands:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action status
python scripts\btc5m_collection_summary.py
```

Expected output should show:
- `scanner ... image=btc5m-scanner.exe`
- `reference ... image=btc5m-reference.exe`
- `resolution ... image=btc5m-resolution.exe`

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

6-hour SQLite backup:
- output dir: `runtime/backups`
- keep count: `28`
- effective retention: `~7 days`
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

Ensure collector-specific executables exist:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\ensure_btc5m_process_exes.ps1
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
- Startup, monitor, health, summary, and dashboard now expect collector-specific image names. If a collector is manually started with plain `python.exe`, Split Tunnel isolation becomes ambiguous and operator visibility can degrade to legacy fallback mode.
- First success criterion is not PnL. First success criterion is:
  - DB growing
  - health checks green
  - audit moving toward `PASS`
  - no long freshness gaps

## Troubleshooting

If Split Tunnel does not show the expected app names:
1. Run:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\ensure_btc5m_process_exes.ps1
```

2. Restart collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action restart
```

3. Confirm status:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action status
```

If status still shows `python.exe`:
- a legacy collector may still be running
- stop all collectors, then start again using the control script
- verify the renamed executable files exist at the Python install path

If dashboard or monitor shows scanner `STOPPED` unexpectedly:
- check the scanner lock file:
  - `polymarket_scanner/btc_5min_clob_scanner.lock`
- run:

```powershell
python scripts\btc5m_collection_summary.py --json
```

- confirm `collectors.scanner.process_image_name == btc5m-scanner.exe`
