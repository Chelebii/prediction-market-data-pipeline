# BTC5M Live Data Collection Runbook

Last updated: 2026-04-05

## Goal

Run the BTC5M dataset collectors unattended so the machine can keep collecting usable research data overnight.

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

Typical Windows install paths:
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-scanner.exe`
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-reference.exe`
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-resolution.exe`

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
- By default they are created next to the real CPython binary, so the running collectors keep a real `btc5m-*.exe` process image instead of redirecting through `python.exe`.
- The launch scripts prepend `repo_root` and `.venv\Lib\site-packages` to `PYTHONPATH`, so collector dependencies still come from the repo environment.
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

Periodic task process image names:
- `btc5m-healthcheck.exe`
- `btc5m-dataset-audit.exe`
- `btc5m-backup-dataset.exe`

## Startup + Task Scheduler Layout

- `control/scripts/start_btc5m_collectors.cmd`
  User logon startup entry. Ensures collector-specific executables exist, opens one persistent monitor console, and best-effort starts `scanner + reference + resolution`.
- `Prediction Market Data Pipeline BTC5M Health Check`
  Runs every 5 minutes.
- `Prediction Market Data Pipeline BTC5M Dataset Audit`
  Runs every 15 minutes.
- `Prediction Market Data Pipeline BTC5M Dataset Backup`
  Runs every 6 hours.

Important:
- Periodic tasks use wrapper `.cmd` files that first `cd` into the repo root.
- Python scripts also normalize relative env paths against repo root, so they no longer depend on Task Scheduler working directory.
- Task Scheduler periodic jobs run through `wscript.exe` plus a hidden `.vbs` wrapper so they do not flash a console window, but the actual Python worker now runs under a BTC5M-specific `.exe` image instead of generic `python.exe`.
- The only visible startup console should be `control/scripts/btc5m_console_monitor.ps1`.
- The monitor updates the window title with status and only prints to stdout on state changes or operator-relevant problems.

## NordVPN Split Tunnel Guidance

Only the long-running BTC5M collectors should be selected for Split Tunnel:
- `btc5m-scanner.exe`
- `btc5m-reference.exe`
- `btc5m-resolution.exe`

Do not select these for Split Tunnel:
- `python.exe`
- `powershell.exe`
- `wscript.exe`
- `cmd.exe`
- `btc5m-healthcheck.exe`
- `btc5m-dataset-audit.exe`
- `btc5m-backup-dataset.exe`

Reason:
- `python.exe` is too broad and can affect unrelated Python tools on the machine.
- `powershell.exe`, `wscript.exe`, and `cmd.exe` are only orchestration wrappers.
- The collector processes that generate the actual Polymarket-facing network traffic run as `btc5m-scanner.exe`, `btc5m-reference.exe`, and `btc5m-resolution.exe`.
- Periodic maintenance jobs have their own names for process hygiene, but they are not the intended VPN-routed apps.

Recommended operator flow:
1. Register/start the BTC5M collection stack normally.
2. Open Task Manager or run the collector status command to confirm the three image names are present:
   - `btc5m-scanner.exe`
   - `btc5m-reference.exe`
   - `btc5m-resolution.exe`
3. Select those three executables in the NordVPN Split Tunnel list.
4. After VPN changes, confirm via the morning summary and monitor that the collectors still show `RUNNING`.

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

## Monitoring Interpretation

Use these fields as the primary operator signals.

Health JSON:
- `startup_age_sec`
  - Active collector runtime age.
  - This should track the currently running collector start time, not the oldest historical dataset run.
- `startup_started_ts`
  - Active startup timestamp used to compute `startup_age_sec`.
- `historical_first_run_age_sec`
  - Historical dataset age.
  - This is for context only and should not be treated as restart drift.

Summary JSON / text:
- `collectors.reference.latest_run.error_count`
  - Lifetime error count for the current long-running reference collector run.
  - By itself this is not an active incident signal.
- `collectors.<name>.recent_error`
  - Active collector error signal for the recent window.
  - Treat `recent_error.active=true` as the meaningful incident indicator.
  - If `error_count > 0` but `recent_error.active=false`, interpret this as historical error residue on a still-healthy run.
- `scanner_activity`
  - Recent scanner lifecycle trend window built from `PUBLISHED`, `WARMUP`, and `REJECTED` events.
  - Use `reject_ratio`, `warmup_ratio`, and `top_reject_reasons` to decide whether scanner quality is degrading or just showing expected gating behavior.

Healthy interpretation:
- `issues=[]` and `warnings=[]`
- collectors running
- snapshot/reference freshness near real time
- `recent_error.active=false`
- `scanner_activity.reject_ratio` stable and bounded while audit remains `PASS`

Degraded interpretation:
- freshness stale, collector not running, or audit material fail
- `recent_error.active=true` for the relevant collector
- `scanner_activity.top_reject_reasons` concentrates on the same validation class and reject ratio trends upward across windows

Current expected scanner behavior:
- `WARMUP` is normal when a candidate has not yet satisfied stability passes.
- `REJECTED` with reasons like `side_snapshot_invalid`, `*.mid_deviation`, `*.price_mid_gap`, `cross.bid_ask_gap`, or `cross.mid_sum_gap` is part of quote gating.
- Treat this as an ops issue only when the trend grows enough to impact freshness or audit quality.

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
- validation: run `PRAGMA quick_check(1)` against the backup copy
- latest pointer: `runtime/backups/btc5m_backup_latest.json`
- sidecar metadata: one `*.meta.json` file per backup

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

Health JSON:

```powershell
Get-Content runtime\monitoring\btc5m_collection_health.json
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

## Path Policy

Canonical workspace root:
- the operator-selected repo root (for example `%USERPROFILE%\Projects\prediction-market-data-pipeline`)

Rules:
- Runtime paths should resolve from repo root via shared path helpers.
- Active metadata should point at the operator's canonical repo-root-based paths for DB, logs, locks, and snapshots.
- Historical `.openclaw` or `5minbots` references in rotated logs or old backup records are migration history, not active path drift.
- If a future review shows canonical paths in current process metadata and latest backup metadata, do not treat old log lines alone as an incident.

## Restore / Recovery

Rules:
- Do not overwrite the live DB while the collectors are running.
- Always stop `scanner + reference + resolution` before restore.

1. Stop the collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action stop
```

2. If needed, preserve the current live DB as a quarantine copy:

```powershell
Copy-Item runtime\data\btc5m_dataset.db runtime\data\btc5m_dataset_pre_restore.db
```

3. Select the backup to restore and copy it to the live path:

```powershell
Copy-Item runtime\backups\btc5m_dataset_YYYYMMDD_HHMMSSZ.db runtime\data\btc5m_dataset.db -Force
```

4. Remove old WAL/SHM leftovers if present:

```powershell
Remove-Item runtime\data\btc5m_dataset.db-wal -ErrorAction SilentlyContinue
Remove-Item runtime\data\btc5m_dataset.db-shm -ErrorAction SilentlyContinue
```

5. Run a quick verification on the restored DB:

```powershell
@'
import sqlite3
conn = sqlite3.connect(r"runtime\data\btc5m_dataset.db")
print(conn.execute("PRAGMA quick_check(1)").fetchone()[0])
conn.close()
'@ | python -
```

6. Restart the collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action start
```

## Operational Notes

- The machine must stay awake and online. If Windows sleep triggers, collectors stop.
- Incident note, 2026-04-05:
  - For the local window `00:00-09:00` (Europe/London), expected slot count was `108`.
  - Only `3` slot definitions were present in the DB, so `105` slots were missing.
  - Cause: the PC was accidentally put into sleep mode.
- This setup is only for data collection.
- Startup, monitor, health, and summary scripts expect collector-specific image names. If a collector is manually started with plain `python.exe`, Split Tunnel isolation becomes ambiguous and operator visibility can degrade to legacy fallback mode.
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

If the monitor shows scanner `STOPPED` unexpectedly:
- check the scanner lock file:
  - `polymarket_scanner/btc_5min_clob_scanner.lock`
- run:

```powershell
python scripts\btc5m_collection_summary.py --json
```

- confirm `collectors.scanner.process_image_name == btc5m-scanner.exe`
