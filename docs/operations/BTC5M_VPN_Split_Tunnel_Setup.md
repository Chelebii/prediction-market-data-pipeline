# BTC5M VPN Split Tunnel Setup

Last updated: 2026-03-18

## Purpose

Use VPN Split Tunnel so only the BTC5M data collectors route through the VPN.
This reduces collateral impact on the rest of the machine while keeping Polymarket access available for the collectors.

## Select These Applications

Add these application image names to the VPN Split Tunnel allowlist:
- `btc5m-scanner.exe`
- `btc5m-reference.exe`
- `btc5m-resolution.exe`

Typical Windows install paths:
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-scanner.exe`
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-reference.exe`
- `%LocalAppData%\Python\pythoncore-3.14-64\btc5m-resolution.exe`

## Do Not Select These

Do not add these to Split Tunnel:
- `python.exe`
- `powershell.exe`
- `cmd.exe`
- `wscript.exe`

Reason:
- they are generic interpreters or orchestration processes
- selecting them can unintentionally route unrelated tools through the VPN

## What Each Collector Does

- `btc5m-scanner.exe`
  - runs the Polymarket BTC 5MIN scanner
  - produces snapshot/orderbook/lifecycle rows
- `btc5m-reference.exe`
  - runs the BTC reference collector
  - produces BTC spot ticks
- `btc5m-resolution.exe`
  - runs the official resolution collector
  - updates resolved market outcomes

## Prepare the Executables

Ensure the collector-specific executables exist:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\ensure_btc5m_process_exes.ps1
```

The helper creates renamed Python executables. These are real process image names, not wrapper-only stubs.
The collectors still inherit the repo's `.venv\Lib\site-packages` through `PYTHONPATH`, so split-tunnel image names stay specific without routing all Python usage on the machine.

## Start and Verify

Start or restart the collectors:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action restart
```

Verify the running process names:

```powershell
powershell -ExecutionPolicy Bypass -File control\scripts\btc5m_collection_control.ps1 -Action status
```

Expected result:
- `scanner ... image=btc5m-scanner.exe`
- `reference ... image=btc5m-reference.exe`
- `resolution ... image=btc5m-resolution.exe`

You can also verify with:

```powershell
python scripts\btc5m_collection_summary.py
```

## Troubleshooting

If the VPN app does not show the expected collector names:
- rerun `ensure_btc5m_process_exes.ps1`
- restart the collectors with the control script
- confirm no legacy `python.exe` collector processes are still running

If a collector falls back to `python.exe`:
- Split Tunnel targeting becomes too broad
- stop all collectors
- recreate the renamed executables
- start the collectors again using the control script

If the monitor or dashboard shows incorrect status:
- run `python scripts\btc5m_collection_summary.py --json`
- check:
  - `collectors.scanner.process_image_name`
  - `collectors.reference.process_image_name`
  - `collectors.resolution.process_image_name`
- expected values:
  - `btc5m-scanner.exe`
  - `btc5m-reference.exe`
  - `btc5m-resolution.exe`
