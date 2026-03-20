@echo off
setlocal
for %%I in ("%~dp0..\..") do set "REPO_ROOT=%%~fI"
set "MONITOR_SCRIPT=%REPO_ROOT%\control\scripts\btc5m_console_monitor.ps1"
title Prediction Market Data Pipeline ^| BTC5M Monitor
cd /d "%REPO_ROOT%"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%REPO_ROOT%\control\scripts\ensure_btc5m_process_exes.ps1" -Quiet >nul 2>nul
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%REPO_ROOT%\control\scripts\btc5m_collection_control.ps1" -Action start >nul 2>nul
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$script='%MONITOR_SCRIPT%'.ToLowerInvariant(); $running=Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.Name -in @('powershell.exe','pwsh.exe') -and $_.ProcessId -ne $PID -and $_.CommandLine -and $_.CommandLine.ToLowerInvariant().Contains($script) }; if($running){ exit 0 } else { exit 1 }" >nul 2>nul
if %errorlevel%==0 exit /b 0
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%MONITOR_SCRIPT%" -NoStart
