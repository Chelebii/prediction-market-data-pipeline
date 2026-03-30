@echo off
setlocal
cd /d "%~dp0\..\.."
set "REPO_ROOT=%CD%"
set "ENSURE_SCRIPT=%REPO_ROOT%\control\scripts\ensure_btc5m_process_exes.ps1"
set "BTC5M_SITE_PACKAGES=%REPO_ROOT%\.venv\Lib\site-packages"
if defined PYTHONPATH (
    set "PYTHONPATH=%REPO_ROOT%;%BTC5M_SITE_PACKAGES%;%PYTHONPATH%"
) else (
    set "PYTHONPATH=%REPO_ROOT%;%BTC5M_SITE_PACKAGES%"
)
set "PROCESS_EXE=%BTC5M_HEALTHCHECK_EXE_PATH%"
if not defined PROCESS_EXE set "PROCESS_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\btc5m-healthcheck.exe"
if not exist "%PROCESS_EXE%" powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ENSURE_SCRIPT%" -Quiet >nul 2>nul
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python314\btc5m-healthcheck.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python311\btc5m-healthcheck.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=python"
"%PROCESS_EXE%" "scripts\btc5m_healthcheck.py"
exit /b %errorlevel%
