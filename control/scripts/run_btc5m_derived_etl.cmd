@echo off
setlocal
cd /d "%~dp0\..\.."
set "REPO_ROOT=%CD%"
set "BTC5M_SITE_PACKAGES=%REPO_ROOT%\.venv\Lib\site-packages"
if defined PYTHONPATH (
    set "PYTHONPATH=%REPO_ROOT%;%BTC5M_SITE_PACKAGES%;%PYTHONPATH%"
) else (
    set "PYTHONPATH=%REPO_ROOT%;%BTC5M_SITE_PACKAGES%"
)
set "PROCESS_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
if not exist "%PROCESS_EXE%" set "PROCESS_EXE=python"

"%PROCESS_EXE%" "scripts\btc5m_build_features.py" --lookback-hours 24 --max-markets 500
if errorlevel 1 exit /b %errorlevel%
"%PROCESS_EXE%" "scripts\btc5m_build_labels.py" --lookback-hours 24 --max-markets 500
if errorlevel 1 exit /b %errorlevel%
"%PROCESS_EXE%" "scripts\btc5m_build_decision_dataset.py" --lookback-hours 24 --max-markets 500
exit /b %errorlevel%
