@echo off
setlocal
cd /d "%~dp0\..\.."
set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
"%PYTHON_EXE%" "scripts\btc5m_audit_dataset.py" --lookback-hours 48 --max-markets 250 --include-active
exit /b %errorlevel%
