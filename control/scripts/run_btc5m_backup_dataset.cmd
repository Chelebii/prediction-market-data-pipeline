@echo off
setlocal
cd /d "%~dp0\..\.."
set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
"%PYTHON_EXE%" "scripts\btc5m_backup_dataset.py"
exit /b %errorlevel%
