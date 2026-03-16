@echo off
setlocal
title BTC5M Monitor
cd /d C:\Users\mavia\.openclaw\5minbots
powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\Users\mavia\.openclaw\5minbots\control\scripts\btc5m_console_monitor.ps1
