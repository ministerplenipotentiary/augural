@echo off
title Crypto Monitor
cd /d "%~dp0"
echo Starting Crypto Monitor...
echo.
"C:\Users\kkrot\AppData\Local\Programs\Python\Python313\python.exe" -u monitor.py
echo.
if errorlevel 1 (
    echo.
    echo [ERROR] Monitor exited with an error. See above for details.
)
echo Press any key to close...
pause >nul
