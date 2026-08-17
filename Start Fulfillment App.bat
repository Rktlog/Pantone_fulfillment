@echo off
title AusPost Fulfillment App

cd /d "%~dp0"

if not exist "venv\Scripts\streamlit.exe" (
    echo Could not find venv\Scripts\streamlit.exe
    echo Make sure this file sits in the same folder as your venv and app.py.
    pause
    exit /b 1
)

echo Starting AusPost Fulfillment App...
echo Close this window to stop the app.
echo.

"venv\Scripts\streamlit.exe" run app.py

if errorlevel 1 pause