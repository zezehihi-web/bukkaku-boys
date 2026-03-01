@echo off
echo ========================================
echo   Akikaku-kun Test Server
echo ========================================
echo.
cd /d "%~dp0"

echo   TEST: http://localhost:8001/test
echo.
echo   Do NOT close this window.
echo   Press Ctrl+C to stop.
echo.

start "" "http://localhost:8001/test"

python -m uvicorn backend.main:app --host 0.0.0.0 --port 8001

echo.
echo Server stopped.
pause
