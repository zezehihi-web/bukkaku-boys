@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo [%date% %time%] start.bat launched
echo.

python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

echo [%date% %time%] server stopped. restarting in 10s...
timeout /t 10 /nobreak >nul
goto :eof
