@echo off
chcp 65001 >nul
echo.
echo  ========================================
echo     ATBB scraping (manual)
echo  ========================================
echo.

cd /d "%~dp0"

if not exist ".env" (
    echo [ERROR] .env file not found
    pause
    exit /b 1
)

echo  Target: Tokyo, Saitama, Chiba, Kanagawa
echo  Output: results\properties_database_list.json
echo.
echo  * Chrome will open automatically. Do not touch.
echo  * Takes 15-30 minutes.
echo.
echo  Press any key to start...
pause >nul

echo.
echo  [START] %date% %time%
echo  ----------------------------------------
echo.

python atbb_list_scraper.py

echo.
echo  ----------------------------------------
echo  [END] %date% %time%
echo.

if exist "results\properties_database_list.json" (
    echo  OK: results\properties_database_list.json
    for %%A in ("results\properties_database_list.json") do echo  Size: %%~zA bytes
) else (
    echo  WARN: JSON file not found.
)

echo.
pause
