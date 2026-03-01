@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo [%date% %time%] ATBB scrape started
python atbb_list_scraper.py
echo [%date% %time%] ATBB scrape finished
