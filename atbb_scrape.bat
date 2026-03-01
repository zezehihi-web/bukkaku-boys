@echo off
chcp 65001 >nul
echo.
echo  ╔═══════════════════════════════════════════╗
echo  ║     ATBB スクレイピング（手動実行）        ║
echo  ╚═══════════════════════════════════════════╝
echo.

cd /d "%~dp0"

:: .env 確認
if not exist ".env" (
    echo [ERROR] .env ファイルが見つかりません
    echo    ATBB_LOGIN_ID / ATBB_PASSWORD を設定してください
    pause
    exit /b 1
)

echo  対象: 東京都・埼玉県・千葉県・神奈川県
echo  出力: results\properties_database_list.json
echo.
echo  ※ Chromeが自動で開きます。触らないでください。
echo  ※ 完了まで15〜30分かかります。
echo.
echo  開始しますか？ (何かキーを押すとスタート)
pause >nul

echo.
echo  [開始] %date% %time%
echo  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.

python atbb_list_scraper.py

echo.
echo  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo  [完了] %date% %time%
echo.

if exist "results\properties_database_list.json" (
    echo  ✅ データ保存先: results\properties_database_list.json
    for %%A in ("results\properties_database_list.json") do echo  ✅ ファイルサイズ: %%~zA bytes
) else (
    echo  ⚠ JSONファイルが見つかりません。ログを確認してください。
)

echo.
pause
