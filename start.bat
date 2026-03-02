@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================
echo   空確くん バックエンドサーバー (自動再起動)
echo ============================================
echo.

:restart
echo [%date% %time%] サーバー起動中...
echo.

python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

echo.
echo [%date% %time%] サーバーが停止しました。10秒後に自動再起動します...
echo   (終了するには Ctrl+C を押してください)
echo.
timeout /t 10 /nobreak >nul
goto restart
