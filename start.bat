@echo off
chcp 65001 >nul
echo.
echo  ╔═══════════════════════════════════════════╗
echo  ║       空確くん バックエンドサーバー        ║
echo  ╚═══════════════════════════════════════════╝
echo.

cd /d "%~dp0"

:: .env 確認
if not exist ".env" (
    echo [ERROR] .env ファイルが見つかりません
    echo    .env を作成して認証情報を設定してください
    pause
    exit /b 1
)

echo  [1/2] 環境チェック中...

:: Python確認
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python が見つかりません
    pause
    exit /b 1
)

:: 主要パッケージ確認
python -c "import fastapi, uvicorn, aiosqlite, playwright, dotenv" 2>nul
if errorlevel 1 (
    echo [WARN] 不足パッケージがあります。インストール中...
    pip install fastapi uvicorn aiosqlite playwright python-dotenv httpx
)

echo  [2/2] サーバー起動中...
echo.
echo  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo   API:      http://localhost:8000
echo   テスト:   http://localhost:8000/test
echo   ヘルス:   http://localhost:8000/api/health
echo  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.
echo  ※ このウィンドウを閉じないでください
echo  ※ 停止するには Ctrl+C を押してください
echo.
echo  起動時の動作:
echo    - イタンジBB / いい生活スクエアに自動ログイン
echo    - 5分おきにセッション生存チェック
echo    - 0時・12時にATBBデータ自動更新
echo.

python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

echo.
echo  サーバーが停止しました。
pause
