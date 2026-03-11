"""FastAPI アプリケーション起動"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from backend.config import FRONTEND_URL
from backend.database import init_db
from backend.middleware.auth import require_admin
from backend.routers import check, knowledge, phone_tasks
from backend.services import playwright_loop, session_keeper, atbb_scheduler, r2_atbb_sync

# テストページのパス
TEST_HTML = Path(__file__).resolve().parent.parent / "test.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """起動時にDB同期・初期化、ブラウザ常駐、ATBBスケジューラー起動"""
    # 0. R2からATBB DBを同期（131,486件の最新データを取得）
    await r2_atbb_sync.startup()

    await init_db()

    # 1. Playwright専用スレッド起動
    playwright_loop.ensure_started()
    print("[main] Playwright専用スレッド起動済み")

    # 2. ブラウザ常駐ログイン（バックグラウンドで初回ログイン + 5分間隔セッション維持）
    session_keeper.startup()

    # 3. ATBBスクレイピングスケジューラー起動（0時・12時に実行）
    await atbb_scheduler.startup()

    yield

    # --- シャットダウン ---
    await atbb_scheduler.shutdown()
    await r2_atbb_sync.shutdown()
    session_keeper.shutdown()
    playwright_loop.stop()
    print("[main] 全サービス停止完了")


app = FastAPI(title="空確くん API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:8001",
        "https://bukkaku-kun.vercel.app",
        os.environ.get("AKIYA_TOOLS_URL", ""),
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(check.router, prefix="/api")
app.include_router(knowledge.router, prefix="/api")
app.include_router(phone_tasks.router, prefix="/api")


@app.get("/test")
async def test_page():
    """テストページを配信"""
    return FileResponse(TEST_HTML, media_type="text/html")


@app.post("/api/auth/verify")
async def verify_auth(admin: str = Depends(require_admin)):
    """APIキーの有効性を検証"""
    return {"valid": True}


@app.get("/api/health")
async def health():
    """ヘルスチェック（各サブシステムの状態を確認）"""
    checks = {}

    # DB接続チェック
    try:
        from backend.database import get_db
        db = await get_db()
        await db.execute("SELECT 1")
        await db.close()
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"

    # Playwrightスレッドチェック
    checks["playwright_thread"] = "ok" if playwright_loop.is_alive() else "dead"

    overall = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": overall, "checks": checks}
