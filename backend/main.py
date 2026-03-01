"""FastAPI アプリケーション起動"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from backend.config import FRONTEND_URL
from backend.database import init_db
from backend.routers import check, knowledge, phone_tasks
from backend.services import playwright_loop, session_keeper, atbb_scheduler

# テストページのパス
TEST_HTML = Path(__file__).resolve().parent.parent / "test.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """起動時にDB初期化、ブラウザ常駐、ATBBスケジューラー起動"""
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
    session_keeper.shutdown()
    playwright_loop.stop()
    print("[main] 全サービス停止完了")


app = FastAPI(title="空確くん API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:8001"],
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


@app.get("/api/health")
async def health():
    return {"status": "ok"}
