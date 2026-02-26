"""FastAPI アプリケーション起動"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import FRONTEND_URL
from backend.database import init_db
from backend.routers import check, knowledge


@asynccontextmanager
async def lifespan(app: FastAPI):
    """起動時にDB初期化"""
    await init_db()
    yield


app = FastAPI(title="空確くん API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(check.router, prefix="/api")
app.include_router(knowledge.router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}
