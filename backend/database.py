"""SQLiteデータベース接続・テーブル管理"""

import aiosqlite
from backend.config import DB_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS check_requests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    submitted_url   TEXT NOT NULL,
    portal_source   TEXT DEFAULT '',
    property_name   TEXT DEFAULT '',
    property_address TEXT DEFAULT '',
    property_rent   TEXT DEFAULT '',
    property_area   TEXT DEFAULT '',
    property_layout TEXT DEFAULT '',
    atbb_matched    BOOLEAN DEFAULT 0,
    atbb_company    TEXT DEFAULT '',
    platform        TEXT DEFAULT '',
    platform_auto   BOOLEAN DEFAULT 0,
    status          TEXT DEFAULT 'pending',
    vacancy_result  TEXT DEFAULT '',
    error_message   TEXT DEFAULT '',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at    DATETIME
);

CREATE TABLE IF NOT EXISTS company_platform_knowledge (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name    TEXT NOT NULL,
    company_phone   TEXT DEFAULT '',
    platform        TEXT NOT NULL,
    use_count       INTEGER DEFAULT 1,
    last_used_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_name, platform)
);
"""


async def get_db() -> aiosqlite.Connection:
    """データベース接続を取得"""
    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    """テーブルを初期化"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()
