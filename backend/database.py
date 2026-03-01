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
    property_build_year TEXT DEFAULT '',
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
    requires_phone  BOOLEAN DEFAULT 0,
    use_count       INTEGER DEFAULT 1,
    last_used_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_name, platform)
);

CREATE TABLE IF NOT EXISTS phone_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    check_request_id INTEGER,
    company_name    TEXT NOT NULL,
    company_phone   TEXT DEFAULT '',
    property_name   TEXT DEFAULT '',
    property_address TEXT DEFAULT '',
    reason          TEXT DEFAULT '',
    status          TEXT DEFAULT 'pending',
    note            TEXT DEFAULT '',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at    DATETIME,
    FOREIGN KEY (check_request_id) REFERENCES check_requests(id)
);

CREATE TABLE IF NOT EXISTS atbb_properties (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    property_key        TEXT NOT NULL UNIQUE,
    name                TEXT DEFAULT '',
    room_number         TEXT DEFAULT '',
    rent                TEXT DEFAULT '',
    management_fee      TEXT DEFAULT '',
    deposit             TEXT DEFAULT '',
    key_money           TEXT DEFAULT '',
    layout              TEXT DEFAULT '',
    area                TEXT DEFAULT '',
    floors              TEXT DEFAULT '',
    address             TEXT DEFAULT '',
    build_year          TEXT DEFAULT '',
    transport           TEXT DEFAULT '',
    structure           TEXT DEFAULT '',
    transaction_type    TEXT DEFAULT '',
    management_company  TEXT DEFAULT '',
    publish_date        TEXT DEFAULT '',
    property_id         TEXT DEFAULT '',
    prefecture          TEXT DEFAULT '',
    status              TEXT DEFAULT '募集中',
    first_seen          TEXT NOT NULL,
    last_seen           TEXT NOT NULL,
    name_history        TEXT DEFAULT '[]',
    created_at          TEXT DEFAULT (datetime('now','localtime')),
    updated_at          TEXT DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_atbb_prop_key ON atbb_properties(property_key);
CREATE INDEX IF NOT EXISTS idx_atbb_name ON atbb_properties(name);
CREATE INDEX IF NOT EXISTS idx_atbb_address ON atbb_properties(address);
CREATE INDEX IF NOT EXISTS idx_atbb_status ON atbb_properties(status);
CREATE INDEX IF NOT EXISTS idx_atbb_prefecture ON atbb_properties(prefecture);
CREATE INDEX IF NOT EXISTS idx_atbb_last_seen ON atbb_properties(last_seen);
"""

# 既存テーブルに新カラムを追加するマイグレーション
MIGRATION_SQL = """
-- check_requests に build_year 追加
ALTER TABLE check_requests ADD COLUMN property_build_year TEXT DEFAULT '';

-- company_platform_knowledge に requires_phone 追加
ALTER TABLE company_platform_knowledge ADD COLUMN requires_phone BOOLEAN DEFAULT 0;
"""


async def get_db() -> aiosqlite.Connection:
    """データベース接続を取得"""
    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    """テーブルを初期化（新規作成 + 既存DBマイグレーション）"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()

        # 既存テーブルへのカラム追加（エラーは無視）
        for stmt in MIGRATION_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    await db.execute(stmt)
                    await db.commit()
                except Exception:
                    pass  # カラムが既に存在する場合
