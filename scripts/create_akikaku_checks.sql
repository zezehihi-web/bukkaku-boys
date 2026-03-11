-- Neon PostgreSQL: akikaku_checks テーブル
-- スピ賃.comと同じDBに作成

CREATE TABLE IF NOT EXISTS akikaku_checks (
    id              SERIAL PRIMARY KEY,
    submitted_url   TEXT NOT NULL DEFAULT '',
    portal_source   TEXT DEFAULT '',
    property_name   TEXT DEFAULT '',
    property_address TEXT DEFAULT '',
    property_rent   TEXT DEFAULT '',
    property_area   TEXT DEFAULT '',
    property_layout TEXT DEFAULT '',
    property_build_year TEXT DEFAULT '',
    atbb_matched    BOOLEAN DEFAULT FALSE,
    atbb_company    TEXT DEFAULT '',
    platform        TEXT DEFAULT '',
    platform_auto   BOOLEAN DEFAULT FALSE,
    status          TEXT DEFAULT 'pending',
    vacancy_result  TEXT DEFAULT '',
    error_message   TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    line_user_id    TEXT DEFAULT '',
    line_notified   BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_akikaku_checks_status ON akikaku_checks(status);
