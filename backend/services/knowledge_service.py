"""管理会社→プラットフォーム ナレッジ学習サービス"""

from backend.database import get_db


async def lookup_platform(company_name: str, company_phone: str = "") -> str | None:
    """管理会社名からプラットフォームを推定

    Args:
        company_name: 管理会社名
        company_phone: 電話番号（オプション）

    Returns:
        'itanji' / 'es_square' / None（未知の場合）
    """
    if not company_name:
        return None

    db = await get_db()
    try:
        # 完全一致で最も使用回数が多いものを返す
        row = await db.execute(
            """SELECT platform FROM company_platform_knowledge
               WHERE company_name = ?
               ORDER BY use_count DESC
               LIMIT 1""",
            (company_name,),
        )
        record = await row.fetchone()
        if record:
            return record["platform"]

        # 部分一致（会社名の先頭一致）
        row = await db.execute(
            """SELECT platform FROM company_platform_knowledge
               WHERE company_name LIKE ? || '%'
               ORDER BY use_count DESC
               LIMIT 1""",
            (company_name[:4],),
        )
        record = await row.fetchone()
        if record:
            return record["platform"]

    finally:
        await db.close()

    return None


async def is_phone_required(company_name: str) -> bool:
    """管理会社が電話確認必要と学習済みか判定"""
    if not company_name:
        return False

    db = await get_db()
    try:
        row = await db.execute(
            """SELECT requires_phone FROM company_platform_knowledge
               WHERE company_name = ? AND requires_phone = 1
               LIMIT 1""",
            (company_name,),
        )
        record = await row.fetchone()
        return record is not None
    finally:
        await db.close()


async def mark_phone_required(company_name: str, company_phone: str = ""):
    """管理会社を「電話確認必要」としてマーク"""
    if not company_name:
        return

    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO company_platform_knowledge (company_name, company_phone, platform, requires_phone)
               VALUES (?, ?, 'phone', 1)
               ON CONFLICT(company_name, platform) DO UPDATE SET
                   requires_phone = 1,
                   last_used_at = CURRENT_TIMESTAMP""",
            (company_name, company_phone),
        )
        await db.commit()
    finally:
        await db.close()


async def record_usage(company_name: str, platform: str, company_phone: str = ""):
    """プラットフォーム使用を記録（use_countを増やす）"""
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO company_platform_knowledge (company_name, company_phone, platform)
               VALUES (?, ?, ?)
               ON CONFLICT(company_name, platform) DO UPDATE SET
                   use_count = use_count + 1,
                   last_used_at = CURRENT_TIMESTAMP""",
            (company_name, company_phone, platform),
        )
        await db.commit()
    finally:
        await db.close()
