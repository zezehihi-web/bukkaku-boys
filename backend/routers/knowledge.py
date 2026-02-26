"""ナレッジDB管理APIルーター"""

from fastapi import APIRouter, HTTPException

from backend.database import get_db
from backend.models import KnowledgeEntry, KnowledgeItem

router = APIRouter(tags=["knowledge"])


@router.get("/knowledge", response_model=list[KnowledgeItem])
async def list_knowledge():
    """ナレッジDB一覧"""
    db = await get_db()
    try:
        rows = await db.execute(
            "SELECT * FROM company_platform_knowledge ORDER BY use_count DESC, company_name"
        )
        records = await rows.fetchall()
    finally:
        await db.close()

    return [
        KnowledgeItem(
            id=r["id"],
            company_name=r["company_name"],
            company_phone=r["company_phone"] or "",
            platform=r["platform"],
            use_count=r["use_count"],
            last_used_at=r["last_used_at"] or "",
        )
        for r in records
    ]


@router.post("/knowledge", response_model=KnowledgeItem)
async def create_knowledge(entry: KnowledgeEntry):
    """ナレッジDB登録"""
    if entry.platform not in ("itanji", "ierabu", "es_square"):
        raise HTTPException(status_code=400, detail="無効なプラットフォーム")

    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO company_platform_knowledge (company_name, company_phone, platform)
               VALUES (?, ?, ?)
               ON CONFLICT(company_name, platform) DO UPDATE SET
                   company_phone = excluded.company_phone,
                   use_count = use_count + 1,
                   last_used_at = CURRENT_TIMESTAMP""",
            (entry.company_name, entry.company_phone, entry.platform),
        )
        await db.commit()

        row = await db.execute(
            "SELECT * FROM company_platform_knowledge WHERE company_name = ? AND platform = ?",
            (entry.company_name, entry.platform),
        )
        record = await row.fetchone()
    finally:
        await db.close()

    return KnowledgeItem(
        id=record["id"],
        company_name=record["company_name"],
        company_phone=record["company_phone"] or "",
        platform=record["platform"],
        use_count=record["use_count"],
        last_used_at=record["last_used_at"] or "",
    )


@router.put("/knowledge/{item_id}", response_model=KnowledgeItem)
async def update_knowledge(item_id: int, entry: KnowledgeEntry):
    """ナレッジDB更新"""
    if entry.platform not in ("itanji", "ierabu", "es_square"):
        raise HTTPException(status_code=400, detail="無効なプラットフォーム")

    db = await get_db()
    try:
        await db.execute(
            """UPDATE company_platform_knowledge
               SET company_name = ?, company_phone = ?, platform = ?, last_used_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (entry.company_name, entry.company_phone, entry.platform, item_id),
        )
        await db.commit()

        row = await db.execute(
            "SELECT * FROM company_platform_knowledge WHERE id = ?", (item_id,)
        )
        record = await row.fetchone()
    finally:
        await db.close()

    if not record:
        raise HTTPException(status_code=404, detail="ナレッジが見つかりません")

    return KnowledgeItem(
        id=record["id"],
        company_name=record["company_name"],
        company_phone=record["company_phone"] or "",
        platform=record["platform"],
        use_count=record["use_count"],
        last_used_at=record["last_used_at"] or "",
    )


@router.delete("/knowledge/{item_id}")
async def delete_knowledge(item_id: int):
    """ナレッジDB削除"""
    db = await get_db()
    try:
        await db.execute("DELETE FROM company_platform_knowledge WHERE id = ?", (item_id,))
        await db.commit()
    finally:
        await db.close()
    return {"status": "ok"}
