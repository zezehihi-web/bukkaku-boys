"""空室確認APIルーター"""

import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException

from backend.database import get_db
from backend.models import CheckRequest, CheckStatus, CheckListItem, PlatformSelection
from backend.services.vacancy_checker import run_vacancy_check

router = APIRouter(tags=["check"])


@router.post("/check", response_model=CheckStatus)
async def create_check(req: CheckRequest):
    """空室確認リクエストを作成し、バックグラウンドで処理を開始"""
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URLが空です")

    # ポータル判定
    portal = ""
    if "suumo.jp" in url:
        portal = "suumo"
    elif "homes.co.jp" in url:
        portal = "homes"

    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO check_requests (submitted_url, portal_source, status)
               VALUES (?, ?, 'pending')""",
            (url, portal),
        )
        await db.commit()
        row_id = cursor.lastrowid

        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (row_id,))
        record = await row.fetchone()
    finally:
        await db.close()

    # バックグラウンドで空室確認処理を開始
    asyncio.create_task(run_vacancy_check(row_id))

    return _row_to_status(record)


@router.get("/check/{check_id}", response_model=CheckStatus)
async def get_check(check_id: int):
    """確認結果を取得（ポーリング用）"""
    db = await get_db()
    try:
        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (check_id,))
        record = await row.fetchone()
    finally:
        await db.close()

    if not record:
        raise HTTPException(status_code=404, detail="確認リクエストが見つかりません")
    return _row_to_status(record)


@router.get("/checks", response_model=list[CheckListItem])
async def list_checks(limit: int = 50):
    """最近の確認結果一覧"""
    db = await get_db()
    try:
        rows = await db.execute(
            """SELECT id, property_name, status, vacancy_result, portal_source, created_at
               FROM check_requests ORDER BY id DESC LIMIT ?""",
            (limit,),
        )
        records = await rows.fetchall()
    finally:
        await db.close()

    return [
        CheckListItem(
            id=r["id"],
            property_name=r["property_name"] or "(解析中)",
            status=r["status"],
            vacancy_result=r["vacancy_result"],
            portal_source=r["portal_source"],
            created_at=r["created_at"] or "",
        )
        for r in records
    ]


@router.post("/check/{check_id}/platform")
async def select_platform(check_id: int, sel: PlatformSelection):
    """ユーザーがプラットフォームを手動選択"""
    if sel.platform not in ("itanji", "ierabu", "es_square"):
        raise HTTPException(status_code=400, detail="無効なプラットフォーム")

    db = await get_db()
    try:
        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (check_id,))
        record = await row.fetchone()
        if not record:
            raise HTTPException(status_code=404, detail="確認リクエストが見つかりません")

        await db.execute(
            """UPDATE check_requests
               SET platform = ?, platform_auto = 0, status = 'checking'
               WHERE id = ?""",
            (sel.platform, check_id),
        )
        await db.commit()

        # ナレッジDBに学習
        if sel.remember and record["atbb_company"]:
            company_name = record["atbb_company"].split(" ")[0] if record["atbb_company"] else ""
            company_phone = ""
            parts = record["atbb_company"].split(" ")
            if len(parts) > 1:
                company_phone = parts[-1]

            if company_name:
                await db.execute(
                    """INSERT INTO company_platform_knowledge (company_name, company_phone, platform)
                       VALUES (?, ?, ?)
                       ON CONFLICT(company_name, platform) DO UPDATE SET
                           use_count = use_count + 1,
                           last_used_at = CURRENT_TIMESTAMP""",
                    (company_name, company_phone, sel.platform),
                )
                await db.commit()
    finally:
        await db.close()

    # バックグラウンドで空室確認を再開
    asyncio.create_task(run_vacancy_check(check_id))

    return {"status": "ok"}


def _row_to_status(row) -> CheckStatus:
    return CheckStatus(
        id=row["id"],
        submitted_url=row["submitted_url"],
        portal_source=row["portal_source"],
        property_name=row["property_name"] or "",
        property_address=row["property_address"] or "",
        property_rent=row["property_rent"] or "",
        property_area=row["property_area"] or "",
        property_layout=row["property_layout"] or "",
        atbb_matched=bool(row["atbb_matched"]),
        atbb_company=row["atbb_company"] or "",
        platform=row["platform"] or "",
        platform_auto=bool(row["platform_auto"]),
        status=row["status"],
        vacancy_result=row["vacancy_result"] or "",
        error_message=row["error_message"] or "",
        created_at=row["created_at"] or "",
        completed_at=row["completed_at"],
    )
