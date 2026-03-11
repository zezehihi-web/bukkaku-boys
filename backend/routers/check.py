"""空室確認APIルーター"""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from backend.config import ITANJI_EMAIL, ES_SQUARE_EMAIL, GOWEB_USER_ID
from backend.credentials_map import parse_platform_key
from backend.database import get_db
from backend.middleware.auth import require_admin
from backend.models import CheckRequest, CheckStatus, CheckListItem, PlatformSelection, PropertyInfoRequest, BatchCheckRequest, BatchCheckResponse
from backend.services.vacancy_checker import run_vacancy_check
from backend.services.url_parser import detect_portal

router = APIRouter(tags=["check"])


def _start_background_check(check_id: int):
    """バックグラウンドで空室確認を開始（Playwright専用スレッドに投入）"""
    from backend.services.playwright_loop import submit_coro
    submit_coro(run_vacancy_check(check_id))


@router.post("/check", response_model=CheckStatus)
async def create_check(req: CheckRequest):
    """空室確認リクエストを作成し、バックグラウンドで処理を開始"""
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URLが空です")

    # ポータル判定（全ポータルサイト自動検出）
    portal = detect_portal(url) or ""

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
    _start_background_check(row_id)

    return _row_to_status(record)


@router.post("/check/property-info", response_model=CheckStatus)
async def create_check_from_property_info(req: PropertyInfoRequest):
    """図面解析結果から空室確認（URLパースをスキップし、ATBB照合から開始）"""
    if not req.property_name.strip():
        raise HTTPException(status_code=400, detail="物件名が空です")

    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO check_requests
               (submitted_url, portal_source, property_name, property_address,
                property_rent, property_area, property_layout, property_build_year,
                status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'matching')""",
            (
                "(図面解析)", "floorplan",
                req.property_name.strip(), req.address.strip(),
                req.rent.strip(), req.area.strip(),
                req.layout.strip(), req.build_year.strip(),
            ),
        )
        await db.commit()
        row_id = cursor.lastrowid

        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (row_id,))
        record = await row.fetchone()
    finally:
        await db.close()

    # URLパースをスキップしてATBB照合から開始
    from backend.services.vacancy_checker import run_vacancy_check_from_property_info
    from backend.services.playwright_loop import submit_coro
    submit_coro(run_vacancy_check_from_property_info(
        row_id, req.property_name.strip(), req.address.strip(),
        req.rent.strip(), req.area.strip(), req.layout.strip(), req.build_year.strip(),
    ))

    return _row_to_status(record)


@router.post("/checks/batch", response_model=BatchCheckResponse)
async def create_batch_check(req: BatchCheckRequest):
    """一括空室確認（最大5件のURLを同時に受付、順次処理）"""
    urls = [u.strip() for u in req.urls if u.strip()]
    if not urls:
        raise HTTPException(status_code=400, detail="URLが空です")
    if len(urls) > 5:
        raise HTTPException(status_code=400, detail="一度に確認できるのは最大5件です")

    ids: list[int] = []
    db = await get_db()
    try:
        for url in urls:
            portal = detect_portal(url) or ""
            cursor = await db.execute(
                """INSERT INTO check_requests (submitted_url, portal_source, status)
                   VALUES (?, ?, 'pending')""",
                (url, portal),
            )
            ids.append(cursor.lastrowid)
        await db.commit()
    finally:
        await db.close()

    # 全件登録後にバックグラウンド処理を開始
    for row_id in ids:
        _start_background_check(row_id)

    return BatchCheckResponse(ids=ids)


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
async def select_platform(check_id: int, sel: PlatformSelection, admin: str = Depends(require_admin)):
    """ユーザーがプラットフォームを手動選択"""
    # 単純キー "itanji" / 複合キー "bukkaku:CIC" 両対応
    platform_type, _ = parse_platform_key(sel.platform)
    if platform_type not in ("itanji", "es_square", "goweb", "bukkaku", "es_b2b"):
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
    _start_background_check(check_id)

    return {"status": "ok"}


@router.get("/platforms/status")
async def platform_status(admin: str = Depends(require_admin)):
    """各プラットフォームの認証設定状態"""
    return {
        "itanji": {"configured": bool(ITANJI_EMAIL), "label": "イタンジBB"},
        "es_square": {"configured": bool(ES_SQUARE_EMAIL), "label": "いい生活スクエア"},
        "goweb": {"configured": bool(GOWEB_USER_ID), "label": "GoWeb"},
        "bukkaku": {"configured": True, "label": "物確.com"},
        "es_b2b": {"configured": True, "label": "いい生活B2B"},
    }


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
        property_build_year=row["property_build_year"] or "" if "property_build_year" in row.keys() else "",
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
