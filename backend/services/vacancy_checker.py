"""空室確認オーケストレーター

全体フローを統括:
1. URL解析（SUUMO/HOMES → 物件情報抽出）
2. ATBB照合（物件名・住所でマッチング）
3. プラットフォーム判定（ナレッジDB参照）
4. 空室確認（Playwright経由）
5. 結果通知（LINE/Slack）
"""

import traceback
from datetime import datetime

from backend.database import get_db
from backend.services.url_parser import parse_portal_url
from backend.services.property_matcher import match_property
from backend.services.knowledge_service import lookup_platform, record_usage
from backend.scrapers.itanji_checker import check_vacancy as itanji_check
from backend.scrapers.ierabu_checker import check_vacancy as ierabu_check
from backend.scrapers.es_square_checker import check_vacancy as es_square_check
from backend.notifications.line_notifier import send_line_notification
from backend.notifications.slack_notifier import send_slack_notification


async def _update_status(check_id: int, **kwargs):
    """DBステータスを更新"""
    db = await get_db()
    try:
        sets = []
        values = []
        for key, val in kwargs.items():
            sets.append(f"{key} = ?")
            values.append(val)
        values.append(check_id)
        await db.execute(
            f"UPDATE check_requests SET {', '.join(sets)} WHERE id = ?",
            values,
        )
        await db.commit()
    finally:
        await db.close()


async def run_vacancy_check(check_id: int):
    """空室確認メイン処理（バックグラウンドタスク）"""
    try:
        # レコード取得
        db = await get_db()
        try:
            row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (check_id,))
            record = await row.fetchone()
        finally:
            await db.close()

        if not record:
            return

        url = record["submitted_url"]
        portal = record["portal_source"]
        status = record["status"]

        # ステータスに応じて処理を再開
        if status == "pending":
            await _step_parse(check_id, url, portal)
        elif status == "awaiting_platform":
            # ユーザーがプラットフォームを選択するのを待つ
            return
        elif status == "checking":
            await _step_check(check_id)

    except Exception as e:
        await _update_status(
            check_id,
            status="error",
            error_message=f"{type(e).__name__}: {e}",
        )
        traceback.print_exc()


async def _step_parse(check_id: int, url: str, portal: str):
    """ステップ1: URL解析"""
    await _update_status(check_id, status="parsing")

    try:
        info = await parse_portal_url(url, portal)
    except Exception as e:
        await _update_status(
            check_id,
            status="error",
            error_message=f"URL解析エラー: {e}",
        )
        return

    await _update_status(
        check_id,
        property_name=info.get("property_name", ""),
        property_address=info.get("address", ""),
        property_rent=info.get("rent", ""),
        property_area=info.get("area", ""),
        property_layout=info.get("layout", ""),
        status="matching",
    )

    await _step_match(
        check_id,
        info.get("property_name", ""),
        info.get("address", ""),
        info.get("rent", ""),
        info.get("area", ""),
        info.get("layout", ""),
    )


async def _step_match(
    check_id: int,
    property_name: str,
    address: str,
    rent: str,
    area: str,
    layout: str,
):
    """ステップ2: ATBB照合"""
    matched = match_property(property_name, address, rent, area, layout)

    if not matched:
        await _update_status(
            check_id,
            atbb_matched=False,
            status="not_found",
            vacancy_result="確認不可（専任物件の可能性）",
            completed_at=datetime.now().isoformat(),
        )
        # 通知
        await _notify(check_id, property_name, "確認不可（専任物件の可能性）", "")
        return

    company_info = matched.get("管理会社情報", "")
    await _update_status(
        check_id,
        atbb_matched=True,
        atbb_company=company_info,
    )

    # ステップ3: プラットフォーム判定
    company_name = company_info.split(" ")[0] if company_info else ""
    company_phone = ""
    parts = company_info.split(" ")
    if len(parts) > 1:
        company_phone = parts[-1]

    platform = await lookup_platform(company_name, company_phone)

    if platform:
        await _update_status(
            check_id,
            platform=platform,
            platform_auto=True,
            status="checking",
        )
        await _step_check(check_id)
    else:
        # ユーザーにプラットフォーム選択を依頼
        await _update_status(check_id, status="awaiting_platform")


async def _step_check(check_id: int):
    """ステップ4: 空室確認"""
    db = await get_db()
    try:
        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (check_id,))
        record = await row.fetchone()
    finally:
        await db.close()

    if not record:
        return

    platform = record["platform"]
    property_name = record["property_name"]

    # 物件名から号室を分離
    room_number = ""
    if "/" in property_name:
        parts = property_name.rsplit("/", 1)
        property_name = parts[0]
        room_number = parts[1]

    try:
        if platform == "itanji":
            result = await itanji_check(property_name, room_number)
        elif platform == "ierabu":
            result = await ierabu_check(property_name, room_number)
        elif platform == "es_square":
            result = await es_square_check(property_name, room_number)
        else:
            result = "該当なし"
    except Exception as e:
        await _update_status(
            check_id,
            status="error",
            error_message=f"空室確認エラー ({platform}): {e}",
        )
        return

    await _update_status(
        check_id,
        vacancy_result=result,
        status="done",
        completed_at=datetime.now().isoformat(),
    )

    # ナレッジDB更新
    company_info = record["atbb_company"] or ""
    company_name_part = company_info.split(" ")[0] if company_info else ""
    if company_name_part and platform:
        company_phone_part = ""
        parts = company_info.split(" ")
        if len(parts) > 1:
            company_phone_part = parts[-1]
        await record_usage(company_name_part, platform, company_phone_part)

    # 通知
    platform_names = {"itanji": "イタンジBB", "ierabu": "いえらぶBB", "es_square": "いい生活スクエア"}
    await _notify(check_id, record["property_name"], result, platform_names.get(platform, platform))


async def _notify(check_id: int, property_name: str, result: str, platform_name: str):
    """結果を通知"""
    message = f"【空確くん】{property_name}\n結果: {result}"
    if platform_name:
        message += f"\n確認先: {platform_name}"

    try:
        await send_line_notification(message)
    except Exception:
        pass

    try:
        await send_slack_notification(message)
    except Exception:
        pass
