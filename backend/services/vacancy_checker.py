"""空室確認オーケストレーター

全体フローを統括:
1. URL解析（SUUMO/HOMES → 物件情報抽出）
2. ATBB照合（物件名・住所・面積・築年数でマッチング）
3. プラットフォーム判定（ナレッジDB参照）
4. 空室確認（Playwright経由）— プラットフォーム別レートリミット付き
5. 確認不可→電話確認タスク生成
6. 結果通知（LINE/Slack）
"""

import asyncio
import time
import traceback
from datetime import datetime

from backend.database import get_db
from backend.services.url_parser import parse_portal_url
from backend.services.property_matcher import match_property
from backend.services.knowledge_service import lookup_platform, record_usage, is_phone_required
from backend.services.r2_property_lookup import search_property as r2_search
from backend.scrapers.itanji_checker import check_vacancy as itanji_check
from backend.scrapers.itanji_checker import check_vacancy_by_url as itanji_check_by_url
from backend.scrapers.es_square_checker import check_vacancy as es_square_check
from backend.scrapers.es_square_checker import check_vacancy_by_url as es_square_check_by_url
from backend.scrapers.goweb_checker import check_vacancy as goweb_check
from backend.scrapers.bukkaku_checker import check_vacancy as bukkaku_check
from backend.scrapers.es_b2b_checker import check_vacancy as es_b2b_check
from backend.scrapers.ierabu_bb_checker import check_vacancy as ierabu_bb_check
from backend.scrapers.realpro_checker import check_vacancy as realpro_check
from backend.scrapers.browser_manager import platform_lock
from backend.credentials_map import get_platform_key, parse_platform_key
from backend.notifications.line_notifier import send_line_notification, send_akishitsu_result, set_akishitsu_conversation_state
# Slack通知は一時無効化（復活時にコメント解除）
# from backend.notifications.slack_notifier import send_slack_notification
SLACK_ENABLED = False

# ============================================================
# Neon リスナー用: 差し替え可能な DB I/O 関数
# ============================================================
# デフォルト: None → 既存SQLiteロジックを使用
# neon_listener.py からset_status_updater/set_record_fetcher で差し替え
_status_updater = None
_record_fetcher = None


def set_status_updater(fn):
    """ステータス更新関数を差し替え（Neonリスナー用）"""
    global _status_updater
    _status_updater = fn


def set_record_fetcher(fn):
    """レコード取得関数を差し替え（Neonリスナー用）"""
    global _record_fetcher
    _record_fetcher = fn


# ============================================================
# プラットフォーム別レートリミッター（IPバン防止）
# ============================================================
# プラットフォームごとの最小間隔（秒）
_RATE_LIMITS = {
    "itanji": 5,       # 常駐セッション — 検索間隔を確保
    "es_square": 5,    # 常駐セッション — 検索間隔を確保
    "goweb": 8,        # オンデマンド — ログイン含む可能性
    "bukkaku": 8,      # オンデマンド — サブドメイン別だがIPは同じ
    "es_b2b": 8,       # オンデマンド — SAML SSO含む可能性
    "ierabu_bb": 15,   # IPバン実績あり — 慎重に: 15秒間隔
    "realpro": 8,      # リアルネットプロ — 中程度の間隔
}
_DEFAULT_RATE_LIMIT = 5

# 最終リクエスト時刻を記録（プラットフォーム種別ごと）
_last_request_time: dict[str, float] = {}


async def _rate_limit(platform_type: str):
    """プラットフォームへのリクエスト前にレート制限を適用"""
    min_interval = _RATE_LIMITS.get(platform_type, _DEFAULT_RATE_LIMIT)
    now = time.time()
    last = _last_request_time.get(platform_type, 0)
    wait = min_interval - (now - last)
    if wait > 0:
        print(f"[rate_limit] {platform_type}: {wait:.1f}秒待機（IPバン防止）")
        await asyncio.sleep(wait)
    _last_request_time[platform_type] = time.time()


async def _update_status(check_id: int, **kwargs):
    """DBステータスを更新（リトライ付き）"""
    # Neonリスナー経由の場合は差し替え関数を使用
    if _status_updater:
        await _status_updater(check_id, **kwargs)
        return

    # 既存SQLiteロジック（ローカルフロントエンド用）
    sets = []
    values = []
    for key, val in kwargs.items():
        sets.append(f"{key} = ?")
        values.append(val)
    values.append(check_id)
    sql = f"UPDATE check_requests SET {', '.join(sets)} WHERE id = ?"

    for attempt in range(3):
        try:
            db = await get_db()
            try:
                await db.execute(sql, values)
                await db.commit()
                return
            finally:
                await db.close()
        except Exception as e:
            if "database is locked" in str(e) and attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise


async def run_vacancy_check_from_property_info(
    check_id: int,
    property_name: str,
    address: str,
    rent: str,
    area: str,
    layout: str,
    build_year: str,
):
    """図面解析結果からの空室確認（URLパースをスキップ、ATBB照合から開始）"""
    try:
        await _step_match(check_id, property_name, address, rent, area, layout, build_year)
    except Exception as e:
        await _update_status(
            check_id,
            status="error",
            error_message=f"{type(e).__name__}: {e}",
        )
        traceback.print_exc()


async def _fetch_record(check_id: int):
    """DB からレコードを取得（差し替え可能）"""
    if _record_fetcher:
        return await _record_fetcher(check_id)

    # 既存SQLiteロジック
    db = await get_db()
    try:
        row = await db.execute("SELECT * FROM check_requests WHERE id = ?", (check_id,))
        return await row.fetchone()
    finally:
        await db.close()


async def run_vacancy_check(check_id: int):
    """空室確認メイン処理（バックグラウンドタスク）"""
    try:
        record = await _fetch_record(check_id)

        if not record:
            return

        url = record["submitted_url"]
        portal = record["portal_source"]
        status = record["status"]

        if status == "pending":
            await _step_parse(check_id, url, portal)
        elif status == "awaiting_platform":
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
        property_build_year=info.get("build_year", ""),
        status="matching",
    )

    await _step_match(
        check_id,
        info.get("property_name", ""),
        info.get("address", ""),
        info.get("rent", ""),
        info.get("area", ""),
        info.get("layout", ""),
        info.get("build_year", ""),
    )


async def _step_match(
    check_id: int,
    property_name: str,
    address: str,
    rent: str,
    area: str,
    layout: str,
    build_year_text: str = "",
):
    """ステップ2: ATBB照合 + R2ショートカット"""
    matched = await match_property(property_name, address, rent, area, layout, build_year_text)

    # 物件名から号室を分離（property_nameに含まれる場合）
    original_name = property_name
    original_room = ""
    if "/" in property_name:
        parts = property_name.rsplit("/", 1)
        original_name = parts[0].strip()
        original_room = parts[1].strip()

    if not matched:
        # ============================================================
        # ATBBで見つからない → R2で検索してみる
        # ATBBにない＝専任とは限らない。R2にあればプラットフォームで確認可能。
        # ============================================================
        print(f"[空確] ATBB不一致 → R2フォールバック検索: {original_name} {original_room}")
        r2_result = await r2_search(original_name, original_room, address)

        if r2_result and r2_result.get("detail_url"):
            r2_source = r2_result["source"]
            r2_url = r2_result["detail_url"]
            r2_score = r2_result.get("score", 0)

            print(f"[空確] R2ヒット（ATBB不一致だがR2にあり）: {r2_source} (score={r2_score})")
            await _update_status(
                check_id,
                atbb_matched=False,
                platform=r2_source,
                platform_auto=True,
                status="checking",
            )

            try:
                await _step_check_direct(check_id, r2_url, r2_source, original_room)
                return  # R2経由で完了
            except Exception as e:
                print(f"[空確] R2直接確認失敗（ATBB不一致ケース）: {e}")
                # R2も失敗 → 確認不可

        # ATBBもR2もダメ → 確認不可
        await _update_status(
            check_id,
            atbb_matched=False,
            platform="",
            platform_auto=False,
            status="not_found",
            vacancy_result="確認不可（専任物件の可能性）",
            completed_at=datetime.now().isoformat(),
        )
        await _notify(check_id, property_name, "確認不可（専任物件の可能性）", "")
        return

    company_info = matched.get("管理会社情報", "")
    await _update_status(
        check_id,
        atbb_matched=True,
        atbb_company=company_info,
    )

    # ============================================================
    # ★ R2ショートカット: プラットフォーム検索をスキップ
    # ATBBでマッチした物件名＋号室でR2を検索し、
    # ヒットすれば詳細URLに直接アクセスして空室確認
    # ============================================================
    atbb_name = matched.get("名前", property_name)
    atbb_room = matched.get("号室", "") or original_room

    # 複数の名前候補でR2を検索（ATBB名 → 元のSUUMO/HOMES名）
    r2_result = None
    for candidate_name in [atbb_name, original_name]:
        r2_result = await r2_search(candidate_name, atbb_room, address)
        if r2_result:
            break

    if r2_result and r2_result.get("detail_url"):
        r2_source = r2_result["source"]
        r2_url = r2_result["detail_url"]
        r2_score = r2_result.get("score", 0)

        # R2でヒット → 直接URLで空室確認
        print(f"[空確] R2ヒット: {atbb_name} → {r2_source} (score={r2_score})")
        await _update_status(
            check_id,
            platform=r2_source,
            platform_auto=True,
            status="checking",
        )

        try:
            await _step_check_direct(check_id, r2_url, r2_source, atbb_room)
            return  # R2経由で完了
        except Exception as e:
            print(f"[空確] R2直接確認失敗 → 従来フローにフォールバック: {e}")
            # R2失敗 → 従来のフローに落ちる

    # ============================================================
    # 従来フロー: プラットフォーム判定 → 検索確認
    # ============================================================

    # ステップ3: プラットフォーム判定
    company_name = company_info.split(" ")[0] if company_info else ""
    company_phone = ""
    parts = company_info.split(" ")
    if len(parts) > 1:
        company_phone = parts[-1]

    # この管理会社が「電話確認必要」と学習済みか確認
    if await is_phone_required(company_name):
        await _create_phone_task(
            check_id, company_name, company_phone, property_name, address,
            "ウェブ確認不可（学習済み）"
        )
        await _update_status(
            check_id,
            status="done",
            vacancy_result="電話確認タスク作成済み",
            completed_at=datetime.now().isoformat(),
        )
        await _notify(check_id, property_name, "電話確認が必要です", "")
        return

    platform = await lookup_platform(company_name, company_phone)

    if platform == "phone":
        # 電話確認が必要な管理会社（静的マッピング済み）
        await _create_phone_task(
            check_id, company_name, company_phone, property_name, address,
            "ウェブ確認不可（電話確認対象会社）"
        )
        await _update_status(
            check_id,
            platform="phone",
            status="done",
            vacancy_result="電話確認タスク作成済み",
            completed_at=datetime.now().isoformat(),
        )
        await _notify(check_id, property_name, "電話確認が必要です", "")
    elif platform:
        await _update_status(
            check_id,
            platform=platform,
            platform_auto=True,
            status="checking",
        )
        await _step_check(check_id)
    else:
        # プラットフォーム不明 → イタンジBB/e生活スクエアでフォールバック検索
        print(f"[空確] プラットフォーム不明: {company_name} → フォールバック検索")
        # 号室分離
        fb_room = ""
        if "/" in property_name:
            fb_parts = property_name.rsplit("/", 1)
            fb_name = fb_parts[0].strip()
            fb_room = fb_parts[1].strip()
        else:
            import re as _re
            m = _re.search(r'\s+(\d{1,4})\s*号?室?\s*$', property_name)
            if m:
                fb_room = m.group(1)
                fb_name = property_name[:m.start()].strip()
            else:
                fb_name = property_name

        fb_result = await _fallback_search(fb_name, fb_room, address)
        if fb_result != "該当なし":
            await _update_status(
                check_id,
                platform="fallback",
                platform_auto=True,
                status="done",
                vacancy_result=fb_result,
                completed_at=datetime.now().isoformat(),
            )
            await _notify(check_id, property_name, fb_result, "フォールバック検索")
        else:
            # フォールバックでも見つからない → awaiting_platform
            await _update_status(check_id, status="awaiting_platform", platform="", platform_auto=False)


async def _step_check_direct(check_id: int, detail_url: str, platform: str, room_number: str = ""):
    """ステップ4a: R2経由の直接URL空室確認（高速パス）

    R2インデックスから取得した詳細URLに直接アクセスして空室判定を行う。
    プラットフォーム上での検索ステップを完全にスキップするため大幅に高速。
    """
    record = await _fetch_record(check_id)

    if not record:
        return

    property_name = record["property_name"]
    property_address = record["property_address"] or ""

    try:
        async with platform_lock(platform):
            await _rate_limit(platform)
            if platform == "itanji":
                result = await itanji_check_by_url(detail_url, room_number)
            elif platform == "es_square":
                result = await es_square_check_by_url(detail_url, room_number)
            elif platform == "ierabu_bb":
                # いえらぶBBはR2直接URLなし → 従来フローで検索
                raise RuntimeError("いえらぶBBはR2直接確認未対応 → 検索フロー")
            else:
                raise RuntimeError(f"R2直接確認未対応のプラットフォーム: {platform}")
    except Exception as e:
        # R2直接確認失敗 → エラーをraiseして呼び出し元でフォールバック
        raise

    # 結果が「該当なし」の場合 → R2のURLが古い可能性があるため、
    # 従来のフロー（検索ベース）にフォールバックするためraiseする
    if result == "該当なし":
        print(f"[空確] R2直接確認で「該当なし」→ 従来フローにフォールバック")
        raise RuntimeError("R2 URL先で該当なし（URLが古い可能性）")

    # 正常結果
    await _update_status(
        check_id,
        vacancy_result=f"{result}（R2直接確認）",
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

    platform_names = {
        "itanji": "イタンジBB（R2直接）",
        "es_square": "いい生活スクエア（R2直接）",
        "ierabu_bb": "いえらぶBB（R2直接）",
        "bukkaku": "物確.com（R2直接）",
        "es_b2b": "いい生活B2B（R2直接）",
    }
    await _notify(check_id, property_name, result, platform_names.get(platform, platform))


async def _fallback_search(property_name: str, room_number: str, address: str = "") -> str:
    """未実装プラットフォーム・不明な管理会社の場合のフォールバック検索

    イタンジBB → e生活スクエア の順に物件名で検索し、
    ヒットすればその結果を返す。見つからなければ "該当なし" を返す。
    """
    # イタンジBBで検索
    try:
        async with platform_lock("itanji"):
            await _rate_limit("itanji")
            result = await itanji_check(property_name, room_number)
        if result != "該当なし":
            print(f"[空確] フォールバック: イタンジBBでヒット → {result}")
            return result
    except Exception as e:
        print(f"[空確] フォールバック: イタンジBB検索エラー: {e}")

    # e生活スクエアで検索
    try:
        async with platform_lock("es_square"):
            await _rate_limit("es_square")
            result = await es_square_check(property_name, room_number, address)
        if result != "該当なし":
            print(f"[空確] フォールバック: e生活スクエアでヒット → {result}")
            return result
    except Exception as e:
        print(f"[空確] フォールバック: e生活スクエア検索エラー: {e}")

    print(f"[空確] フォールバック: イタンジBB/e生活スクエアともに該当なし: {property_name}")
    return "該当なし"


async def _step_check(check_id: int):
    """ステップ4: 空室確認（従来のプラットフォーム検索方式）"""
    record = await _fetch_record(check_id)

    if not record:
        return

    platform = record["platform"]
    property_name = record["property_name"]
    property_address = record["property_address"] or ""

    # 物件名から号室を分離
    # パターン: "物件名/303", "物件名 303号室", "物件名 303"
    room_number = ""
    if "/" in property_name:
        parts = property_name.rsplit("/", 1)
        property_name = parts[0].strip()
        room_number = parts[1].strip()
    else:
        import re as _re
        m = _re.search(r'\s+(\d{1,4})\s*号?室?\s*$', property_name)
        if m:
            room_number = m.group(1)
            property_name = property_name[:m.start()].strip()

    try:
        # プラットフォーム種別でディスパッチ
        # 複合キー "bukkaku:CIC" / "es_b2b:TFD" の場合は split して credential_key を渡す
        platform_type, credential_key = parse_platform_key(platform)

        # プラットフォームのページを排他的に使用（同時操作によるTargetClosedError防止）
        async with platform_lock(platform_type):
            await _rate_limit(platform_type)

            if platform_type == "itanji":
                result = await itanji_check(property_name, room_number)
            elif platform_type == "es_square":
                result = await es_square_check(property_name, room_number, property_address)
            elif platform_type == "goweb":
                result = await goweb_check(property_name, room_number, credential_key)
            elif platform_type == "bukkaku":
                result = await bukkaku_check(property_name, room_number, credential_key)
            elif platform_type == "es_b2b":
                result = await es_b2b_check(property_name, room_number, credential_key)
            elif platform_type == "ierabu_bb":
                result = await ierabu_bb_check(property_name, room_number)
            elif platform_type == "realpro":
                result = await realpro_check(property_name, room_number)
            elif platform_type in ("dkpartners", "skips", "kimaroom"):
                # 未実装プラットフォーム → イタンジBB/e生活スクエアでフォールバック検索
                print(f"[空確] {platform_type}チェッカー未実装: {property_name} → イタンジBB/e生活スクエアでフォールバック検索")
                result = await _fallback_search(property_name, room_number, property_address)
            else:
                # 不明なプラットフォーム → フォールバック検索
                print(f"[空確] 不明なプラットフォーム {platform_type}: {property_name} → フォールバック検索")
                result = await _fallback_search(property_name, room_number, property_address)
    except Exception as e:
        # プラットフォームで確認できなかった → 電話確認タスクに振り分け
        err_detail = f"{type(e).__name__}: {e}"
        traceback.print_exc()

        company_info = record["atbb_company"] or ""
        company_name = company_info.split(" ")[0] if company_info else ""
        company_phone = ""
        parts = company_info.split(" ")
        if len(parts) > 1:
            company_phone = parts[-1]

        await _create_phone_task(
            check_id, company_name, company_phone,
            record["property_name"], property_address,
            f"プラットフォーム確認エラー ({platform}): {err_detail}"
        )

        await _update_status(
            check_id,
            status="done",
            vacancy_result="電話確認タスク作成済み",
            error_message=f"空室確認エラー ({platform}): {err_detail}",
            completed_at=datetime.now().isoformat(),
        )
        await _notify(check_id, record["property_name"], "電話確認が必要です", "")
        return

    # プラットフォームで「該当なし」→ 電話確認タスクも生成
    if result == "該当なし":
        company_info = record["atbb_company"] or ""
        company_name = company_info.split(" ")[0] if company_info else ""
        company_phone = ""
        parts = company_info.split(" ")
        if len(parts) > 1:
            company_phone = parts[-1]

        await _create_phone_task(
            check_id, company_name, company_phone,
            record["property_name"], property_address,
            f"{platform}で該当なし"
        )

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

    platform_names = {
        "itanji": "イタンジBB", "es_square": "いい生活スクエア", "goweb": "GoWeb",
        "bukkaku": "物確.com", "es_b2b": "いい生活B2B", "ierabu_bb": "いえらぶBB",
        "realpro": "リアルネットプロ",
    }
    await _notify(check_id, record["property_name"], result, platform_names.get(platform_type, platform))


async def _create_phone_task(
    check_id: int,
    company_name: str,
    company_phone: str,
    property_name: str,
    property_address: str,
    reason: str,
):
    """電話確認タスクを作成してSlackに通知"""
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO phone_tasks
               (check_request_id, company_name, company_phone, property_name, property_address, reason)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (check_id, company_name, company_phone, property_name, property_address, reason),
        )
        await db.commit()
    finally:
        await db.close()

    # Slack通知
    message = (
        f":telephone_receiver: 【電話確認タスク】\n"
        f"物件名: {property_name}\n"
        f"住所: {property_address}\n"
        f"管理会社: {company_name}\n"
        f"電話: {company_phone}\n"
        f"理由: {reason}"
    )
    if SLACK_ENABLED:
        try:
            from backend.notifications.slack_notifier import send_slack_notification
            await send_slack_notification(message)
        except Exception:
            pass


async def _notify(check_id: int, property_name: str, result: str, platform_name: str):
    """結果を通知（管理者 + ユーザー）"""
    message = f"【空確くん】{property_name}\n結果: {result}"
    if platform_name:
        message += f"\n確認先: {platform_name}"

    # 管理者向け通知
    try:
        await send_line_notification(message)
    except Exception:
        pass

    if SLACK_ENABLED:
        try:
            from backend.notifications.slack_notifier import send_slack_notification
            await send_slack_notification(message)
        except Exception:
            pass

    # ユーザー向けLINEプッシュ通知（line_user_idが紐付いている場合）
    try:
        record = await _fetch_record(check_id)
        if record and record.get("line_user_id") and not record.get("line_notified"):
            await send_akishitsu_result(
                record["line_user_id"], property_name, result, check_id
            )
            # 会話状態を設定（Webhookでボタン押下を処理するため）
            await set_akishitsu_conversation_state(
                record["line_user_id"], check_id, property_name, result
            )
            # 重複通知防止フラグを設定
            await _update_status(check_id, line_notified=True)
    except Exception:
        pass
