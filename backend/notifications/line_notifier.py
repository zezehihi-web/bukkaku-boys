"""LINE Messaging API 通知"""

import logging
import httpx

from backend.config import LINE_CHANNEL_ACCESS_TOKEN, LINE_USER_ID

LINE_API_URL = "https://api.line.me/v2/bot/message/push"
log = logging.getLogger(__name__)


async def send_line_notification(message: str):
    """LINE Messaging APIでプッシュ通知を送信（管理者向け）"""
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_USER_ID:
        return  # 未設定時はスキップ

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            LINE_API_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
            },
            json={
                "to": LINE_USER_ID,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10.0,
        )
        resp.raise_for_status()


def _result_color(result: str) -> str:
    """結果テキストに応じたカラーコードを返す"""
    if result.startswith("募集中"):
        return "#06C755"
    if result.startswith("申込あり") or result.startswith("募集終了"):
        return "#E53935"
    if result.startswith("電話確認"):
        return "#1E88E5"
    if result.startswith("確認不可"):
        return "#FF8F00"
    return "#888888"


def _result_emoji(result: str) -> str:
    """結果テキストに応じた絵文字を返す"""
    if result.startswith("募集中"):
        return "🟢"
    if result.startswith("申込あり") or result.startswith("募集終了"):
        return "🔴"
    if result.startswith("電話確認"):
        return "📞"
    if result.startswith("確認不可"):
        return "⚠️"
    return "📋"


def _build_follow_up_flex(property_name: str, vacancy_result: str) -> dict:
    """結果に応じたフォローアップFlexメッセージを構築"""
    is_available = vacancy_result.startswith("募集中")

    buttons = []
    if is_available:
        buttons.extend([
            {"type": "button", "style": "primary", "color": "#06C755", "height": "sm",
             "action": {"type": "message", "label": "申し込みしたい", "text": "申し込みしたい"}},
            {"type": "button", "style": "primary", "color": "#1E88E5", "height": "sm",
             "action": {"type": "message", "label": "内見したい", "text": "内見したい"}},
            {"type": "button", "style": "primary", "color": "#FF9500", "height": "sm",
             "action": {"type": "message", "label": "相談したい", "text": "相談したい"}},
        ])
    else:
        buttons.append(
            {"type": "button", "style": "primary", "color": "#FF9500", "height": "sm",
             "action": {"type": "message", "label": "相談したい", "text": "相談したい"}},
        )

    buttons.append(
        {"type": "button", "style": "secondary", "color": "#9CA3AF", "height": "sm",
         "action": {"type": "message", "label": "別の物件を確認する", "text": "別の物件を確認する"}},
    )

    if is_available:
        header_text = "この物件にご興味がありますか？"
    elif vacancy_result.startswith("申込あり") or vacancy_result.startswith("募集終了"):
        header_text = "残念ながら、この物件は現在募集を行っておりません。"
    else:
        header_text = "この物件の空室確認ができませんでした。"

    return {
        "type": "flex",
        "altText": header_text,
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": header_text, "weight": "bold", "size": "sm",
                     "color": "#333333", "wrap": True, "align": "center"},
                    {"type": "separator", "margin": "lg"},
                    {"type": "box", "layout": "vertical", "spacing": "sm", "margin": "lg",
                     "contents": buttons},
                ],
                "paddingAll": "16px",
            },
        },
    }


async def send_akishitsu_result(
    line_user_id: str,
    property_name: str,
    vacancy_result: str,
    check_id: int,
):
    """空確くんユーザー向けの結果プッシュ通知（Flex Message + フォローアップ）"""
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_user_id:
        return

    color = _result_color(vacancy_result)
    emoji = _result_emoji(vacancy_result)

    flex_message = {
        "type": "flex",
        "altText": f"【空確くん】{property_name} → {vacancy_result}",
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": "🏠 空確くん 結果通知",
                        "weight": "bold",
                        "size": "sm",
                        "color": "#1DB446",
                    }
                ],
                "paddingAll": "14px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": property_name or "物件",
                        "weight": "bold",
                        "size": "md",
                        "wrap": True,
                    },
                    {"type": "separator", "margin": "md"},
                    {
                        "type": "text",
                        "text": f"{emoji} {vacancy_result}",
                        "weight": "bold",
                        "size": "lg",
                        "color": color,
                        "align": "center",
                        "wrap": True,
                        "margin": "lg",
                    },
                ],
                "paddingAll": "14px",
            },
            "styles": {"header": {"backgroundColor": "#F5F5F5"}},
        },
    }

    # 送信するメッセージリスト（結果 + フォローアップ）
    messages = [flex_message]

    if not vacancy_result.startswith("電話確認"):
        # フォローアップFlexを追加
        follow_up = _build_follow_up_flex(property_name, vacancy_result)
        messages.append(follow_up)
    else:
        # 電話確認の場合は案内テキスト
        messages.append({
            "type": "text",
            "text": "この物件は管理会社への電話確認が必要なため、スタッフが確認のうえ結果をお伝えします。\n\nしばらくお待ちくださいませ。",
        })

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                LINE_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                },
                json={
                    "to": line_user_id,
                    "messages": messages,
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            log.info("Sent akishitsu result + follow-up to user %s (check %d)", line_user_id[:8], check_id)
    except Exception as exc:
        log.warning("Failed to send akishitsu result to %s: %s", line_user_id[:8], exc)


async def send_akishitsu_batch_result(
    line_user_id: str,
    results: list[dict],
):
    """バッチ結果をまとめて1通のFlexメッセージで送信

    results: [{"check_id": int, "property_name": str, "vacancy_result": str}, ...]
    """
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_user_id:
        return
    if not results:
        return

    # 各物件の結果行を構築
    result_rows = []
    available_names = []  # 募集中の物件名リスト
    available_ids = []    # 募集中のcheck_idリスト

    for item in results:
        name = item.get("property_name", "物件")
        result = item.get("vacancy_result", "")
        check_id = item.get("check_id", 0)
        emoji = _result_emoji(result)
        color = _result_color(result)

        if result.startswith("募集中"):
            available_names.append(name)
            available_ids.append(check_id)

        # 物件名
        result_rows.append({
            "type": "text",
            "text": name,
            "size": "sm",
            "weight": "bold",
            "wrap": True,
            "color": "#333333",
        })
        # 結果
        result_rows.append({
            "type": "text",
            "text": f"{emoji} {result}",
            "size": "sm",
            "color": color,
            "wrap": True,
        })
        # セパレーター（最後以外）
        if item != results[-1]:
            result_rows.append({"type": "separator", "margin": "md"})

    # まとめFlexメッセージ
    summary_flex = {
        "type": "flex",
        "altText": f"【空確くん】{len(results)}件の確認結果",
        "contents": {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": f"🏠 空確くん 一括確認結果（{len(results)}件）",
                        "weight": "bold",
                        "size": "sm",
                        "color": "#1DB446",
                    }
                ],
                "paddingAll": "14px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": result_rows,
                "paddingAll": "14px",
            },
            "styles": {"header": {"backgroundColor": "#F5F5F5"}},
        },
    }

    messages = [summary_flex]

    # フォローアップ: 募集中物件があればまとめて聞く
    if available_names:
        names_text = "\n".join(f"・{n}" for n in available_names)
        follow_up = _build_batch_follow_up_flex(names_text, len(available_names))
        messages.append(follow_up)
    else:
        # 募集中がなければ別の物件を確認するだけ
        messages.append({
            "type": "flex",
            "altText": "他の物件を確認しますか？",
            "contents": {
                "type": "bubble",
                "size": "kilo",
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                        {"type": "text", "text": "募集中の物件はありませんでした。",
                         "weight": "bold", "size": "sm", "color": "#333333",
                         "wrap": True, "align": "center"},
                        {"type": "separator", "margin": "lg"},
                        {"type": "box", "layout": "vertical", "spacing": "sm", "margin": "lg",
                         "contents": [
                            {"type": "button", "style": "primary", "color": "#FF9500", "height": "sm",
                             "action": {"type": "message", "label": "相談したい", "text": "相談したい"}},
                            {"type": "button", "style": "secondary", "color": "#9CA3AF", "height": "sm",
                             "action": {"type": "message", "label": "別の物件を確認する", "text": "別の物件を確認する"}},
                        ]},
                    ],
                    "paddingAll": "16px",
                },
            },
        })

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                LINE_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                },
                json={
                    "to": line_user_id,
                    "messages": messages[:5],  # LINE API上限5メッセージ
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            log.info("Sent akishitsu batch result (%d items) to user %s", len(results), line_user_id[:8])
    except Exception as exc:
        log.warning("Failed to send batch result to %s: %s", line_user_id[:8], exc)


def _build_batch_follow_up_flex(available_names_text: str, count: int) -> dict:
    """バッチ結果用フォローアップ — 募集中物件をまとめて次のアクションを聞く"""
    return {
        "type": "flex",
        "altText": f"募集中の物件が{count}件あります",
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": f"募集中の物件が{count}件あります！",
                     "weight": "bold", "size": "sm", "color": "#06C755",
                     "wrap": True, "align": "center"},
                    {"type": "text", "text": available_names_text,
                     "size": "xs", "color": "#666666", "wrap": True, "margin": "md"},
                    {"type": "separator", "margin": "lg"},
                    {"type": "box", "layout": "vertical", "spacing": "sm", "margin": "lg",
                     "contents": [
                        {"type": "button", "style": "primary", "color": "#06C755", "height": "sm",
                         "action": {"type": "message", "label": "申し込みしたい", "text": "申し込みしたい"}},
                        {"type": "button", "style": "primary", "color": "#1E88E5", "height": "sm",
                         "action": {"type": "message", "label": "内見したい", "text": "内見したい"}},
                        {"type": "button", "style": "primary", "color": "#FF9500", "height": "sm",
                         "action": {"type": "message", "label": "相談したい", "text": "相談したい"}},
                        {"type": "button", "style": "secondary", "color": "#9CA3AF", "height": "sm",
                         "action": {"type": "message", "label": "別の物件を確認する", "text": "別の物件を確認する"}},
                    ]},
                ],
                "paddingAll": "16px",
            },
        },
    }


async def set_akishitsu_conversation_state(
    line_user_id: str,
    check_id: int,
    property_name: str,
    vacancy_result: str,
):
    """空確くんの会話状態をNeon PostgreSQLに設定（Next.js側のWebhookで使用）

    Python backend から Neon DB (line_conversation_states テーブル) に直接書き込み、
    Webhook ハンドラーがボタン押下時に参照できるようにする。
    """
    import os
    from urllib.parse import urlparse

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        log.warning("DATABASE_URL not set, skipping conversation state set")
        return

    # 電話確認の場合は会話状態を設定しない
    if vacancy_result.startswith("電話確認"):
        return

    # Parse DATABASE_URL to get Neon HTTP endpoint
    parsed = urlparse(database_url)
    neon_host = parsed.hostname
    http_url = f"https://{neon_host}/sql"

    query = (
        "INSERT INTO line_conversation_states "
        "(line_user_id, step, case_id, source, akishitsu_check_id, akishitsu_property_name, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, NOW()) "
        "ON CONFLICT (line_user_id) DO UPDATE SET "
        "step = EXCLUDED.step, case_id = EXCLUDED.case_id, source = EXCLUDED.source, "
        "akishitsu_check_id = EXCLUDED.akishitsu_check_id, "
        "akishitsu_property_name = EXCLUDED.akishitsu_property_name, "
        "updated_at = NOW()"
    )
    params = [
        line_user_id,
        "akishitsu_next_step",
        f"akishitsu_{check_id}",
        "akishitsu",
        check_id,
        property_name,
    ]

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                http_url,
                headers={
                    "Content-Type": "application/json",
                    "Neon-Connection-String": database_url,
                },
                json={"query": query, "params": params},
                timeout=5.0,
            )
            resp.raise_for_status()
            log.info("Set akishitsu conversation state for user %s", line_user_id[:8])
    except Exception as exc:
        log.warning("Failed to set conversation state for %s: %s", line_user_id[:8], exc)
