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
                        "maxLines": 2,
                    },
                    {"type": "separator", "margin": "md"},
                    {
                        "type": "box",
                        "layout": "horizontal",
                        "margin": "lg",
                        "contents": [
                            {
                                "type": "text",
                                "text": f"{emoji} {vacancy_result}",
                                "weight": "bold",
                                "size": "xl",
                                "color": color,
                                "align": "center",
                            }
                        ],
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


async def set_akishitsu_conversation_state(
    line_user_id: str,
    check_id: int,
    property_name: str,
    vacancy_result: str,
):
    """空確くんの会話状態をKVに設定（Next.js側のWebhookで使用）

    Python backend から KV (Upstash) に直接書き込み、
    Webhook ハンドラーがボタン押下時に参照できるようにする。
    """
    import os
    rest_url = os.getenv("UPSTASH_REDIS_REST_URL") or os.getenv("KV_REST_API_URL")
    rest_token = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("KV_REST_API_TOKEN")

    if not rest_url or not rest_token:
        log.warning("KV credentials not available, skipping conversation state set")
        return

    # 電話確認の場合は会話状態を設定しない
    if vacancy_result.startswith("電話確認"):
        return

    import json
    from datetime import datetime

    state = json.dumps({
        "line_user_id": line_user_id,
        "step": "akishitsu_next_step",
        "case_id": f"akishitsu_{check_id}",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "source": "akishitsu",
        "akishitsu_check_id": check_id,
        "akishitsu_property_name": property_name,
    })

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                rest_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {rest_token}",
                },
                json=["SET", f"conversation:{line_user_id}", state],
                timeout=5.0,
            )
            resp.raise_for_status()
            log.info("Set akishitsu conversation state for user %s", line_user_id[:8])
    except Exception as exc:
        log.warning("Failed to set conversation state for %s: %s", line_user_id[:8], exc)
