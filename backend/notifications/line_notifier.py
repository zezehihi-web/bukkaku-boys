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


async def send_akishitsu_result(
    line_user_id: str,
    property_name: str,
    vacancy_result: str,
    check_id: int,
):
    """空確くんユーザー向けの結果プッシュ通知（Flex Message）"""
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
                    "messages": [flex_message],
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            log.info("Sent akishitsu result to user %s (check %d)", line_user_id[:8], check_id)
    except Exception as exc:
        log.warning("Failed to send akishitsu result to %s: %s", line_user_id[:8], exc)
