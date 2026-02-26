"""LINE Messaging API 通知"""

import httpx

from backend.config import LINE_CHANNEL_ACCESS_TOKEN, LINE_USER_ID

LINE_API_URL = "https://api.line.me/v2/bot/message/push"


async def send_line_notification(message: str):
    """LINE Messaging APIでプッシュ通知を送信"""
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
