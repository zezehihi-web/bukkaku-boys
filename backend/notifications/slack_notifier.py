"""Slack Incoming Webhook 通知"""

import httpx

from backend.config import SLACK_WEBHOOK_URL


async def send_slack_notification(message: str):
    """Slack Webhookでメッセージを送信"""
    if not SLACK_WEBHOOK_URL:
        return  # 未設定時はスキップ

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10.0,
        )
        resp.raise_for_status()
