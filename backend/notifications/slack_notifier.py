"""Slack 通知（Bot Token API または Incoming Webhook）"""

import httpx

from backend.config import SLACK_WEBHOOK_URL, SLACK_BOT_TOKEN, SLACK_CHANNEL

SLACK_API_URL = "https://slack.com/api/chat.postMessage"


async def send_slack_notification(message: str):
    """Slackでメッセージを送信（Bot Token優先、フォールバックでWebhook）"""
    if SLACK_BOT_TOKEN and SLACK_CHANNEL:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                SLACK_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                },
                json={"channel": SLACK_CHANNEL, "text": message},
                timeout=10.0,
            )
            resp.raise_for_status()
        return

    if SLACK_WEBHOOK_URL:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                SLACK_WEBHOOK_URL,
                json={"text": message},
                timeout=10.0,
            )
            resp.raise_for_status()
        return

    # どちらも未設定 → スキップ
