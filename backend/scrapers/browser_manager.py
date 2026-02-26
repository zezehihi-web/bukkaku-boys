"""Playwright ブラウザ常駐管理

各プラットフォーム（イタンジBB / いえらぶBB / いい生活スクエア）ごとに
独立したブラウザコンテキストを管理し、ログイン状態を維持する。
"""

import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

_playwright = None
_browser: Browser | None = None
_contexts: dict[str, BrowserContext] = {}
_pages: dict[str, Page] = {}
_lock = asyncio.Lock()


async def _ensure_browser() -> Browser:
    """ブラウザインスタンスを取得（未起動なら起動）"""
    global _playwright, _browser
    if _browser is None or not _browser.is_connected():
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
        )
    return _browser


async def get_page(platform: str) -> Page:
    """指定プラットフォーム用のページを取得（Cookie分離のため別コンテキスト）

    Args:
        platform: 'itanji' / 'ierabu' / 'es_square'
    """
    async with _lock:
        if platform in _pages:
            page = _pages[platform]
            # ページがまだ生きているか確認
            try:
                await page.title()
                return page
            except Exception:
                # ページが閉じている→再作成
                pass

        browser = await _ensure_browser()
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()
        _contexts[platform] = context
        _pages[platform] = page
        return page


async def close_all():
    """全ブラウザを閉じる"""
    global _browser, _playwright
    for ctx in _contexts.values():
        try:
            await ctx.close()
        except Exception:
            pass
    _contexts.clear()
    _pages.clear()
    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None
    if _playwright:
        try:
            await _playwright.stop()
        except Exception:
            pass
        _playwright = None
