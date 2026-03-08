"""Playwright ブラウザ常駐管理

各プラットフォーム（イタンジBB / いい生活スクエア）ごとに
独立したブラウザコンテキストを管理し、ログイン状態を維持する。

重要: 各プラットフォームのページは1つしかないため、
platform_lock() で排他制御し、同時に複数コルーチンが
同じページを操作しないようにする。
"""

import asyncio
import sys
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

_IS_LINUX = sys.platform.startswith("linux")

_playwright = None
_browser: Browser | None = None
_contexts: dict[str, BrowserContext] = {}
_pages: dict[str, Page] = {}
_lock = asyncio.Lock()

# プラットフォームごとの排他ロック（同じページの同時操作を防止）
_platform_locks: dict[str, asyncio.Lock] = {}


async def _ensure_browser() -> Browser:
    """ブラウザインスタンスを取得（未起動なら起動）"""
    global _playwright, _browser
    if _browser is None or not _browser.is_connected():
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=_IS_LINUX,  # Linux(bravo)=headless, Windows=headed
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ] + ([] if _IS_LINUX else ["--start-maximized"]),
        )
    return _browser


async def get_page(platform: str) -> Page:
    """指定プラットフォーム用のページを取得（Cookie分離のため別コンテキスト）

    Args:
        platform: 'itanji' / 'es_square'
    """
    async with _lock:
        if platform in _pages:
            page = _pages[platform]
            # ページがまだ生きているか確認（5秒タイムアウト）
            try:
                await asyncio.wait_for(page.title(), timeout=5)
                return page
            except Exception:
                # ページが閉じている or 応答なし → 再作成
                try:
                    ctx = _contexts.pop(platform, None)
                    if ctx:
                        await ctx.close()
                except Exception:
                    pass
                _pages.pop(platform, None)

        browser = await _ensure_browser()
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()
        # 全ページ操作にデフォルトタイムアウト（60秒）
        page.set_default_timeout(60000)
        page.set_default_navigation_timeout(60000)
        _contexts[platform] = context
        _pages[platform] = page
        return page


def _get_platform_lock(platform: str) -> asyncio.Lock:
    """プラットフォーム用の排他ロックを取得（なければ作成）"""
    if platform not in _platform_locks:
        _platform_locks[platform] = asyncio.Lock()
    return _platform_locks[platform]


@asynccontextmanager
async def platform_lock(platform: str):
    """プラットフォームのページを排他的に使用するコンテキストマネージャ

    Usage:
        async with platform_lock("itanji"):
            page = await get_page("itanji")
            await page.goto(...)
            ...

    これにより、同じプラットフォームのページに対する
    複数コルーチンの同時操作を防止する。
    """
    lock = _get_platform_lock(platform)
    await lock.acquire()
    try:
        yield
    finally:
        lock.release()


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
