"""いえらぶBB 空室確認チェッカー

いえらぶBBにログインし、物件名で検索してステータスを判定する。
"""

from playwright.async_api import Page

from backend.config import IERABU_EMAIL, IERABU_PASSWORD, IERABU_LOGIN_URL
from backend.scrapers.browser_manager import get_page

STATUS_KEYWORDS = {
    "募集中": "募集中",
    "空室": "募集中",
    "空き": "募集中",
    "申込あり": "申込あり",
    "申込中": "申込あり",
    "申し込みあり": "申込あり",
    "紹介不可": "募集終了",
    "募集終了": "募集終了",
    "成約済": "募集終了",
    "成約済み": "募集終了",
    "取り下げ": "募集終了",
}


async def _login(page: Page) -> bool:
    """いえらぶBBにログイン"""
    if not IERABU_EMAIL or not IERABU_PASSWORD:
        raise ValueError("IERABU_EMAIL/IERABU_PASSWORD が未設定です")

    await page.goto(IERABU_LOGIN_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    # メールアドレス入力
    email_selectors = [
        'input[name="login_id"]',
        'input[name="email"]',
        "#login_id",
        "#email",
        'input[type="email"]',
        'input[type="text"]',
    ]
    email_input = None
    for sel in email_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            email_input = loc.first
            break
    if not email_input:
        raise RuntimeError("いえらぶBB: ログインID入力欄が見つかりません")

    await email_input.fill(IERABU_EMAIL)
    await page.wait_for_timeout(300)

    # パスワード入力
    password_selectors = [
        'input[name="password"]',
        "#password",
        'input[type="password"]',
    ]
    password_input = None
    for sel in password_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            password_input = loc.first
            break
    if not password_input:
        raise RuntimeError("いえらぶBB: パスワード入力欄が見つかりません")

    await password_input.fill(IERABU_PASSWORD)
    await page.wait_for_timeout(300)

    # ログインボタン
    btn_selectors = [
        'button:has-text("ログイン")',
        'input[type="submit"]',
        'button[type="submit"]',
        '#login_btn',
    ]
    login_btn = None
    for sel in btn_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            login_btn = loc.first
            break
    if not login_btn:
        raise RuntimeError("いえらぶBB: ログインボタンが見つかりません")

    try:
        async with page.expect_navigation(timeout=30000):
            await login_btn.click()
    except Exception:
        await login_btn.click()

    await page.wait_for_timeout(3000)

    # ログイン後のURL確認
    if "ielove.jp" in page.url and "login" not in page.url:
        return True
    return False


async def _ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認"""
    current_url = page.url
    if "ielove.jp" in current_url and "login" not in current_url:
        return True
    return await _login(page)


def _detect_status(text: str) -> str:
    """テキストからステータスを判定"""
    for keyword, status in STATUS_KEYWORDS.items():
        if keyword in text:
            return status
    return ""


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """いえらぶBBで物件の空室状況を確認

    Returns:
        '募集中' / '申込あり' / '募集終了' / '該当なし'
    """
    page = await get_page("ierabu")
    await _ensure_logged_in(page)

    search_keyword = property_name
    if room_number:
        search_keyword = f"{property_name} {room_number}"

    # 検索バー
    search_selectors = [
        'input[placeholder*="検索"]',
        'input[placeholder*="物件名"]',
        'input[name*="keyword"]',
        'input[name*="search"]',
        'input[name*="freeword"]',
        'input[type="search"]',
    ]

    search_input = None
    for sel in search_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            search_input = loc.first
            break

    if not search_input:
        # 物件検索ページへ直接遷移
        await page.goto("https://bb.ielove.jp/ielovebb/bukken/search", wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        for sel in search_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                search_input = loc.first
                break

    if not search_input:
        raise RuntimeError("いえらぶBB: 検索欄が見つかりません")

    await search_input.fill(search_keyword)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(3000)

    body_text = await page.inner_text("body")

    if "該当する物件" in body_text and "ありません" in body_text:
        return "該当なし"
    if "0件" in body_text:
        return "該当なし"

    status = _detect_status(body_text)
    if status:
        return status

    # 最初の物件をクリックして詳細確認
    property_links = page.locator("a[href*='bukken']")
    if await property_links.count() > 0:
        await property_links.first.click()
        await page.wait_for_timeout(2000)
        detail_text = await page.inner_text("body")
        status = _detect_status(detail_text)
        if status:
            return status

    return "該当なし"
