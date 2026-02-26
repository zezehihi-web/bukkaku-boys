"""イタンジBB 空室確認チェッカー

既存のscrape_itanji.pyのauto_login()ロジックを再利用。
物件名で検索し、募集ステータスを判定する。
"""

from playwright.async_api import Page

from backend.config import ITANJI_EMAIL, ITANJI_PASSWORD, ITANJI_TOP_URL
from backend.scrapers.browser_manager import get_page

# ステータス判定キーワード
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
    "取り下げ": "募集終了",
}


async def _login(page: Page) -> bool:
    """イタンジBBにログイン（scrape_itanji.py auto_login()準拠）"""
    if not ITANJI_EMAIL or not ITANJI_PASSWORD:
        raise ValueError("ITANJI_EMAIL/ITANJI_PASSWORD が未設定です")

    await page.goto(ITANJI_TOP_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    # ログインリンクを探す
    login_selectors = [
        'a:has-text("ログイン")',
        'a[href*="login"]',
        'a[href*="itandi-accounts"]',
    ]
    login_link = None
    for sel in login_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            login_link = loc.first
            break

    if not login_link:
        raise RuntimeError("イタンジBB: ログインリンクが見つかりません")

    try:
        async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
            await login_link.click()
    except Exception:
        await login_link.click()

    await page.wait_for_timeout(2000)

    # メールアドレス入力
    email_input = None
    for sel in ["#email", 'input[name="email"]', 'input[type="email"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            email_input = loc.first
            break
    if not email_input:
        raise RuntimeError("イタンジBB: メールアドレス入力欄が見つかりません")

    await email_input.fill(ITANJI_EMAIL)
    await page.wait_for_timeout(500)

    # パスワード入力
    password_input = None
    for sel in ["#password", 'input[name="password"]', 'input[type="password"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            password_input = loc.first
            break
    if not password_input:
        raise RuntimeError("イタンジBB: パスワード入力欄が見つかりません")

    await password_input.fill(ITANJI_PASSWORD)
    await page.wait_for_timeout(500)

    # ログインボタン
    btn_selectors = [
        'input.filled-button[value="ログイン"]',
        'button:has-text("ログイン")',
        'input[type="submit"]',
        'button[type="submit"]',
    ]
    login_btn = None
    for sel in btn_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            login_btn = loc.first
            break
    if not login_btn:
        raise RuntimeError("イタンジBB: ログインボタンが見つかりません")

    try:
        async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
            await login_btn.click()
    except Exception:
        await login_btn.click()

    await page.wait_for_timeout(3000)

    current_url = page.url
    if "itandibb.com" in current_url or "bukkakun.com" in current_url:
        return True
    if "itandi" in current_url.lower():
        return True
    return False


async def _ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    current_url = page.url
    if "itandibb.com" in current_url or "bukkakun.com" in current_url:
        # ログインページでなければOK
        if "login" not in current_url.lower() and "itandi-accounts" not in current_url.lower():
            return True
    return await _login(page)


def _detect_status(text: str) -> str:
    """ページテキストから募集ステータスを判定"""
    for keyword, status in STATUS_KEYWORDS.items():
        if keyword in text:
            return status
    return ""


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """イタンジBBで物件の空室状況を確認

    Args:
        property_name: 物件名
        room_number: 号室（オプション）

    Returns:
        '募集中' / '申込あり' / '募集終了' / '該当なし'
    """
    page = await get_page("itanji")
    await _ensure_logged_in(page)

    # 物件検索ページに移動
    search_keyword = property_name
    if room_number:
        search_keyword = f"{property_name} {room_number}"

    # 検索バーに入力して検索
    # イタンジBBの検索UIに依存
    search_selectors = [
        'input[placeholder*="検索"]',
        'input[placeholder*="物件名"]',
        'input[name*="keyword"]',
        'input[name*="search"]',
        'input[type="search"]',
    ]

    search_input = None
    for sel in search_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            search_input = loc.first
            break

    if not search_input:
        # 検索ページへ直接遷移
        await page.goto("https://bukkakun.com/properties", wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        for sel in search_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                search_input = loc.first
                break

    if not search_input:
        raise RuntimeError("イタンジBB: 検索欄が見つかりません")

    await search_input.fill(search_keyword)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(3000)

    # 検索結果からステータスを判定
    body_text = await page.inner_text("body")

    # 検索結果なし
    if "該当する物件" in body_text and "ありません" in body_text:
        return "該当なし"
    if "0件" in body_text:
        return "該当なし"

    # ステータスキーワード判定
    status = _detect_status(body_text)
    if status:
        return status

    # 結果があるがステータス不明 → 詳細を確認
    # 最初の物件リンクをクリック
    property_links = page.locator("a[href*='property']")
    if await property_links.count() > 0:
        await property_links.first.click()
        await page.wait_for_timeout(2000)
        detail_text = await page.inner_text("body")
        status = _detect_status(detail_text)
        if status:
            return status

    return "該当なし"
