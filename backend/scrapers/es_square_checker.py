"""いい生活スクエア 空室確認チェッカー

既存のscrape_es_square.pyのlogin()ロジックを再利用。
Auth0認証経由でログインし、物件名検索 → ステータス判定。
"""

from playwright.async_api import Page

from backend.config import ES_SQUARE_EMAIL, ES_SQUARE_PASSWORD, ES_SQUARE_LOGIN_URL
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
    "取り下げ": "募集終了",
    "掲載終了": "募集終了",
}


async def _find_in_any_frame(page: Page, selectors: list[str]):
    """フレーム横断でロケータを探す（Auth0対応）"""
    for frame in page.frames:
        for sel in selectors:
            try:
                loc = frame.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    return loc.first
            except Exception:
                continue
    # フォールバック: メインページのみ
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                return loc.first
        except Exception:
            continue
    return None


async def _submit_auth_form(page: Page) -> bool:
    """Auth0フォームにメール/パスワードを入力して送信"""
    email_input = await _find_in_any_frame(page, [
        "input#username",
        "input[name='username']",
        'input[type="email"]',
        'input[name*="email"]',
    ])
    password_input = await _find_in_any_frame(page, [
        "input#password",
        "input[name='password']",
        'input[type="password"]',
    ])
    if not email_input or not password_input:
        return False

    await email_input.fill(ES_SQUARE_EMAIL)
    await password_input.fill(ES_SQUARE_PASSWORD)

    try:
        await password_input.press("Enter")
    except Exception:
        submit = await _find_in_any_frame(page, [
            'button:has-text("続ける")',
            "button[name='action'][value='default']",
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("ログイン")',
        ])
        if submit:
            await submit.click()
        else:
            return False
    return True


async def _login(page: Page) -> bool:
    """いい生活スクエアにログイン（scrape_es_square.py login()準拠）"""
    if not ES_SQUARE_EMAIL or not ES_SQUARE_PASSWORD:
        raise ValueError("ES_SQUARE_EMAIL/ES_SQUARE_PASSWORD が未設定です")

    await page.goto(ES_SQUARE_LOGIN_URL, wait_until="load", timeout=90000)
    await page.wait_for_timeout(1200)

    # Auth0フォームが表示されていればそのまま送信
    if not await _submit_auth_form(page):
        # 「いい生活アカウントでログイン」ボタンを探す
        btn_selectors = [
            'button:has-text("いい生活アカウントでログイン")',
            "button.css-rk6wt",
            "div.css-4rmlxi button",
            "button.MuiButton-contained",
        ]
        login_btn = None
        for sel in btn_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                login_btn = loc.first
                break
        if not login_btn:
            raise RuntimeError("いい生活スクエア: ログインボタンが見つかりません")

        await login_btn.click()
        await page.wait_for_timeout(1200)

        if not await _submit_auth_form(page):
            raise RuntimeError("いい生活スクエア: ログイン入力欄が見つかりません")

    # ログイン完了待ち
    try:
        await page.wait_for_url("**/rent.es-square.net/**", timeout=40000)
    except Exception:
        pass

    if "rent.es-square.net" not in page.url:
        try:
            await page.goto(
                "https://rent.es-square.net/bukken/chintai/search?p=1&items_per_page=10",
                wait_until="load",
                timeout=30000,
            )
        except Exception:
            pass

    if "rent.es-square.net" in page.url and "/bukken/" in page.url:
        return True

    # リトライ
    await _submit_auth_form(page)
    await page.wait_for_timeout(5000)
    return "rent.es-square.net" in page.url


async def _ensure_logged_in(page: Page) -> bool:
    """ログイン状態確認"""
    if "rent.es-square.net" in page.url and "login" not in page.url:
        return True
    return await _login(page)


def _detect_status(text: str) -> str:
    """テキストからステータス判定"""
    for keyword, status in STATUS_KEYWORDS.items():
        if keyword in text:
            return status
    return ""


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """いい生活スクエアで物件の空室状況を確認

    Returns:
        '募集中' / '申込あり' / '募集終了' / '該当なし'
    """
    page = await get_page("es_square")
    await _ensure_logged_in(page)

    search_keyword = property_name
    if room_number:
        search_keyword = f"{property_name} {room_number}"

    # 物件検索ページへ
    search_url = f"https://rent.es-square.net/bukken/chintai/search?p=1&items_per_page=10"
    await page.goto(search_url, wait_until="load", timeout=30000)
    await page.wait_for_timeout(2000)

    # フリーワード検索
    search_selectors = [
        'input[placeholder*="検索"]',
        'input[placeholder*="物件名"]',
        'input[placeholder*="フリーワード"]',
        'input[name*="keyword"]',
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
        raise RuntimeError("いい生活スクエア: 検索欄が見つかりません")

    await search_input.fill(search_keyword)

    # 検索ボタンをクリック
    search_btn = None
    for sel in ['button:has-text("検索")', 'input[value="検索"]', 'button[type="submit"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            search_btn = loc.first
            break

    if search_btn:
        await search_btn.click()
    else:
        await page.keyboard.press("Enter")

    await page.wait_for_timeout(3000)

    body_text = await page.inner_text("body")

    if "該当する物件" in body_text and "ありません" in body_text:
        return "該当なし"
    if "0件" in body_text:
        return "該当なし"

    # 「申込あり」は募集終了扱い（既存ロジック準拠）
    status = _detect_status(body_text)
    if status:
        return status

    # 物件詳細をクリックして確認
    property_links = page.locator("a[href*='bukken']")
    if await property_links.count() > 0:
        await property_links.first.click()
        await page.wait_for_timeout(2000)
        detail_text = await page.inner_text("body")
        status = _detect_status(detail_text)
        if status:
            return status

    return "該当なし"
