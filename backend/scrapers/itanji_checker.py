"""イタンジBB 空室確認チェッカー

itandibb.com/rent_rooms/list で物件名検索。
判定ロジック: 該当部屋の行に「申込あり」があれば申込あり、なければ募集中。
"""

import re
from playwright.async_api import Page

from backend.config import ITANJI_EMAIL, ITANJI_PASSWORD, ITANJI_TOP_URL
from backend.scrapers.browser_manager import get_page

LIST_SEARCH_URL = "https://itandibb.com/rent_rooms/list"


async def login(page: Page) -> bool:
    """イタンジBBにログイン（外部から呼び出し可能）"""
    if not ITANJI_EMAIL or not ITANJI_PASSWORD:
        raise ValueError("ITANJI_EMAIL/ITANJI_PASSWORD が未設定です")

    await page.goto(ITANJI_TOP_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

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

    # 400エラーチェック
    title = await page.title()
    body_text = await page.inner_text("body")
    if "400" in title or "Bad Request" in body_text:
        await page.goto(ITANJI_TOP_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        for sel in login_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                login_link = loc.first
                break
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
        if "login" not in current_url.lower() and "itandi-accounts" not in current_url.lower():
            return True
    return False


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認"""
    current_url = page.url
    if "itandibb.com" in current_url:
        if "login" not in current_url.lower() and "itandi-accounts" not in current_url.lower():
            return True
    return False


async def ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    if await is_logged_in(page):
        return True
    return await login(page)


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """イタンジBBで物件の空室状況を確認

    物件名で検索し、該当部屋の行に「申込あり」があれば申込あり、なければ募集中。

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    page = await get_page("itanji")
    await ensure_logged_in(page)

    await page.goto(LIST_SEARCH_URL, wait_until="load", timeout=30000)
    await page.wait_for_timeout(3000)

    # 物件名フィールドに入力
    name_input = page.locator('input[name="building_name:match"]')
    if await name_input.count() == 0:
        raise RuntimeError("イタンジBB: 物件名検索フィールドが見つかりません")

    await name_input.fill(property_name)
    await page.wait_for_timeout(500)

    # 部屋番号があれば入力
    if room_number:
        room_input = page.locator('input[name="room_number:match"]')
        if await room_input.count() > 0:
            await room_input.fill(room_number)
            await page.wait_for_timeout(300)

    # 検索ボタンをクリック
    btn_selectors = [
        'button.ListSearchButton[type="submit"]',
        'button:has-text("検索")[type="submit"]',
        'button:has-text("検索")',
    ]
    search_btn = None
    for sel in btn_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            search_btn = loc.first
            break

    if not search_btn:
        raise RuntimeError("イタンジBB: 検索ボタンが見つかりません")

    await search_btn.click()
    await page.wait_for_timeout(5000)

    # 結果件数を確認（"N 戸" パターン）
    body_text = await page.inner_text("body")
    hit_match = re.search(r"(\d+)\s*戸", body_text)

    if hit_match and int(hit_match.group(1)) == 0:
        return "該当なし"

    if not hit_match:
        return "該当なし"

    # ヒットあり → 「申込あり」がなければ募集中
    # 部屋番号指定がある場合、その行だけ確認
    if room_number:
        # 部屋番号の前後コンテキストで判定
        has_application = await page.evaluate("""(roomNum) => {
            const body = document.body.innerText;
            const idx = body.indexOf(roomNum);
            if (idx === -1) return false;
            // 部屋番号の前後300文字を確認
            const context = body.substring(Math.max(0, idx - 300), idx + 300);
            return context.includes('申込あり');
        }""", room_number)

        return "申込あり" if has_application else "募集中"

    # 部屋番号なし: ページ全体で判定
    if "申込あり" in body_text:
        return "申込あり"
    return "募集中"
