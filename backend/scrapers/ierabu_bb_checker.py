"""いえらぶBB 空室確認チェッカー

bb.ielove.jp の検索メニューで物件名検索。
判定ロジック: 検索結果に物件があり、「申込あり」「申込中」がなければ募集中。
"""

import re
from playwright.async_api import Page

from backend.config import IERABU_BB_LOGIN_URL, IERABU_BB_ID, IERABU_BB_PASSWORD
from backend.scrapers.browser_manager import get_page

SEARCH_MENU_URL = "https://bb.ielove.jp/ielovebb/rent/searchmenu/"


async def login(page: Page) -> bool:
    """いえらぶBBにログイン"""
    if not IERABU_BB_ID or not IERABU_BB_PASSWORD:
        raise ValueError("IERABU_EMAIL/IERABU_PASSWORD が未設定です")

    await page.goto(IERABU_BB_LOGIN_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    # ID入力
    id_input = None
    for sel in ['input[name="login_id"]', 'input#login_id', 'input[type="text"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            id_input = loc.first
            break
    if not id_input:
        raise RuntimeError("いえらぶBB: ID入力欄が見つかりません")

    await id_input.fill(IERABU_BB_ID)

    # パスワード入力
    pw_input = None
    for sel in ['input[name="login_pw"]', 'input#login_pw', 'input[type="password"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            pw_input = loc.first
            break
    if not pw_input:
        raise RuntimeError("いえらぶBB: パスワード入力欄が見つかりません")

    await pw_input.fill(IERABU_BB_PASSWORD)

    # ログインボタン
    submit = None
    for sel in ['input[type="submit"]', 'button[type="submit"]', '.login-btn']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            submit = loc.first
            break
    if not submit:
        raise RuntimeError("いえらぶBB: ログインボタンが見つかりません")

    await submit.click()
    await page.wait_for_timeout(3000)

    return "bb.ielove.jp" in page.url and "login" not in page.url


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認"""
    return "bb.ielove.jp" in page.url and "login" not in page.url


async def ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    if await is_logged_in(page):
        return True
    return await login(page)


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """いえらぶBBで物件の空室状況を確認

    検索メニューの物件名フィールド(renm)で検索し、結果を判定。

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    page = await get_page("ierabu_bb")
    await ensure_logged_in(page)

    # 検索メニューへ
    await page.goto(SEARCH_MENU_URL, wait_until="load", timeout=30000)
    await page.wait_for_timeout(2000)

    # 物件名入力
    name_input = page.locator('input#renm')
    if await name_input.count() == 0:
        raise RuntimeError("いえらぶBB: 物件名入力欄(renm)が見つかりません")

    await name_input.fill(property_name)
    await page.wait_for_timeout(500)

    # 検索実行
    submit = page.locator('input[type="submit"], button[type="submit"]').first
    await submit.click()
    await page.wait_for_timeout(4000)

    body_text = await page.inner_text("body")

    # 0件判定: 「物件が見つかりませんでした。」 or 「0件」 or bkn_detailリンクなし
    if "物件が見つかりませんでした" in body_text:
        return "該当なし"

    bkn_count = await page.locator("a.bkn_detail").count()
    if bkn_count == 0:
        # 件数テキストでも確認
        count_match = re.search(r'全(\d+)件', body_text)
        if count_match and int(count_match.group(1)) == 0:
            return "該当なし"
        if "0件" in body_text:
            return "該当なし"
        # bkn_detailリンクもなく件数表示もない → 該当なし
        return "該当なし"

    # ヒットあり → 部屋番号で絞り込み
    if room_number:
        if room_number in body_text:
            # 部屋番号の前後コンテキストで申込チェック
            idx = body_text.find(room_number)
            context = body_text[max(0, idx - 300):idx + 300]
            if "申込あり" in context or "申込中" in context or "申込済" in context:
                return "申込あり"
            return "募集中"
        else:
            return "該当なし"

    # 部屋番号なし: ページ全体で判定
    if any(kw in body_text for kw in ["申込あり", "申込中", "申込済"]):
        return "申込あり"
    return "募集中"
