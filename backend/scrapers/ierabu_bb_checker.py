"""いえらぶBB 空室確認チェッカー

bb.ielove.jp の検索メニューで物件名検索。
判定ロジック: 検索結果に物件があり、「申込あり」「申込中」がなければ募集中。
"""

import re
import logging
from playwright.async_api import Page

from backend.config import IERABU_BB_LOGIN_URL, IERABU_BB_ID, IERABU_BB_PASSWORD
from backend.scrapers.browser_manager import get_page

logger = logging.getLogger(__name__)

SEARCH_MENU_URL = "https://bb.ielove.jp/ielovebb/rent/searchmenu/"


async def login(page: Page) -> bool:
    """いえらぶBBにログイン"""
    if not IERABU_BB_ID or not IERABU_BB_PASSWORD:
        raise ValueError("IERABU_EMAIL/IERABU_PASSWORD が未設定です")

    logger.warning(f"いえらぶBB: ログイン開始 URL={IERABU_BB_LOGIN_URL}")

    # ヘッドレス検出回避のためステルスJSを注入
    await page.add_init_script("""
        // webdriver プロパティを隠す
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        // Chrome ランタイムを偽装
        window.chrome = { runtime: {} };
        // plugins を偽装
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        // languages を偽装
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ja-JP', 'ja', 'en-US', 'en'],
        });
    """)

    await page.goto(IERABU_BB_LOGIN_URL, wait_until="load", timeout=60000)

    # ページが完全に描画されるまで待機（networkidle + 追加待機）
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass  # タイムアウトしても続行
    await page.wait_for_timeout(3000)

    # 現在のURL・ページ構造をログ出力
    current_url = page.url
    logger.info(f"いえらぶBB: ログインページ到達 URL={current_url}")
    page_title = await page.title()
    logger.info(f"いえらぶBB: ページタイトル={page_title}")

    # ID入力（幅広いセレクタで探索）
    id_input = None
    id_selectors = [
        'input[name="login_id"]',
        'input#login_id',
        'input[name="email"]',
        'input[name="user_id"]',
        'input[name="id"]',
        'input[type="email"]',
        'input[type="text"]:not([name=""])',
        'input[type="text"]',
    ]
    for sel in id_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            # 可視性チェック
            first = loc.first
            try:
                if await first.is_visible():
                    id_input = first
                    logger.info(f"いえらぶBB: ID入力欄発見 selector={sel}")
                    break
            except Exception:
                id_input = first
                logger.info(f"いえらぶBB: ID入力欄発見(可視性不明) selector={sel}")
                break
    if not id_input:
        # デバッグ: ページ上の全input要素を列挙
        inputs = await page.query_selector_all("input")
        input_info = []
        for inp in inputs:
            attrs = await inp.evaluate("""el => ({
                type: el.type, name: el.name, id: el.id,
                placeholder: el.placeholder, visible: el.offsetParent !== null
            })""")
            input_info.append(attrs)
        logger.error(f"いえらぶBB: ID入力欄なし ページ上のinput要素={input_info}")
        raise RuntimeError(
            f"いえらぶBB: ID入力欄が見つかりません "
            f"(URL={current_url}, title={page_title}, inputs={len(inputs)})"
        )

    await id_input.fill(IERABU_BB_ID)

    # パスワード入力
    pw_input = None
    pw_selectors = [
        'input[name="login_pw"]',
        'input#login_pw',
        'input[name="password"]',
        'input[name="passwd"]',
        'input[type="password"]',
    ]
    for sel in pw_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            pw_input = loc.first
            logger.info(f"いえらぶBB: パスワード入力欄発見 selector={sel}")
            break
    if not pw_input:
        raise RuntimeError("いえらぶBB: パスワード入力欄が見つかりません")

    await pw_input.fill(IERABU_BB_PASSWORD)

    # ログインボタン
    submit = None
    submit_selectors = [
        'input[type="submit"]',
        'button[type="submit"]',
        '.login-btn',
        'button:has-text("ログイン")',
        'input[value="ログイン"]',
    ]
    for sel in submit_selectors:
        loc = page.locator(sel)
        if await loc.count() > 0:
            submit = loc.first
            logger.info(f"いえらぶBB: ログインボタン発見 selector={sel}")
            break
    if not submit:
        raise RuntimeError("いえらぶBB: ログインボタンが見つかりません")

    await submit.click()

    # ログイン後のページ遷移待機
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    await page.wait_for_timeout(3000)

    success = "bb.ielove.jp" in page.url and "login" not in page.url
    logger.info(f"いえらぶBB: ログイン{'成功' if success else '失敗'} URL={page.url}")
    return success


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
