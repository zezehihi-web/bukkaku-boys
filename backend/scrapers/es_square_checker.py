"""いい生活スクエア 空室確認チェッカー

物件名URLパラメータ(tatemono_name)で直接検索。
判定ロジック: 該当物件の行に「申込あり」があれば申込あり、なければ募集中。
"""

import urllib.parse
from playwright.async_api import Page

from backend.config import ES_SQUARE_EMAIL, ES_SQUARE_PASSWORD, ES_SQUARE_LOGIN_URL
from backend.scrapers.browser_manager import get_page

SEARCH_BASE = "https://rent.es-square.net/bukken/chintai/search"


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
        "input#username", "input[name='username']",
        'input[type="email"]', 'input[name*="email"]',
    ])
    password_input = await _find_in_any_frame(page, [
        "input#password", "input[name='password']", 'input[type="password"]',
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
            'button:has-text("ログイン")',
        ])
        if submit:
            await submit.click()
        else:
            return False
    return True


async def login(page: Page) -> bool:
    """いい生活スクエアにログイン（外部から呼び出し可能）"""
    if not ES_SQUARE_EMAIL or not ES_SQUARE_PASSWORD:
        raise ValueError("ES_SQUARE_EMAIL/ES_SQUARE_PASSWORD が未設定です")

    await page.goto(ES_SQUARE_LOGIN_URL, wait_until="load", timeout=90000)
    await page.wait_for_timeout(1200)

    if not await _submit_auth_form(page):
        btn_selectors = [
            'button:has-text("いい生活アカウントでログイン")',
            "button.css-rk6wt", "div.css-4rmlxi button",
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

    try:
        await page.wait_for_url("**/rent.es-square.net/**", timeout=40000)
    except Exception:
        pass

    if "rent.es-square.net" not in page.url:
        try:
            await page.goto(SEARCH_BASE, wait_until="load", timeout=30000)
        except Exception:
            pass

    if "rent.es-square.net" in page.url and "login" not in page.url:
        return True

    await _submit_auth_form(page)
    await page.wait_for_timeout(5000)
    return "rent.es-square.net" in page.url


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認"""
    return "rent.es-square.net" in page.url and "login" not in page.url


async def ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    if await is_logged_in(page):
        return True
    return await login(page)


async def check_vacancy(property_name: str, room_number: str = "", address: str = "") -> str:
    """いい生活スクエアで物件の空室状況を確認

    物件名で検索し、該当物件の行に「申込あり」があれば申込あり、なければ募集中。

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    page = await get_page("es_square")
    await ensure_logged_in(page)
    await page.wait_for_timeout(1000)

    # 物件名で直接URL検索
    params = urllib.parse.urlencode({
        "tatemono_name": property_name,
        "order": "saishu_koshin_time.desc",
        "p": "1",
        "items_per_page": "100",
    })
    search_url = f"{SEARCH_BASE}?{params}"

    await page.goto(search_url, wait_until="load", timeout=30000)
    await page.wait_for_timeout(4000)

    # 件数確認: 「N件/ N件」のテキストを取得
    body_text = await page.inner_text("body")

    if "0 件" in body_text or "0件" in body_text:
        return "該当なし"

    # 部屋番号がある場合、その部屋の行を探す
    if room_number:
        # 部屋番号が含まれる行を探して、その行に「申込あり」があるか確認
        has_room = await page.evaluate("""(roomNum) => {
            const body = document.body.innerText;
            return body.includes(roomNum);
        }""", room_number)

        if not has_room:
            return "該当なし"

        # その部屋の行に「申込あり」があるか
        has_application = await page.evaluate("""(args) => {
            const [propName, roomNum] = args;
            // ページ内の全テキストノードを走査して、
            // 部屋番号の近く（同じコンテナ内）に「申込あり」があるか確認
            const images = document.querySelectorAll('img');
            for (const img of images) {
                let container = img.parentElement;
                for (let i = 0; i < 10 && container; i++) {
                    const text = container.innerText || '';
                    if (text.includes(roomNum)) {
                        return text.includes('申込あり') || text.includes('申込中');
                    }
                    container = container.parentElement;
                }
            }
            // フォールバック: body全体で判定
            const body = document.body.innerText;
            if (body.includes(roomNum)) {
                // 部屋番号の前後のテキストを確認
                const idx = body.indexOf(roomNum);
                const context = body.substring(Math.max(0, idx - 200), idx + 200);
                return context.includes('申込あり') || context.includes('申込中');
            }
            return false;
        }""", [property_name, room_number])

        return "申込あり" if has_application else "募集中"

    # 部屋番号なしの場合: 検索結果に物件があるか確認
    has_property = property_name in body_text
    if not has_property:
        return "該当なし"

    # 「申込あり」がなければ募集中
    if "申込あり" in body_text or "申込中" in body_text:
        return "申込あり"
    return "募集中"
