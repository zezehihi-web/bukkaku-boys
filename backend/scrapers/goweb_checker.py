"""GoWeb 空室確認チェッカー

GoWebプラットフォーム（goweb.work / 100kadou.net 系）での空室確認。
管理会社ごとにサブドメイン+認証情報が異なるマルチテナント構成。
物件名でGET検索し、結果一覧のテキストから募集状況を判定する。

判定ロジック: 該当部屋の行に「募集中」→募集中、「申込中」「申込完了」→申込あり。
"""

import urllib.parse
from playwright.async_api import Page

from backend.config import GOWEB_LOGIN_URL, GOWEB_USER_ID, GOWEB_PASSWORD
from backend.credentials_map import get_credentials
from backend.scrapers.browser_manager import get_page


# ログイン済みドメインのキャッシュ（credential_key → True）
_logged_in_domains: dict[str, bool] = {}

# デフォルト認証情報（後方互換用 — credential_key なしで呼ばれた場合）
_DEFAULT_KEY = "__goweb_default__"


def _derive_search_url(login_url: str) -> str:
    """ログインURLからベースURLを導出し、検索URLを返す"""
    if "/accounts/login" in login_url:
        base = login_url.rsplit("/accounts/login", 1)[0]
    elif "/rentals/" in login_url:
        base = login_url.rsplit("/rentals/", 1)[0]
    else:
        base = login_url.rstrip("/")
    return f"{base}/rentals/search"


def _get_login_url(login_url: str) -> str:
    """URLにログインパスが含まれていなければ付与"""
    if "/accounts/login" in login_url:
        return login_url
    return login_url.rstrip("/") + "/accounts/login"


def _resolve_credentials(credential_key: str) -> tuple[str, str, str]:
    """credential_key から (login_url, user_id, password) を解決

    credential_key が空の場合は既存の GOWEB_* 環境変数を使用（後方互換）。
    """
    if not credential_key:
        if not GOWEB_USER_ID or not GOWEB_PASSWORD:
            raise ValueError("GOWEB_USER_ID/GOWEB_PASSWORD が未設定です")
        return (GOWEB_LOGIN_URL, GOWEB_USER_ID, GOWEB_PASSWORD)

    url, user_id, password = get_credentials(credential_key)
    return (_get_login_url(url), user_id, password)


async def login(page: Page, credential_key: str = "") -> bool:
    """GoWebにログイン

    Args:
        page: Playwrightページ
        credential_key: 環境変数プレフィックス（例: "AMBITION"）。空ならデフォルト。
    """
    login_url, user_id, password = _resolve_credentials(credential_key)

    await page.goto(login_url, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    # ユーザーID入力
    user_input = page.locator("#AccountLoginid")
    if await user_input.count() == 0:
        raise RuntimeError(f"GoWeb ({credential_key or 'default'}): ユーザーID入力欄が見つかりません")
    await user_input.fill(user_id)
    await page.wait_for_timeout(300)

    # パスワード入力
    pw_input = page.locator("#AccountPassword")
    if await pw_input.count() == 0:
        raise RuntimeError(f"GoWeb ({credential_key or 'default'}): パスワード入力欄が見つかりません")
    await pw_input.fill(password)
    await page.wait_for_timeout(300)

    # ログインボタンクリック
    submit_btn = page.locator('input[type="submit"]')
    if await submit_btn.count() == 0:
        submit_btn = page.locator('button:has-text("ログイン")')
    if await submit_btn.count() == 0:
        raise RuntimeError(f"GoWeb ({credential_key or 'default'}): ログインボタンが見つかりません")

    try:
        async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
            await submit_btn.first.click()
    except Exception:
        await submit_btn.first.click()

    await page.wait_for_timeout(3000)

    logged_in = await is_logged_in(page)
    if logged_in:
        key = credential_key or _DEFAULT_KEY
        _logged_in_domains[key] = True
    return logged_in


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認（URLに /accounts/login が含まれなければOK）"""
    url = page.url
    if "/accounts/login" in url:
        return False
    # GoWebドメインにいること
    if "goweb" in url or "100kadou.net" in url or "/rentals/" in url:
        return True
    return False


async def ensure_logged_in(page: Page, credential_key: str = "") -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    key = credential_key or _DEFAULT_KEY
    if key in _logged_in_domains:
        if await is_logged_in(page):
            return True
        _logged_in_domains.pop(key, None)

    return await login(page, credential_key)


async def check_vacancy(property_name: str, room_number: str = "", credential_key: str = "") -> str:
    """GoWebで物件の空室状況を確認

    物件名で検索し、結果一覧から募集状況を判定する。

    Args:
        property_name: 物件名（建物名）
        room_number: 号室番号
        credential_key: 環境変数プレフィックス（例: "AMBITION"）。空ならデフォルト。

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    page = await get_page("goweb")
    await ensure_logged_in(page, credential_key)

    # 検索URL構築
    login_url, _, _ = _resolve_credentials(credential_key)
    search_url_base = _derive_search_url(login_url)
    params = urllib.parse.urlencode({"building_name": property_name})
    search_url = f"{search_url_base}?{params}"

    await page.goto(search_url, wait_until="load", timeout=30000)
    await page.wait_for_timeout(4000)

    # 結果テキスト取得
    result_box = page.locator("div.result_box")
    if await result_box.count() == 0:
        body_text = await page.inner_text("body")
    else:
        body_text = await result_box.first.inner_text()

    # 該当なし判定
    if "該当するデータがありません" in body_text:
        return "該当なし"

    # 部屋番号指定がある場合、その部屋の行を確認
    if room_number:
        # 部屋番号が結果内に含まれるか
        if room_number not in body_text:
            return "該当なし"

        # 部屋番号の前後テキストで募集状況を確認
        status = await page.evaluate("""(roomNum) => {
            const box = document.querySelector('.result_box');
            const text = box ? box.innerText : document.body.innerText;
            const idx = text.indexOf(roomNum);
            if (idx === -1) return 'not_found';
            // 部屋番号の後ろ500文字を確認（申込状況は部屋名より後に表示される）
            const context = text.substring(idx, idx + 500);
            if (context.includes('申込完了') || context.includes('申込中')) return 'applied';
            if (context.includes('募集中')) return 'available';
            return 'not_found';
        }""", room_number)

        if status == "available":
            return "募集中"
        elif status == "applied":
            return "申込あり"
        return "該当なし"

    # 部屋番号なし: 結果に「募集中」があれば募集中
    if "募集中" in body_text:
        return "募集中"
    if "申込中" in body_text or "申込完了" in body_text:
        return "申込あり"

    return "該当なし"
