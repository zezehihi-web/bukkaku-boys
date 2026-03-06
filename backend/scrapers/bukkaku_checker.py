"""物確.com (bukkaku.jp) 空室確認チェッカー

管理会社ごとにサブドメインが異なるマルチテナント構成。
credential_key（例: "CIC"）から環境変数を動的ロードしてログイン→検索→判定。

判定ロジック: goweb_checker と同様、建物名検索→結果テキストからステータス判定。
具体的なセレクタは偵察スクリプト (recon_bukkaku.py) の結果で微調整する。
"""

import urllib.parse
from playwright.async_api import Page

from backend.credentials_map import get_credentials
from backend.scrapers.browser_manager import get_page


# ログイン済みサブドメインのキャッシュ（credential_key → True）
_logged_in_domains: dict[str, bool] = {}


async def _get_login_selectors(page: Page) -> dict:
    """ログインフォームのセレクタを動的に検出

    物確.comは統一UIだが、バージョン違いに備えて複数候補を試す。
    """
    id_candidates = [
        "input[name='login_id']", "input[name='loginId']",
        "input[name='user_id']", "input[name='userId']",
        "input[name='username']", "input[name='email']",
        "input#login_id", "input#userId",
        "input[type='text']",
    ]
    pw_candidates = [
        "input[name='password']", "input[name='login_password']",
        "input[type='password']",
    ]
    submit_candidates = [
        "button[type='submit']", "input[type='submit']",
        "button:has-text('ログイン')",
    ]

    selectors = {}
    for sel in id_candidates:
        loc = page.locator(sel)
        if await loc.count() > 0 and await loc.first.is_visible():
            selectors["id"] = sel
            break

    for sel in pw_candidates:
        loc = page.locator(sel)
        if await loc.count() > 0 and await loc.first.is_visible():
            selectors["pw"] = sel
            break

    for sel in submit_candidates:
        loc = page.locator(sel)
        if await loc.count() > 0:
            selectors["submit"] = sel
            break

    return selectors


async def login(page: Page, credential_key: str) -> bool:
    """物確.comにログイン

    Args:
        page: Playwrightページ
        credential_key: 環境変数プレフィックス（例: "CIC"）
    """
    url, login_id, password = get_credentials(credential_key)

    # ログインURLへ遷移（サブドメインのトップ = ログインページ想定）
    login_url = url.rstrip("/")
    await page.goto(login_url, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    selectors = await _get_login_selectors(page)
    if "id" not in selectors or "pw" not in selectors:
        raise RuntimeError(f"物確.com ({credential_key}): ログインフォームが見つかりません")

    # ID入力
    id_input = page.locator(selectors["id"]).first
    await id_input.fill(login_id)
    await page.wait_for_timeout(300)

    # パスワード入力
    pw_input = page.locator(selectors["pw"]).first
    await pw_input.fill(password)
    await page.wait_for_timeout(300)

    # 送信
    if "submit" in selectors:
        submit_btn = page.locator(selectors["submit"]).first
        try:
            async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
                await submit_btn.click()
        except Exception:
            await submit_btn.click()
    else:
        await pw_input.press("Enter")

    await page.wait_for_timeout(3000)

    logged_in = await is_logged_in(page)
    if logged_in:
        _logged_in_domains[credential_key] = True
    return logged_in


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認

    ログインページ特有のセレクタ（パスワード入力欄）が無ければログイン済みと判定。
    """
    url = page.url
    if "/login" in url or "/signin" in url:
        return False

    # パスワード入力欄が見えていたらまだログインページ
    pw_loc = page.locator("input[type='password']")
    if await pw_loc.count() > 0:
        try:
            if await pw_loc.first.is_visible():
                return False
        except Exception:
            pass

    return True


async def ensure_logged_in(page: Page, credential_key: str) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    # 既知のドメインで、ページがまだ生きていればスキップ
    if credential_key in _logged_in_domains:
        if await is_logged_in(page):
            return True
        # セッション切れ
        _logged_in_domains.pop(credential_key, None)

    return await login(page, credential_key)


async def check_vacancy(property_name: str, room_number: str = "", credential_key: str = "") -> str:
    """物確.comで物件の空室状況を確認

    Args:
        property_name: 物件名（建物名）
        room_number: 号室番号
        credential_key: 環境変数プレフィックス（例: "CIC"）

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    if not credential_key:
        raise ValueError("credential_key が指定されていません")

    # 物確.com用のブラウザコンテキスト（サブドメイン間はcookieで自然分離）
    page = await get_page("bukkaku")
    await ensure_logged_in(page, credential_key)

    # 建物名で検索
    # 物確.comの検索UIは偵察結果で確定するが、
    # 一般的なパターンとしてGETパラメータ or フォーム送信を想定
    base_url, _, _ = get_credentials(credential_key)
    base_url = base_url.rstrip("/")

    # 検索URLパターン候補（偵察結果で確定させる）
    # パターン1: GETパラメータ
    search_url = f"{base_url}/search?building_name={urllib.parse.quote(property_name)}"
    await page.goto(search_url, wait_until="load", timeout=30000)
    await page.wait_for_timeout(4000)

    # 検索フォームが表示された場合（GETパラメータ非対応の場合）のフォールバック
    building_input = None
    for sel in ["input[name*='building']", "input[name*='tatemono']",
                "input[name*='name']", "input[placeholder*='建物']",
                "input[placeholder*='物件']"]:
        loc = page.locator(sel)
        if await loc.count() > 0 and await loc.first.is_visible():
            building_input = loc.first
            break

    if building_input:
        await building_input.fill(property_name)
        for sel in ["button[type='submit']", "input[type='submit']",
                    "button:has-text('検索')"]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click()
                break
        await page.wait_for_timeout(4000)

    # 結果テキスト取得
    body_text = await page.inner_text("body")

    # 該当なし判定
    no_result_keywords = ["該当するデータがありません", "0件", "見つかりません", "該当なし"]
    for kw in no_result_keywords:
        if kw in body_text:
            return "該当なし"

    # 部屋番号指定がある場合
    if room_number:
        if room_number not in body_text:
            return "該当なし"

        # 部屋番号の周辺テキストで募集状況を確認
        status = await page.evaluate("""(roomNum) => {
            const text = document.body.innerText;
            const idx = text.indexOf(roomNum);
            if (idx === -1) return 'not_found';
            const context = text.substring(idx, idx + 500);
            if (context.includes('申込') || context.includes('成約') || context.includes('契約済')) return 'applied';
            if (context.includes('募集中') || context.includes('空室')) return 'available';
            return 'unknown';
        }""", room_number)

        if status == "available":
            return "募集中"
        elif status == "applied":
            return "申込あり"
        return "該当なし"

    # 部屋番号なし: 結果全体で判定
    if "募集中" in body_text or "空室" in body_text:
        return "募集中"
    if "申込" in body_text or "成約" in body_text or "契約済" in body_text:
        return "申込あり"

    return "該当なし"
