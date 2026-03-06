"""いい生活B2B (es-b2b.com) 空室確認チェッカー

管理会社ごとにサブドメインが異なるマルチテナント構成。
SAML SSO (auth.es-account.com) による認証フローに対応。
既存 es_square_checker の Auth0処理パターンを参考にフレーム横断探索を行う。

判定ロジック: 建物名検索→結果テキストから「申込あり」等のステータス判定。
具体的なセレクタは偵察スクリプト (recon_es_b2b.py) の結果で微調整する。
"""

import urllib.parse
from playwright.async_api import Page

from backend.credentials_map import get_credentials
from backend.scrapers.browser_manager import get_page


# ログイン済みサブドメインのキャッシュ
_logged_in_domains: dict[str, bool] = {}


async def _find_in_any_frame(page: Page, selectors: list[str]):
    """フレーム横断でロケータを探す（SAML SSO対応）

    auth.es-account.com のログインフォームがiframe内にある場合に対応。
    """
    for frame in page.frames:
        for sel in selectors:
            try:
                loc = frame.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    return loc.first
            except Exception:
                continue
    # フォールバック: メインページ
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                return loc.first
        except Exception:
            continue
    return None


async def _submit_auth_form(page: Page, login_id: str, password: str) -> bool:
    """SAML SSO認証フォームにID/パスワードを入力して送信"""
    id_input = await _find_in_any_frame(page, [
        "#IDArea", "input#username", "input[name='username']",
        "input[name='loginId']", "input[name='login_id']",
        "input[type='email']", "input[name='email']",
    ])
    pw_input = await _find_in_any_frame(page, [
        "#PasswordArea", "input#password", "input[name='password']",
        "input[type='password']",
    ])

    if not id_input or not pw_input:
        return False

    await id_input.fill(login_id)
    await pw_input.fill(password)

    # 送信
    try:
        await pw_input.press("Enter")
    except Exception:
        submit = await _find_in_any_frame(page, [
            "button[type='submit']",
            "button:has-text('ログイン')",
            "button:has-text('サインイン')",
            "button:has-text('次へ')",
            "input[type='submit']",
        ])
        if submit:
            await submit.click()
        else:
            return False

    return True


async def login(page: Page, credential_key: str) -> bool:
    """いい生活B2Bにログイン

    Args:
        page: Playwrightページ
        credential_key: 環境変数プレフィックス（例: "TFD"）
    """
    url, login_id, password = get_credentials(credential_key)

    # サブドメインのトップへアクセス → SAML SSOにリダイレクトされる想定
    await page.goto(url.rstrip("/"), wait_until="load", timeout=90000)
    await page.wait_for_timeout(2000)

    # SAML SSO認証フォームが直接表示されるパターン
    if await _submit_auth_form(page, login_id, password):
        await page.wait_for_timeout(5000)
    else:
        # 「いい生活アカウントでログイン」ボタンが先に表示されるパターン
        sso_btn_selectors = [
            "button:has-text('いい生活アカウントでログイン')",
            "a:has-text('いい生活アカウントでログイン')",
            "button.MuiButton-contained",
            "button:has-text('ログイン')",
            "a:has-text('ログイン')",
        ]
        sso_btn = None
        for sel in sso_btn_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                sso_btn = loc.first
                break

        if sso_btn:
            await sso_btn.click()
            await page.wait_for_timeout(3000)

            if not await _submit_auth_form(page, login_id, password):
                raise RuntimeError(f"いい生活B2B ({credential_key}): SSO認証フォームが見つかりません")
            await page.wait_for_timeout(5000)
        else:
            raise RuntimeError(f"いい生活B2B ({credential_key}): ログインボタンが見つかりません")

    # SSOリダイレクト完了を待機
    try:
        await page.wait_for_url("**es-b2b.com**", timeout=30000)
    except Exception:
        pass

    # es-b2b.comドメインに戻っていない場合、手動で遷移
    if "es-b2b.com" not in page.url:
        try:
            await page.goto(url.rstrip("/"), wait_until="load", timeout=30000)
        except Exception:
            pass

    logged_in = await is_logged_in(page)
    if logged_in:
        _logged_in_domains[credential_key] = True
    return logged_in


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認"""
    url = page.url
    # es-b2b.comドメインにいて、ログインページでなければOK
    if "es-b2b.com" in url and "login" not in url and "auth" not in url:
        return True
    # 認証中間ページにいる可能性
    if "auth.es-account.com" in url:
        return False
    return False


async def ensure_logged_in(page: Page, credential_key: str) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    if credential_key in _logged_in_domains:
        if await is_logged_in(page):
            return True
        _logged_in_domains.pop(credential_key, None)

    return await login(page, credential_key)


async def check_vacancy(property_name: str, room_number: str = "", credential_key: str = "") -> str:
    """いい生活B2Bで物件の空室状況を確認

    Args:
        property_name: 物件名（建物名）
        room_number: 号室番号
        credential_key: 環境変数プレフィックス（例: "TFD"）

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    if not credential_key:
        raise ValueError("credential_key が指定されていません")

    page = await get_page("es_b2b")
    await ensure_logged_in(page, credential_key)
    await page.wait_for_timeout(1000)

    # 建物名で検索
    # いい生活B2Bの検索UI: es_squareと類似のtatemono_nameパラメータを想定
    base_url, _, _ = get_credentials(credential_key)
    base_url = base_url.rstrip("/")

    # 検索URLパターン（偵察結果で確定させる）
    params = urllib.parse.urlencode({
        "tatemono_name": property_name,
    })
    search_url = f"{base_url}/search?{params}"
    await page.goto(search_url, wait_until="load", timeout=30000)
    await page.wait_for_timeout(4000)

    # フォーム送信方式のフォールバック
    building_input = None
    for sel in ["input[name*='tatemono']", "input[name*='building']",
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
    if "0件" in body_text or "0 件" in body_text or "見つかりません" in body_text:
        return "該当なし"

    # 部屋番号指定がある場合
    if room_number:
        if room_number not in body_text:
            return "該当なし"

        # 部屋番号の周辺で申込ステータスを確認
        has_application = await page.evaluate("""(args) => {
            const [propName, roomNum] = args;
            const body = document.body.innerText;
            const idx = body.indexOf(roomNum);
            if (idx === -1) return false;
            const context = body.substring(Math.max(0, idx - 200), idx + 200);
            return context.includes('申込あり') || context.includes('申込中')
                || context.includes('成約') || context.includes('契約済');
        }""", [property_name, room_number])

        return "申込あり" if has_application else "募集中"

    # 部屋番号なし
    if property_name not in body_text:
        return "該当なし"

    if "申込あり" in body_text or "申込中" in body_text:
        return "申込あり"
    return "募集中"
