"""いい生活B2B (es-b2b.com) 偵察スクリプト

TFDなどのサブドメインにログインし、SAML SSO認証フロー・検索UI・結果ページ構造を特定する。
使い捨て: 本番には組み込まない。

使い方:
  python -m backend.scrapers.recon_es_b2b

  環境変数:
    TFD_URL=https://tfd.es-b2b.com
    TFD_ID=your_login_id
    TFD_PASS=your_password
"""

import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright


SCREENSHOT_DIR = Path("results/recon_es_b2b")


async def recon():
    url = os.getenv("TFD_URL", "https://tfd.es-b2b.com")
    login_id = os.getenv("TFD_ID", "")
    password = os.getenv("TFD_PASS", "")

    if not login_id or not password:
        print("ERROR: TFD_ID / TFD_PASS 環境変数が未設定です")
        return

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = await ctx.new_page()

        # ---- Step 1: ログインページにアクセス ----
        print(f"[RECON] ログインページへ: {url}")
        await page.goto(url, wait_until="load", timeout=60000)
        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(SCREENSHOT_DIR / "01_login_page.png"), full_page=True)
        print(f"  初期URL: {page.url}")

        # SAML SSOリダイレクト先を確認
        if "auth.es-account.com" in page.url or "es-account" in page.url:
            print("  → SAML SSO (auth.es-account.com) にリダイレクトされました")
        elif "login" in page.url.lower():
            print("  → ログインページに到達")

        # ---- Step 2: ログインフォーム解析 ----
        print("\n[RECON] === ログインフォーム解析 ===")

        # メインページとiframe両方を確認
        for frame_idx, frame in enumerate(page.frames):
            inputs = await frame.query_selector_all("input")
            if inputs:
                print(f"  Frame[{frame_idx}] URL: {frame.url[:80]}")
            for inp in inputs:
                inp_type = await inp.get_attribute("type") or ""
                inp_name = await inp.get_attribute("name") or ""
                inp_id = await inp.get_attribute("id") or ""
                inp_placeholder = await inp.get_attribute("placeholder") or ""
                if inp_type not in ("hidden",):
                    print(f"    input: type={inp_type}, name={inp_name}, id={inp_id}, placeholder={inp_placeholder}")

            buttons = await frame.query_selector_all("button, input[type='submit']")
            for btn in buttons:
                tag = await btn.evaluate("el => el.tagName")
                text = await btn.inner_text() if tag == "BUTTON" else (await btn.get_attribute("value") or "")
                btn_id = await btn.get_attribute("id") or ""
                print(f"    button: tag={tag}, id={btn_id}, text={text.strip()[:30]}")

        # ---- Step 3: ログイン実行 ----
        print(f"\n[RECON] ログイン試行中...")

        # いい生活アカウント系のセレクタ（既存es_square_checkerを参考）
        id_selectors = [
            "#IDArea", "input#username", "input[name='username']",
            "input[name='loginId']", "input[name='login_id']",
            "input[type='email']", "input[name='email']",
            "input[type='text']:not([name=''])",
        ]
        pw_selectors = [
            "#PasswordArea", "input#password", "input[name='password']",
            "input[type='password']",
        ]

        # フレーム横断で探索
        id_input = None
        pw_input = None
        target_frame = None

        for frame in page.frames:
            for sel in id_selectors:
                try:
                    loc = frame.locator(sel)
                    if await loc.count() > 0 and await loc.first.is_visible():
                        id_input = loc.first
                        target_frame = frame
                        print(f"  ID入力欄: {sel} (frame: {frame.url[:50]})")
                        break
                except Exception:
                    continue
            if id_input:
                break

        if target_frame:
            for sel in pw_selectors:
                try:
                    loc = target_frame.locator(sel)
                    if await loc.count() > 0 and await loc.first.is_visible():
                        pw_input = loc.first
                        print(f"  PW入力欄: {sel}")
                        break
                except Exception:
                    continue

        if id_input and pw_input:
            await id_input.fill(login_id)
            await pw_input.fill(password)
            await page.wait_for_timeout(500)

            # 送信
            submit_selectors = [
                "button[type='submit']", "input[type='submit']",
                "button:has-text('ログイン')", "button:has-text('Login')",
                "button:has-text('サインイン')", "button:has-text('次へ')",
            ]
            submitted = False
            if target_frame:
                for sel in submit_selectors:
                    try:
                        loc = target_frame.locator(sel)
                        if await loc.count() > 0:
                            await loc.first.click()
                            submitted = True
                            print(f"  送信ボタン: {sel}")
                            break
                    except Exception:
                        continue

            if not submitted:
                await pw_input.press("Enter")

            await page.wait_for_timeout(8000)

            # SAML認証フロー: 追加の認証ステップがあるか確認
            print(f"  認証後URL: {page.url}")
            if "consent" in page.url or "authorize" in page.url:
                print("  → 同意画面が表示されています")
                await page.screenshot(path=str(SCREENSHOT_DIR / "02a_consent.png"), full_page=True)
                # 同意ボタンを探す
                for sel in ["button:has-text('許可')", "button:has-text('同意')", "button:has-text('Accept')"]:
                    loc = page.locator(sel)
                    if await loc.count() > 0:
                        await loc.first.click()
                        await page.wait_for_timeout(5000)
                        break
        else:
            print("  WARNING: ID/PW入力欄が見つかりません")
            # いい生活アカウントでログインボタンがあるか
            sso_selectors = [
                "button:has-text('いい生活アカウントでログイン')",
                "a:has-text('いい生活アカウントでログイン')",
                "button.MuiButton-contained",
            ]
            for sel in sso_selectors:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    print(f"  SSOボタン発見: {sel}")
                    await loc.first.click()
                    await page.wait_for_timeout(5000)
                    await page.screenshot(path=str(SCREENSHOT_DIR / "02a_sso_redirect.png"), full_page=True)
                    print(f"  SSOリダイレクト先: {page.url}")
                    # リダイレクト先でログインフォームを再探索
                    for frame in page.frames:
                        for id_sel in id_selectors:
                            try:
                                loc2 = frame.locator(id_sel)
                                if await loc2.count() > 0 and await loc2.first.is_visible():
                                    id_input = loc2.first
                                    print(f"  → ID入力欄発見: {id_sel}")
                                    break
                            except Exception:
                                continue
                        if id_input:
                            for pw_sel in pw_selectors:
                                try:
                                    loc2 = frame.locator(pw_sel)
                                    if await loc2.count() > 0 and await loc2.first.is_visible():
                                        pw_input = loc2.first
                                        break
                                except Exception:
                                    continue
                            if pw_input:
                                await id_input.fill(login_id)
                                await pw_input.fill(password)
                                await pw_input.press("Enter")
                                await page.wait_for_timeout(8000)
                            break
                    break

        await page.screenshot(path=str(SCREENSHOT_DIR / "02_after_login.png"), full_page=True)
        print(f"  ログイン後URL: {page.url}")

        # ---- Step 4: ログイン後のナビゲーション解析 ----
        print("\n[RECON] === ログイン後のページ解析 ===")
        links = await page.query_selector_all("a")
        for link in links[:30]:
            href = await link.get_attribute("href") or ""
            text = (await link.inner_text()).strip()[:50]
            if text:
                print(f"  link: text={text}, href={href}")

        # ---- Step 5: 物件検索フォーム探索 ----
        print("\n[RECON] === 物件検索フォーム探索 ===")

        # ナビゲーションメニューから検索ページへ
        search_nav = [
            "a:has-text('物件検索')", "a:has-text('検索')",
            "a:has-text('建物検索')", "a[href*='search']",
            "a[href*='bukken']", "a[href*='tatemono']",
        ]
        for sel in search_nav:
            loc = page.locator(sel)
            if await loc.count() > 0:
                print(f"\n[RECON] 検索リンク発見: {sel}")
                try:
                    await loc.first.click()
                    await page.wait_for_timeout(3000)
                    await page.screenshot(path=str(SCREENSHOT_DIR / "03_search_page.png"), full_page=True)
                    print(f"  検索ページURL: {page.url}")
                    break
                except Exception as e:
                    print(f"  クリック失敗: {e}")

        # 検索フォーム解析
        all_inputs = await page.query_selector_all("input, select, textarea")
        for inp in all_inputs:
            tag = await inp.evaluate("el => el.tagName")
            inp_type = await inp.get_attribute("type") or ""
            name = await inp.get_attribute("name") or ""
            inp_id = await inp.get_attribute("id") or ""
            placeholder = await inp.get_attribute("placeholder") or ""
            if inp_type not in ("hidden",):
                print(f"  {tag}: type={inp_type}, name={name}, id={inp_id}, placeholder={placeholder}")

        # ---- Step 6: テスト検索 ----
        building_selectors = [
            "input[name*='tatemono']", "input[name*='building']",
            "input[name*='property']", "input[name*='name']",
            "input[placeholder*='建物']", "input[placeholder*='物件']",
        ]
        building_input = None
        for sel in building_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                building_input = loc.first
                print(f"\n[RECON] 建物名入力欄: {sel}")
                break

        if building_input:
            await building_input.fill("テスト")
            for sel in ["button[type='submit']", "input[type='submit']", "button:has-text('検索')"]:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    try:
                        await loc.first.click()
                        await page.wait_for_timeout(5000)
                    except Exception:
                        pass
                    break

            await page.screenshot(path=str(SCREENSHOT_DIR / "04_search_result.png"), full_page=True)
            print(f"  検索結果URL: {page.url}")

            # 結果ページ解析
            print("\n[RECON] === 検索結果ページ解析 ===")
            body_text = await page.inner_text("body")

            for kw in ["件", "hit", "result"]:
                if kw in body_text[:500]:
                    idx = body_text.find(kw)
                    context = body_text[max(0, idx-30):idx+10]
                    print(f"  件数表示?: ...{context}...")

            tables = await page.query_selector_all("table")
            print(f"  table要素: {len(tables)}個")

            for kw in ["募集中", "申込", "空室", "満室", "成約", "契約済"]:
                if kw in body_text:
                    print(f"  ステータスキーワード「{kw}」: 発見")

        # ---- Step 7: HTML保存 ----
        html = await page.content()
        (SCREENSHOT_DIR / "page_source.html").write_text(html, encoding="utf-8")
        print(f"\n[RECON] ページソースを保存: {SCREENSHOT_DIR / 'page_source.html'}")

        await browser.close()
        print("\n[RECON] 完了")


if __name__ == "__main__":
    asyncio.run(recon())
