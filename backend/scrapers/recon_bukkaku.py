"""物確.com (bukkaku.jp) 偵察スクリプト

CICなどのサブドメインにログインし、フォームセレクタ・検索UI・結果ページ構造を特定する。
使い捨て: 本番には組み込まない。

使い方:
  python -m backend.scrapers.recon_bukkaku

  環境変数:
    CIC_URL=https://cic.bukkaku.jp
    CIC_ID=your_login_id
    CIC_PASS=your_password
"""

import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright


SCREENSHOT_DIR = Path("results/recon_bukkaku")


async def recon():
    url = os.getenv("CIC_URL", "https://cic.bukkaku.jp")
    login_id = os.getenv("CIC_ID", "")
    password = os.getenv("CIC_PASS", "")

    if not login_id or not password:
        print("ERROR: CIC_ID / CIC_PASS 環境変数が未設定です")
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

        # ログインフォームのセレクタ探索
        print("\n[RECON] === ログインフォーム解析 ===")
        inputs = await page.query_selector_all("input")
        for inp in inputs:
            inp_type = await inp.get_attribute("type") or ""
            inp_name = await inp.get_attribute("name") or ""
            inp_id = await inp.get_attribute("id") or ""
            inp_placeholder = await inp.get_attribute("placeholder") or ""
            print(f"  input: type={inp_type}, name={inp_name}, id={inp_id}, placeholder={inp_placeholder}")

        buttons = await page.query_selector_all("button, input[type='submit']")
        for btn in buttons:
            tag = await btn.evaluate("el => el.tagName")
            text = await btn.inner_text() if tag == "BUTTON" else await btn.get_attribute("value")
            print(f"  button: tag={tag}, text={text}")

        # ---- Step 2: ログイン実行 ----
        print(f"\n[RECON] ログイン試行中...")

        # 一般的なログインフォームセレクタを試行
        id_selectors = [
            "input[name='login_id']", "input[name='loginId']",
            "input[name='user_id']", "input[name='userId']",
            "input[name='username']", "input[name='email']",
            "input[type='text']", "input[type='email']",
            "input#login_id", "input#userId", "input#username",
        ]
        pw_selectors = [
            "input[name='password']", "input[name='login_password']",
            "input[type='password']",
        ]
        submit_selectors = [
            "button[type='submit']", "input[type='submit']",
            "button:has-text('ログイン')", "button:has-text('Login')",
        ]

        id_input = None
        for sel in id_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                id_input = loc.first
                print(f"  ID入力欄: {sel}")
                break

        pw_input = None
        for sel in pw_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                pw_input = loc.first
                print(f"  PW入力欄: {sel}")
                break

        if id_input and pw_input:
            await id_input.fill(login_id)
            await pw_input.fill(password)
            await page.wait_for_timeout(500)

            submit_btn = None
            for sel in submit_selectors:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    submit_btn = loc.first
                    print(f"  送信ボタン: {sel}")
                    break

            if submit_btn:
                try:
                    async with page.expect_navigation(timeout=30000):
                        await submit_btn.click()
                except Exception:
                    await submit_btn.click()
            else:
                await pw_input.press("Enter")

            await page.wait_for_timeout(5000)
        else:
            print("  WARNING: ID/PW入力欄が見つかりません")

        await page.screenshot(path=str(SCREENSHOT_DIR / "02_after_login.png"), full_page=True)
        print(f"  ログイン後URL: {page.url}")

        # ---- Step 3: ログイン後のナビゲーション解析 ----
        print("\n[RECON] === ログイン後のページ解析 ===")
        links = await page.query_selector_all("a")
        for link in links[:30]:
            href = await link.get_attribute("href") or ""
            text = (await link.inner_text()).strip()[:50]
            if text:
                print(f"  link: text={text}, href={href}")

        # ---- Step 4: 物件検索フォーム探索 ----
        print("\n[RECON] === 物件検索フォーム探索 ===")
        search_keywords = ["検索", "search", "物件", "建物"]
        for kw in search_keywords:
            elements = await page.query_selector_all(f"*:has-text('{kw}')")
            if elements:
                print(f"  '{kw}' を含む要素: {len(elements)}個")

        # 検索っぽいinputを探す
        all_inputs = await page.query_selector_all("input[type='text'], input[type='search']")
        for inp in all_inputs:
            name = await inp.get_attribute("name") or ""
            placeholder = await inp.get_attribute("placeholder") or ""
            inp_id = await inp.get_attribute("id") or ""
            print(f"  検索候補input: name={name}, id={inp_id}, placeholder={placeholder}")

        # 検索ページへのナビゲーションを試みる
        search_nav_selectors = [
            "a:has-text('検索')", "a:has-text('物件検索')",
            "a:has-text('建物検索')", "a[href*='search']",
        ]
        for sel in search_nav_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                print(f"\n[RECON] 検索ページリンク発見: {sel}")
                try:
                    await loc.first.click()
                    await page.wait_for_timeout(3000)
                    await page.screenshot(path=str(SCREENSHOT_DIR / "03_search_page.png"), full_page=True)
                    print(f"  検索ページURL: {page.url}")

                    # 検索フォームの入力欄を再解析
                    print("\n[RECON] === 検索フォーム解析 ===")
                    all_inputs2 = await page.query_selector_all("input, select, textarea")
                    for inp in all_inputs2:
                        tag = await inp.evaluate("el => el.tagName")
                        inp_type = await inp.get_attribute("type") or ""
                        name = await inp.get_attribute("name") or ""
                        inp_id = await inp.get_attribute("id") or ""
                        placeholder = await inp.get_attribute("placeholder") or ""
                        print(f"  {tag}: type={inp_type}, name={name}, id={inp_id}, placeholder={placeholder}")
                    break
                except Exception as e:
                    print(f"  クリック失敗: {e}")

        # ---- Step 5: テスト検索実行 ----
        # 建物名入力欄を探して適当な名前で検索
        building_selectors = [
            "input[name*='building']", "input[name*='tatemono']",
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
            submit_selectors2 = [
                "button[type='submit']", "input[type='submit']",
                "button:has-text('検索')",
            ]
            for sel in submit_selectors2:
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

            # 結果ページの構造解析
            print("\n[RECON] === 検索結果ページ解析 ===")
            body_text = await page.inner_text("body")
            # 件数表示
            for kw in ["件", "hit", "result"]:
                if kw in body_text[:500]:
                    # 件数部分を抽出
                    idx = body_text.find(kw)
                    context = body_text[max(0, idx-30):idx+10]
                    print(f"  件数表示?: ...{context}...")

            # テーブル/リスト構造
            tables = await page.query_selector_all("table")
            print(f"  table要素: {len(tables)}個")
            lists = await page.query_selector_all("ul, ol, dl")
            print(f"  list要素: {len(lists)}個")
            divs = await page.query_selector_all("div.result, div.search-result, div.property, div.bukken")
            print(f"  結果div: {len(divs)}個")

            # ステータス関連キーワード
            for kw in ["募集中", "申込", "空室", "満室", "成約", "契約済"]:
                if kw in body_text:
                    print(f"  ステータスキーワード「{kw}」: 発見")

        # ---- Step 6: HTML構造をファイル保存 ----
        html = await page.content()
        (SCREENSHOT_DIR / "page_source.html").write_text(html, encoding="utf-8")
        print(f"\n[RECON] ページソースを保存: {SCREENSHOT_DIR / 'page_source.html'}")

        await browser.close()
        print("\n[RECON] 完了")


if __name__ == "__main__":
    asyncio.run(recon())
