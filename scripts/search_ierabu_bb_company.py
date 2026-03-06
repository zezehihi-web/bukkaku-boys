"""いえらぶBBで管理会社名検索（kcnmフィールド使用）

物件名ではなく管理会社名で直接検索し、各社がいえらぶBBに存在するか確認。
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from playwright.async_api import async_playwright

LOGIN_URL = "https://bb.ielove.jp/ielovebb/login/index/"
SEARCH_URL = "https://bb.ielove.jp/ielovebb/rent/searchmenu/"
LOGIN_ID = "bebe1234"
LOGIN_PASS = "beberise1"

# itanjiにもes_squareにもなかった9社
COMPANIES = [
    "京王不動産",
    "小寺商店",
    "六耀",
    "ポルンガ",
    "内田物産",
    "ドリームコネクション",
    "まいら",
    "愛三土地建物",
    "栗原建設",
]


async def main():
    print("=== いえらぶBB 管理会社名検索 (kcnmフィールド) ===\n", flush=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # ログイン
        print("ログイン中...", flush=True)
        await page.goto(LOGIN_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        id_input = page.locator('input[name="login_id"], input[name="id"], input#login_id, input[type="text"]').first
        pw_input = page.locator('input[name="login_pw"], input[name="password"], input#login_pw, input[type="password"]').first
        await id_input.fill(LOGIN_ID)
        await pw_input.fill(LOGIN_PASS)

        submit = page.locator('input[type="submit"], button[type="submit"], .login-btn, .btn-login').first
        await submit.click()
        await page.wait_for_timeout(3000)
        print(f"  ログイン後URL: {page.url}\n", flush=True)

        for company in COMPANIES:
            try:
                # 検索ページへ
                await page.goto(SEARCH_URL, wait_until="load", timeout=30000)
                await page.wait_for_timeout(1500)

                # 管理会社名フィールドに入力
                kcnm = page.locator('input#kcnm')
                await kcnm.fill(company)

                # 検索ボタン
                search_btn = page.locator('input[type="submit"], button[type="submit"]').first
                await search_btn.click()
                await page.wait_for_timeout(3000)

                # 結果確認
                body = await page.inner_text("body")

                # 件数を探す
                import re
                count_match = re.search(r'(\d+)\s*件', body[:500])
                count = count_match.group(1) if count_match else "?"

                if "0件" in body[:500] or "見つかりませんでした" in body:
                    print(f"  {company:20s} -> NOT FOUND (0件)", flush=True)
                else:
                    print(f"  {company:20s} -> FOUND ({count}件)", flush=True)
                    # 最初の物件名を取得
                    first_prop = await page.evaluate("""() => {
                        const el = document.querySelector('a.bkn_detail, .bkn_name, .bukken-title');
                        return el ? el.textContent.trim().substring(0, 80) : '';
                    }""")
                    if first_prop:
                        print(f"         物件例: {first_prop}", flush=True)

            except Exception as e:
                print(f"  {company:20s} -> ERROR: {e}", flush=True)

            await asyncio.sleep(2)

        await browser.close()

    print("\n=== 完了 ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
