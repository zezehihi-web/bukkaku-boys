"""いえらぶBB ドリームコネクション詳細確認"""
import asyncio
import sys
import os
import re

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


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # ログイン
        await page.goto(LOGIN_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        id_input = page.locator('input[name="login_id"], input[name="id"], input#login_id, input[type="text"]').first
        pw_input = page.locator('input[name="login_pw"], input[name="password"], input#login_pw, input[type="password"]').first
        await id_input.fill(LOGIN_ID)
        await pw_input.fill(LOGIN_PASS)
        submit = page.locator('input[type="submit"], button[type="submit"]').first
        await submit.click()
        await page.wait_for_timeout(3000)

        # ドリームコネクション検索
        await page.goto(SEARCH_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(1500)

        kcnm = page.locator('input#kcnm')
        await kcnm.fill("ドリームコネクション")

        search_btn = page.locator('input[type="submit"], button[type="submit"]').first
        await search_btn.click()
        await page.wait_for_timeout(4000)

        # 結果ページの詳細取得
        body = await page.inner_text("body")

        # 件数
        count_match = re.search(r'全(\d+)件', body)
        if count_match:
            print(f"ドリームコネクション: 全{count_match.group(1)}件", flush=True)
        else:
            # 別パターンで件数確認
            count_match2 = re.search(r'(\d+)\s*件', body[:1000])
            print(f"ドリームコネクション: {count_match2.group(0) if count_match2 else '件数不明'}", flush=True)

        # 物件リストの詳細
        details = await page.evaluate("""() => {
            // 物件名リンクを取得
            const links = document.querySelectorAll('a.bkn_detail');
            const results = [];
            for (const a of links) {
                const row = a.closest('tr') || a.closest('.bkn_row') || a.parentElement;
                results.push({
                    text: a.textContent.trim().substring(0, 100),
                    href: a.href,
                    row_text: row ? row.textContent.trim().substring(0, 200) : ''
                });
            }

            // fallback: table rows
            if (results.length === 0) {
                const rows = document.querySelectorAll('table tr');
                for (let i = 0; i < Math.min(10, rows.length); i++) {
                    results.push({
                        text: rows[i].textContent.trim().substring(0, 200),
                        href: '',
                        row_text: ''
                    });
                }
            }

            return results;
        }""")

        print(f"\n結果: {len(details)}件のリンク/行", flush=True)
        for i, d in enumerate(details[:10]):
            print(f"  {i+1}. {d['text']}", flush=True)
            if d.get('row_text'):
                # 行テキストから物件名等を抽出
                clean = d['row_text'].replace('\n', ' ').replace('\t', ' ')
                clean = re.sub(r'\s+', ' ', clean)[:150]
                print(f"     {clean}", flush=True)

        # ページの主要テキストの冒頭を表示
        print(f"\n--- ページテキスト(先頭500文字) ---", flush=True)
        print(body[:500], flush=True)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
