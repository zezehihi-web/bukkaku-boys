"""リアルネットプロ検索テスト"""
import asyncio
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # ログイン
        await page.goto("https://www.realnetpro.com/index.php", wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        await page.locator("input#login_input").fill("info-bebe")
        await page.locator("input#password_input").fill("beberise00")
        await page.locator('button:has-text("ログイン")').click()
        await page.wait_for_timeout(3000)
        print(f"ログイン後URL: {page.url}")

        # 検索フィールドに物件名入力
        search_input = page.locator("input#top_free")
        await search_input.fill("AXIA CITY大山")
        await page.wait_for_timeout(500)

        # 検索ボタンを探す
        # "検 索" テキストの近くにあるボタン or リンク
        search_btn = await page.evaluate("""() => {
            // onclick属性付きの検索ボタンを探す
            const allEls = document.querySelectorAll('*[onclick]');
            for (const el of allEls) {
                const onclick = el.getAttribute('onclick') || '';
                if (onclick.includes('search') || onclick.includes('free')) {
                    return {
                        tag: el.tagName,
                        text: el.textContent.trim().substring(0, 50),
                        onclick: onclick.substring(0, 100),
                        class: el.className
                    };
                }
            }
            return null;
        }""")
        print(f"\n検索ボタン候補: {search_btn}")

        # Enterキーで検索
        await search_input.press("Enter")
        await page.wait_for_timeout(5000)

        print(f"\n検索後URL: {page.url}")

        # 結果ページの構造確認
        text = await page.inner_text("body")
        print(f"\n=== 検索結果(先頭800文字) ===\n{text[:800]}")

        # 物件ブロック確認
        buildings = await page.evaluate("""() => {
            const candidates = [
                '.one_building', '.building_item', '.bukken-item',
                '.search-result-item', 'tr.data', '.property-item',
                '.result-item', '.list-item'
            ];
            for (const sel of candidates) {
                const els = document.querySelectorAll(sel);
                if (els.length > 0) {
                    return {
                        selector: sel,
                        count: els.length,
                        first_text: els[0].innerText.substring(0, 300)
                    };
                }
            }
            // テーブル行を確認
            const trs = document.querySelectorAll('table tr');
            if (trs.length > 2) {
                return {
                    selector: 'table tr',
                    count: trs.length,
                    first_text: trs[1] ? trs[1].innerText.substring(0, 300) : ''
                };
            }
            return null;
        }""")
        print(f"\n物件ブロック: {buildings}")

        # ステータス要素
        statuses = await page.evaluate("""() => {
            const spans = document.querySelectorAll('span.st, .status, .badge');
            return Array.from(spans).slice(0, 10).map(s => ({
                text: s.textContent.trim(),
                class: s.className
            }));
        }""")
        print(f"\nステータス要素: {statuses}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
