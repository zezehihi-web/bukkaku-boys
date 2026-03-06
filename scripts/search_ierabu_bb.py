"""いえらぶBBで9社の物件を検索

itanji/es_squareに未登録だった9社の物件名で検索し、
いえらぶBBに存在するか確認する。
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
LOGIN_ID = "bebe1234"
LOGIN_PASS = "beberise1"

# itanjiにもes_squareにもなかった9社 + 物件名
SEARCH_TARGETS = [
    ("京王不動産", "Coeur Blanc八幡山"),
    ("小寺商店", "ザ・パークハウス西麻布レジデンス"),
    ("六耀", "ニューハイム上板橋"),
    ("ポルンガ", "エクセリア高島平"),
    ("ポルンガ", "Okapi亀有"),
    ("内田物産", "ロングエイト"),
    ("ドリームコネクション", "ピアース高田馬場"),
    ("まいら", "アークマーク中野鷺宮"),
    ("栗原建設", "ルミエール平井"),
]


async def main():
    print("=== いえらぶBB 管理会社物件検索 ===\n", flush=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # ログイン
        print("ログイン中...", flush=True)
        await page.goto(LOGIN_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        # ログインフォーム探索
        html = await page.content()
        print(f"  ページタイトル: {await page.title()}", flush=True)

        # ID/パスワード入力
        id_input = page.locator('input[name="login_id"], input[name="id"], input#login_id, input[type="text"]').first
        pw_input = page.locator('input[name="login_pw"], input[name="password"], input#login_pw, input[type="password"]').first

        await id_input.fill(LOGIN_ID)
        await pw_input.fill(LOGIN_PASS)

        # ログインボタン
        submit = page.locator('input[type="submit"], button[type="submit"], .login-btn, .btn-login').first
        await submit.click()
        await page.wait_for_timeout(3000)

        print(f"  ログイン後URL: {page.url}", flush=True)

        # 検索ページへ遷移
        search_url = "https://bb.ielove.jp/ielovebb/rent/searchmenu/"
        await page.goto(search_url, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        # 検索フォームの構造を確認
        print(f"\n検索ページ構造探索中...", flush=True)

        # フリーワード/物件名検索のinputを探す
        form_html = await page.evaluate("""() => {
            const inputs = document.querySelectorAll('input[type="text"], textarea');
            const result = [];
            for (const el of inputs) {
                result.push({
                    name: el.name || '',
                    id: el.id || '',
                    placeholder: el.placeholder || '',
                    type: el.type || '',
                    class: el.className || '',
                });
            }
            return JSON.stringify(result, null, 2);
        }""")
        print(f"  テキスト入力欄: {form_html}", flush=True)

        # select要素も確認
        selects = await page.evaluate("""() => {
            const sels = document.querySelectorAll('select');
            const result = [];
            for (const el of sels) {
                result.push({
                    name: el.name || '',
                    id: el.id || '',
                    class: el.className || '',
                    options_count: el.options.length,
                });
            }
            return JSON.stringify(result, null, 2);
        }""")
        print(f"  セレクト要素: {selects}", flush=True)

        # リンクからフリーワード検索を探す
        links = await page.evaluate("""() => {
            const as = document.querySelectorAll('a');
            const result = [];
            for (const a of as) {
                const text = a.textContent.trim().substring(0, 50);
                const href = a.href || '';
                if (text.includes('フリー') || text.includes('キーワード') ||
                    text.includes('物件名') || text.includes('建物名') ||
                    href.includes('free') || href.includes('keyword') ||
                    href.includes('search')) {
                    result.push({text, href});
                }
            }
            return JSON.stringify(result, null, 2);
        }""")
        print(f"  関連リンク: {links}", flush=True)

        # ページのメインメニュー/検索オプションを確認
        menu_text = await page.evaluate("""() => {
            const body = document.body.innerText;
            const lines = body.split('\\n').filter(l => l.trim());
            return lines.slice(0, 80).join('\\n');
        }""")
        print(f"\n  ページテキスト(上部80行):\n{menu_text}", flush=True)

        # フリーワード検索がある場合、各物件を検索
        # まずフリーワード入力欄を探す
        freeword_input = None
        for sel in [
            'input[name*="free"]', 'input[name*="keyword"]',
            'input[name*="word"]', 'input[name*="bldg"]',
            'input[name*="building"]', 'input[name*="tatemono"]',
            'input[placeholder*="フリー"]', 'input[placeholder*="物件"]',
            'input[placeholder*="キーワード"]', 'input[placeholder*="建物"]',
        ]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                freeword_input = loc.first
                print(f"\n  フリーワード入力欄発見: {sel}", flush=True)
                break

        if freeword_input:
            print("\n--- 物件検索開始 ---\n", flush=True)
            for company, prop_name in SEARCH_TARGETS:
                try:
                    await freeword_input.fill(prop_name)
                    # 検索ボタンを押す
                    search_btn = page.locator('input[type="submit"], button[type="submit"], .search-btn, .btn-search').first
                    await search_btn.click()
                    await page.wait_for_timeout(3000)

                    body = await page.inner_text("body")
                    if "0件" in body or "見つかりません" in body:
                        print(f"  {company:20s} [{prop_name}] -> NOT FOUND", flush=True)
                    else:
                        print(f"  {company:20s} [{prop_name}] -> FOUND", flush=True)
                        # 最初の結果のテキストを表示
                        first_result = await page.evaluate("""() => {
                            const el = document.querySelector('.bkn_detail, .result-item, .bukken-item, tr.data');
                            return el ? el.innerText.substring(0, 200) : '(結果テキスト取得不可)';
                        }""")
                        print(f"         {first_result[:100]}", flush=True)

                    # 検索ページに戻る
                    await page.goto(search_url, wait_until="load", timeout=30000)
                    await page.wait_for_timeout(1500)
                    # フリーワード入力欄を再取得
                    freeword_input = page.locator(sel).first

                except Exception as e:
                    print(f"  {company:20s} [{prop_name}] -> ERROR: {e}", flush=True)

                await asyncio.sleep(2)
        else:
            print("\n  フリーワード検索が見つかりません。別の方法を試します...", flush=True)

            # URL直接検索を試す
            # いえらぶBBの検索URLパターンを試す
            search_patterns = [
                "https://bb.ielove.jp/ielovebb/rent/search/?free_word={q}",
                "https://bb.ielove.jp/ielovebb/rent/list/?keyword={q}",
                "https://bb.ielove.jp/ielovebb/rent/search/?building_name={q}",
            ]

            import urllib.parse
            test_prop = "Coeur Blanc八幡山"
            for pattern in search_patterns:
                url = pattern.format(q=urllib.parse.quote(test_prop))
                try:
                    await page.goto(url, wait_until="load", timeout=15000)
                    await page.wait_for_timeout(2000)
                    body = await page.inner_text("body")
                    print(f"  試行: {url[:80]}...", flush=True)
                    print(f"    結果: {body[:200]}", flush=True)
                except Exception as e:
                    print(f"  試行失敗: {pattern[:50]}... -> {e}", flush=True)

        await browser.close()

    print("\n=== 完了 ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
