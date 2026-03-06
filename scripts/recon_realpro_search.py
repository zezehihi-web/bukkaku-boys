"""リアルネットプロ検索ページ構造調査"""
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

        # フォーム要素
        inputs = await page.evaluate("""() => {
            const inputs = document.querySelectorAll('input, textarea, select');
            return Array.from(inputs).map(i => ({
                tag: i.tagName, name: i.name || '', id: i.id || '',
                type: i.type || '', placeholder: i.placeholder || '',
                visible: i.offsetParent !== null,
            }));
        }""")
        print("\n=== フォーム要素 ===")
        for inp in inputs[:40]:
            vis = "V" if inp["visible"] else "H"
            print(f"  [{vis}] {inp['tag']:8s} name={inp['name']:25s} id={inp['id']:25s} type={inp['type']:10s} ph={inp['placeholder']}")

        # ボタン/リンク
        buttons = await page.evaluate("""() => {
            const els = document.querySelectorAll('button, input[type=submit], input[type=button], a');
            return Array.from(els).slice(0, 30).map(e => ({
                tag: e.tagName, text: e.textContent.trim().substring(0, 40),
                href: (e.href || '').substring(0, 80),
                onclick: (e.getAttribute('onclick') || '').substring(0, 80),
                visible: e.offsetParent !== null,
            }));
        }""")
        print("\n=== ボタン/リンク ===")
        for b in buttons:
            if b["visible"]:
                print(f"  {b['tag']:8s} text={b['text']:30s} href={b['href']} onclick={b['onclick']}")

        # body テキスト冒頭
        text = await page.inner_text("body")
        print(f"\n=== ページテキスト(先頭600文字) ===\n{text[:600]}")

        # フレームの確認
        frames = page.frames
        print(f"\n=== フレーム数: {len(frames)} ===")
        for i, frame in enumerate(frames):
            print(f"  frame[{i}]: url={frame.url[:80]} name={frame.name}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
