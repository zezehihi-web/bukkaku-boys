"""イタンジBBに登録されている管理会社を全件取得するスクリプト

戦略:
1. PlaywrightでイタンジBBにログイン (bukkakun.com → itandi-accounts SSO)
2. 認証済みCookieを取得
3. 管理会社検索API (api.itandibb.com) を直接叩いて全社収集
"""

import asyncio
import json
import os
import sys
import urllib.parse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from playwright.async_api import async_playwright
import aiohttp


ITANJI_EMAIL = os.getenv("ITANJI_EMAIL", "")
ITANJI_PASSWORD = os.getenv("ITANJI_PASSWORD", "")
ITANJI_TOP_URL = "https://bukkakun.com/"
LIST_SEARCH_URL = "https://itandibb.com/rent_rooms/list"
API_BASE = "https://api.itandibb.com/api/internal/management_companies/search"

# 検索文字
HIRAGANA = list("あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん")
DAKUON = list("がぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽ")
KATAKANA = list("アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン")
ALPHABET = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
DIGITS = list("0123456789")

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")


async def login_and_get_cookies(page) -> list[dict] | None:
    """イタンジBBにログインしてCookieを返す。"""
    print("[LOGIN] イタンジBBにログイン中...")
    await page.goto(ITANJI_TOP_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    for sel in ['a:has-text("ログイン")', 'a[href*="login"]', 'a[href*="itandi-accounts"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            try:
                async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
                    await loc.first.click()
            except Exception:
                await loc.first.click()
            break
    await page.wait_for_timeout(3000)

    title = await page.title()
    if "400" in title:
        await page.goto(ITANJI_TOP_URL, wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)
        for sel in ['a:has-text("ログイン")', 'a[href*="login"]']:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click()
                break
        await page.wait_for_timeout(3000)

    email_input = None
    for sel in ["#email", 'input[name="email"]', 'input[type="email"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            email_input = loc.first
            break
    if not email_input:
        print("[ERROR] メール入力欄が見つかりません")
        return None

    await email_input.fill(ITANJI_EMAIL)
    await page.wait_for_timeout(500)

    pw_input = None
    for sel in ["#password", 'input[name="password"]', 'input[type="password"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            pw_input = loc.first
            break
    if not pw_input:
        print("[ERROR] パスワード欄が見つかりません")
        return None

    await pw_input.fill(ITANJI_PASSWORD)
    await page.wait_for_timeout(500)

    for sel in ['input.filled-button[value="ログイン"]', 'button:has-text("ログイン")',
                 'input[type="submit"]', 'button[type="submit"]']:
        loc = page.locator(sel)
        if await loc.count() > 0:
            try:
                async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
                    await loc.first.click()
            except Exception:
                await loc.first.click()
            break
    await page.wait_for_timeout(3000)

    current_url = page.url
    if "login" in current_url.lower() and "itandi-accounts" in current_url.lower():
        print(f"[ERROR] ログイン失敗: {current_url}")
        return None

    print(f"[LOGIN] ログイン成功: {current_url}")

    # Cookieを取得
    cookies = await page.context.cookies()
    print(f"[COOKIES] {len(cookies)}個のCookieを取得")
    return cookies


async def search_companies(session: aiohttp.ClientSession, char: str) -> list[dict]:
    """APIを直接叩いて管理会社を検索。"""
    url = f"{API_BASE}?company_name={urllib.parse.quote(char)}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # {"management_companies": [...]} 形式
                    for key in ("management_companies", "results", "data", "companies", "items"):
                        if key in data and isinstance(data[key], list):
                            return data[key]
                    return [data]
                return []
            else:
                text = await resp.text()
                print(f"  [WARN] HTTP {resp.status}: {text[:200]}")
                return []
    except Exception as e:
        print(f"  [ERROR] {e}")
        return []


async def main():
    print(f"[{datetime.now():%H:%M:%S}] イタンジBB管理会社スクレイピング開始")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # ===== Phase 1: Playwrightでログイン → Cookie取得 =====
    cookies = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        cookies = await login_and_get_cookies(page)
        await browser.close()

    if not cookies:
        print("[ERROR] Cookie取得失敗。終了。")
        return

    # ===== Phase 2: APIで管理会社を収集 =====
    print(f"\n[PHASE 2] APIで管理会社を収集...")

    # Cookieをaiohttp用に変換
    cookie_jar = aiohttp.CookieJar()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": LIST_SEARCH_URL,
        "Origin": "https://itandibb.com",
    }

    async with aiohttp.ClientSession(headers=headers, cookie_jar=cookie_jar) as session:
        # Cookieをセット
        for c in cookies:
            cookie = {
                "key": c["name"],
                "value": c["value"],
                "domain": c.get("domain", ""),
                "path": c.get("path", "/"),
            }
            session.cookie_jar.update_cookies(
                {c["name"]: c["value"]},
                response_url=aiohttp.client.URL(f"https://{c.get('domain', 'itandibb.com').lstrip('.')}/")
            )

        # まずテスト: 「あ」で検索
        print("[TEST] APIテスト: 「あ」...")
        test_results = await search_companies(session, "あ")
        print(f"[TEST] 結果: {len(test_results)}件")

        if test_results:
            # レスポンス構造を確認
            sample = test_results[0]
            print(f"[TEST] レスポンス構造: {json.dumps(sample, ensure_ascii=False)[:300]}")

        if not test_results:
            print("[ERROR] APIからデータ取得できません。認証が必要な可能性があります。")
            print("[FALLBACK] 別のアプローチを試します...")
            return

        # 全文字で検索
        all_companies = {}  # name -> full_data
        search_chars = HIRAGANA + DAKUON + KATAKANA + ALPHABET + DIGITS
        total = len(search_chars)

        print(f"\n[SCRAPE] {total}文字でAPI検索開始...")
        for i, char in enumerate(search_chars, 1):
            results = await search_companies(session, char)
            new_count = 0
            for item in results:
                # 管理会社名を抽出（レスポンス構造に依存）
                if isinstance(item, dict):
                    name = item.get("name") or item.get("company_name") or item.get("label") or str(item)
                else:
                    name = str(item)
                if name not in all_companies:
                    all_companies[name] = item
                    new_count += 1
            print(f"  [{i}/{total}] '{char}': {len(results)}件 (新規{new_count}, 累計{len(all_companies)})")

            # レート制限: 軽い待機
            await asyncio.sleep(0.3)

    # ===== 結果出力 =====
    sorted_names = sorted(all_companies.keys())
    output_path = os.path.join(RESULTS_DIR, "itanji_companies.txt")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"イタンジBB登録管理会社一覧 ({datetime.now():%Y-%m-%d %H:%M})\n")
        f.write(f"ユニーク管理会社数: {len(sorted_names)}\n")
        f.write("=" * 60 + "\n\n")
        for idx, name in enumerate(sorted_names, 1):
            f.write(f"{idx:4d}. {name}\n")

    json_path = os.path.join(RESULTS_DIR, "itanji_companies.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now().isoformat(),
            "unique_companies": len(sorted_names),
            "companies": [all_companies[n] for n in sorted_names],
        }, f, ensure_ascii=False, indent=2)

    print(f"\n[DONE] {len(sorted_names)}社を取得")
    print(f"  テキスト: {output_path}")
    print(f"  JSON: {json_path}")
    print(f"[{datetime.now():%H:%M:%S}] 完了")


if __name__ == "__main__":
    asyncio.run(main())
