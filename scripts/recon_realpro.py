"""realnetpro.com 偵察スクリプト v3

ログイン → リスト検索 → 検索結果 → 物件詳細の構造を徹底分析。
使い捨て: 本番には組み込まない。

使い方:
  cd C:\\Users\\yamag\\空確くん
  python scripts/recon_realpro.py
"""

import asyncio
import io
import re
import sys
from pathlib import Path
from dotenv import load_dotenv

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

from playwright.async_api import async_playwright

SCREENSHOT_DIR = PROJECT_ROOT / "results" / "recon_realpro"
LOGIN_URL = "https://www.realnetpro.com/index.php"
LOGIN_ID = "info-bebe"
LOGIN_PASS = "beberise00"


async def safe_text(el, max_len=60):
    try:
        t = (await el.inner_text()).strip().replace("\n", " ").replace("\xa0", " ")
        return t[:max_len]
    except Exception:
        return "(取得不可)"


async def recon():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[RECON] 保存先: {SCREENSHOT_DIR}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = await ctx.new_page()

        # ==== Step 1: Login ====
        print(f"\n{'='*60}")
        print("[STEP 1] Login")
        print(f"{'='*60}")

        await page.goto(LOGIN_URL, wait_until="load", timeout=60000)
        await page.wait_for_timeout(2000)
        await page.fill("input#login_input", LOGIN_ID)
        await page.fill("input#password_input", LOGIN_PASS)

        try:
            async with page.expect_navigation(timeout=30000):
                await page.click("button:has-text('ログイン')")
        except Exception:
            await page.wait_for_timeout(5000)

        await page.wait_for_timeout(2000)
        print(f"  URL: {page.url}")
        print(f"  title: {await page.title()}")
        await page.screenshot(path=str(SCREENSHOT_DIR / "01_logged_in.png"), full_page=True)

        # ==== Step 2: Navigate to list search ====
        print(f"\n{'='*60}")
        print("[STEP 2] リスト検索ページへ遷移")
        print(f"{'='*60}")

        await page.goto(
            "https://www.realnetpro.com/main.php?method=estate&display=building",
            wait_until="load", timeout=30000)
        await page.wait_for_timeout(3000)
        print(f"  URL: {page.url}")
        await page.screenshot(path=str(SCREENSHOT_DIR / "02_list_search.png"), full_page=True)

        # ==== Step 2.5: Document the search form ====
        print(f"\n{'='*60}")
        print("[STEP 2.5] 検索フォーム (form#main_form) の構造")
        print(f"{'='*60}")

        print("  form: id=main_form, action=search_cookie.php, method=post")
        print("  --- hidden fields ---")
        print("    page_method = 'estate'")
        print("    page_type = 'building'")
        print("    ini_pref (id=ini_pref) = prefecture code (e.g. '13' for Tokyo)")
        print("    ini_pref_name (id=ini_pref_name) = prefecture name")
        print("  --- visible search fields ---")
        print("    input[name='keyword'] type=search -> フリーワード")
        print("    select[name='transportation_id'] -> 交通機関")
        print("    input[name='required_time'] -> 駅からの時間")
        print("    select[name='update_date'] -> 更新日")
        print("    checkbox[name='enable_enter_flag'] -> 即入居物件で絞り込み")
        print("    select[name='rental_cost1'] / select[name='rental_cost2'] -> 賃料")
        print("    checkbox[name='include_common_fee'] -> 管理費含む")
        print("    checkbox[name='include_parking_cost'] -> 駐車場料金を含む")
        print("    checkbox[name='deposit_recommpence'] -> 敷金・礼金なし")
        print("    select[name='square_meter_l'] / select[name='square_meter_h'] -> 面積")
        print("    checkbox[name='madori_id[]'] -> 間取り")
        print("    select[name='built_year_l'] / select[name='built_year_h'] -> 築年数")
        print("    checkbox[name='building_type[]'] -> 建物種別")
        print("    checkbox[name='room_feature_id[]'] -> 設備・特徴")
        print("    checkbox[name='city_id[]'] -> 市区町村")
        print("    checkbox[name='town_id[]'] -> 町名")
        print("    checkbox[name='route_id[]'] -> 沿線・路線")
        print("    checkbox[name='company_id[]'] -> 管理会社")
        print("  --- submit ---")
        print("    go_search_submit / go_search クラスのクリック -> grecaptcha -> form#main_form.submit()")
        print("    Enter key on input[name=keyword] -> form#main_form.submit()")

        # ==== Step 3: Try submitting the search form directly ====
        print(f"\n{'='*60}")
        print("[STEP 3] キーワード検索: アクシアシティ大山 (form submit)")
        print(f"{'='*60}")

        # Fill keyword
        keyword_input = page.locator("input[name='keyword']")
        if await keyword_input.count() > 0:
            await keyword_input.fill("アクシアシティ大山")
            print("  keyword filled: アクシアシティ大山")

        # Submit form directly (bypass reCAPTCHA since it's v3 invisible)
        try:
            async with page.expect_navigation(timeout=30000):
                await page.evaluate("document.getElementById('main_form').submit()")
        except Exception as e:
            print(f"  submit例外: {e}")
            await page.wait_for_timeout(5000)

        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(SCREENSHOT_DIR / "03_search_result_axia.png"), full_page=True)
        print(f"  URL: {page.url}")

        # Check result count
        body_text = (await page.inner_text("body")).replace("\xa0", " ")
        # 件数パターンを探す
        count_match = re.search(r'(\d+)\s*棟\s*(\d+)\s*戸', body_text)
        if count_match:
            print(f"  結果: {count_match.group(0)}")
        else:
            # ページ上部付近を表示
            header_area = await page.query_selector(".result_header, .search_count, .count_area")
            if header_area:
                ht = await safe_text(header_area, 100)
                print(f"  結果ヘッダー: {ht}")

        # ==== Step 4: Try with broader search - shorter keyword ====
        print(f"\n{'='*60}")
        print("[STEP 4] 短いキーワードで検索: アクシア")
        print(f"{'='*60}")

        await page.goto(
            "https://www.realnetpro.com/main.php?method=estate&display=building",
            wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        keyword_input = page.locator("input[name='keyword']")
        await keyword_input.fill("アクシア")

        try:
            async with page.expect_navigation(timeout=30000):
                await page.evaluate("document.getElementById('main_form').submit()")
        except Exception as e:
            print(f"  submit例外: {e}")
            await page.wait_for_timeout(5000)

        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(SCREENSHOT_DIR / "04_search_result_axia_short.png"), full_page=True)
        print(f"  URL: {page.url}")

        body_text = (await page.inner_text("body")).replace("\xa0", " ")
        count_match = re.search(r'(\d+)\s*棟\s*(\d+)\s*戸', body_text)
        if count_match:
            print(f"  結果: {count_match.group(0)}")
        else:
            print(f"  結果テキスト先頭: {body_text[:200]}")

        html = await page.content()
        (SCREENSHOT_DIR / "04_search_result_axia_short.html").write_text(html, encoding="utf-8")

        # ==== Step 5: Try without prefecture filter ====
        print(f"\n{'='*60}")
        print("[STEP 5] 都道府県制限なしで検索: アクシアシティ大山")
        print(f"{'='*60}")

        await page.goto(
            "https://www.realnetpro.com/main.php?method=estate&display=building",
            wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        keyword_input = page.locator("input[name='keyword']")
        await keyword_input.fill("アクシアシティ大山")
        # Clear the prefecture filter
        await page.evaluate("""() => {
            var el = document.getElementById('ini_pref');
            if (el) el.value = '';
            var el2 = document.getElementById('ini_pref_name');
            if (el2) el2.value = '';
        }""")

        try:
            async with page.expect_navigation(timeout=30000):
                await page.evaluate("document.getElementById('main_form').submit()")
        except Exception as e:
            print(f"  submit例外: {e}")
            await page.wait_for_timeout(5000)

        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(SCREENSHOT_DIR / "05_no_pref_search.png"), full_page=True)
        print(f"  URL: {page.url}")

        body_text = (await page.inner_text("body")).replace("\xa0", " ")
        count_match = re.search(r'(\d+)\s*棟\s*(\d+)\s*戸', body_text)
        if count_match:
            print(f"  結果: {count_match.group(0)}")

        html = await page.content()
        (SCREENSHOT_DIR / "05_no_pref_search.html").write_text(html, encoding="utf-8")

        # ==== Step 6: Search a common building name to get results ====
        print(f"\n{'='*60}")
        print("[STEP 6] よくある物件名で検索: ハイツ (結果構造確認用)")
        print(f"{'='*60}")

        await page.goto(
            "https://www.realnetpro.com/main.php?method=estate&display=building",
            wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        keyword_input = page.locator("input[name='keyword']")
        await keyword_input.fill("ハイツ")

        try:
            async with page.expect_navigation(timeout=30000):
                await page.evaluate("document.getElementById('main_form').submit()")
        except Exception as e:
            print(f"  submit例外: {e}")
            await page.wait_for_timeout(5000)

        await page.wait_for_timeout(5000)
        await page.screenshot(path=str(SCREENSHOT_DIR / "06_search_result_heights.png"), full_page=True)
        print(f"  URL: {page.url}")

        body_text = (await page.inner_text("body")).replace("\xa0", " ")
        count_match = re.search(r'(\d+)\s*棟\s*(\d+)\s*戸', body_text)
        if count_match:
            print(f"  結果: {count_match.group(0)}")

        html = await page.content()
        (SCREENSHOT_DIR / "06_search_result_heights.html").write_text(html, encoding="utf-8")

        # ==== Step 7: Analyze search results structure ====
        print(f"\n{'='*60}")
        print("[STEP 7] 検索結果ページの構造分析")
        print(f"{'='*60}")

        # 物件表示の構造を解析
        # div/table with building info
        for sel in [".building_area", ".one_building", ".building_box",
                    ".estate_box", ".result_box", ".bukken_box",
                    "#building_list", ".building_list", ".search_result",
                    ".result_area", ".estate_list_area", ".list_area",
                    "#search_result_area", ".tatemono", ".building"]:
            els = await page.query_selector_all(sel)
            if els:
                print(f"\n  *FOUND* {sel}: {len(els)}個")
                for j, el in enumerate(els[:2]):
                    t = await safe_text(el, 200)
                    cls = await el.get_attribute("class") or ""
                    print(f"    [{j}] class={cls}: {t[:150]}")

        # テーブルのうちデータが多いもの
        tables = await page.query_selector_all("table")
        for i, tbl in enumerate(tables):
            rows = await tbl.query_selector_all("tr")
            if len(rows) >= 3:
                tbl_cls = await tbl.get_attribute("class") or ""
                tbl_id = await tbl.get_attribute("id") or ""
                print(f"\n  table[{i}] id={tbl_id} class={tbl_cls} rows={len(rows)}")
                # First 3 rows
                for j in range(min(3, len(rows))):
                    cells = await rows[j].query_selector_all("th, td")
                    cell_texts = []
                    for cell in cells:
                        t = await safe_text(cell, 30)
                        cell_texts.append(t)
                    print(f"    row[{j}]: {cell_texts}")

        # ステータスキーワード
        print("\n  --- ステータスキーワード ---")
        for kw in ["募集中", "申込", "空室", "空き", "満室", "成約", "契約済",
                    "入居中", "退去予定", "貸止", "仲介可", "即入居",
                    "空予定", "退去日", "入居可能日"]:
            if kw in body_text:
                idx = body_text.find(kw)
                context = body_text[max(0, idx - 20):idx + 40].replace("\n", " ").strip()
                print(f"  [{kw}]: ...{context}...")

        # 物件リンクのパターン
        print("\n  --- 物件リンクパターン ---")
        all_links = await page.query_selector_all("a")
        seen = set()
        for link in all_links:
            href = await link.get_attribute("href") or ""
            if any(kw in href for kw in ["building_id", "detail", "room_id", "estate_id"]):
                if href not in seen:
                    seen.add(href)
                    t = await safe_text(link, 40)
                    print(f"    [{t}] -> {href}")

        # onclick pattern
        onclick_els = await page.query_selector_all("[onclick*='building'], [onclick*='detail'], [onclick*='room']")
        if onclick_els:
            print(f"\n  onclick要素: {len(onclick_els)}個")
            for el in onclick_els[:5]:
                onclick = await el.get_attribute("onclick") or ""
                t = await safe_text(el, 30)
                print(f"    [{t}] -> {onclick[:80]}")

        # ==== Step 8: Try to click on a property ====
        print(f"\n{'='*60}")
        print("[STEP 8] 物件詳細ページへの遷移を試みる")
        print(f"{'='*60}")

        # building_nameやproperty名のリンクを探す
        property_links = []
        all_a = await page.query_selector_all("a")
        for a in all_a:
            href = await a.get_attribute("href") or ""
            if "building" in href or "detail" in href or "room" in href:
                property_links.append(a)

        # onclickを持つtrやdivも
        clickable_rows = await page.query_selector_all("tr[onclick], div[onclick], td[onclick]")
        for el in clickable_rows[:5]:
            onclick = await el.get_attribute("onclick") or ""
            print(f"  clickable: onclick={onclick[:80]}")

        # 建物名の表示構造を探す - HTML内のパターン
        building_patterns = re.findall(
            r'class="[^"]*building[_-]?name[^"]*"', html)
        if building_patterns:
            print(f"\n  building_name class patterns: {set(building_patterns)}")

        room_patterns = re.findall(
            r'class="[^"]*room[_-]?(?:name|number|no|status|info)[^"]*"', html)
        if room_patterns:
            print(f"  room class patterns: {set(room_patterns)}")

        # building_id / room_id in HTML
        bid_patterns = re.findall(r'building_id["\s=:]+(\d+)', html)
        if bid_patterns:
            print(f"\n  building_id values in HTML: {list(set(bid_patterns))[:5]}")

        rid_patterns = re.findall(r'room_id["\s=:]+(\d+)', html)
        if rid_patterns:
            print(f"  room_id values in HTML: {list(set(rid_patterns))[:5]}")

        # JavaScript navigation functions
        js_nav_patterns = re.findall(
            r'function\s+(\w*(?:detail|building|room|open|view)\w*)\s*\(', html, re.I)
        if js_nav_patterns:
            print(f"\n  JS navigation functions: {list(set(js_nav_patterns))}")

        # 物件情報を表示するdivパターンを探す
        for sel in ["div.one_building", "div.building_info", "div[class*='building']",
                     "div[class*='estate']", "div[class*='property']", "div[class*='result']",
                     "div[class*='bukken']", "div.one_room"]:
            els = await page.query_selector_all(sel)
            if els:
                print(f"\n  {sel}: {len(els)}個")
                for j, el in enumerate(els[:2]):
                    inner = await el.evaluate("el => el.innerHTML")
                    # 短いHTML構造を表示
                    inner_short = inner[:300].replace("\n", " ")
                    print(f"    [{j}] innerHTML: {inner_short}")

        # ==== Step 9: Try direct URL for a building detail ====
        print(f"\n{'='*60}")
        print("[STEP 9] 建物詳細URL パターンを試す")
        print(f"{'='*60}")

        # 結果HTMLからbuilding_idを探す
        if bid_patterns:
            bid = bid_patterns[0]
            detail_url = f"https://www.realnetpro.com/main.php?method=estate&display=building_detail&building_id={bid}"
            print(f"  試行URL: {detail_url}")
            await page.goto(detail_url, wait_until="load", timeout=30000)
            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(SCREENSHOT_DIR / "07_building_detail.png"), full_page=True)
            print(f"  URL: {page.url}")
            print(f"  title: {await page.title()}")

            body_text = (await page.inner_text("body")).replace("\xa0", " ")

            # 建物名を探す
            print("\n  --- 建物名 ---")
            for sel in [".building_name", "#building_name", "h1", "h2",
                         ".property_name", ".tatemono_name"]:
                els = await page.query_selector_all(sel)
                for el in els[:3]:
                    t = await safe_text(el, 60)
                    if t:
                        print(f"    {sel}: [{t}]")

            # 部屋リスト
            print("\n  --- 部屋リスト ---")
            for kw in ["号室", "部屋", "101", "201", "301",
                        "募集中", "空室", "入居中", "賃料", "間取り", "面積"]:
                if kw in body_text:
                    idx = body_text.find(kw)
                    context = body_text[max(0, idx - 30):idx + 50].replace("\n", " ").strip()
                    print(f"    [{kw}]: ...{context}...")

            html = await page.content()
            (SCREENSHOT_DIR / "07_building_detail.html").write_text(html, encoding="utf-8")

            # 部屋テーブル構造を詳しく見る
            tables = await page.query_selector_all("table")
            for i, tbl in enumerate(tables):
                rows = await tbl.query_selector_all("tr")
                if len(rows) >= 2:
                    tbl_cls = await tbl.get_attribute("class") or ""
                    tbl_id = await tbl.get_attribute("id") or ""
                    # 物件情報っぽいテーブルを特定
                    tbl_text = (await safe_text(tbl, 200))
                    if any(kw in tbl_text for kw in ["号室", "賃料", "間取", "面積", "募集", "部屋"]):
                        print(f"\n  *物件テーブル* table[{i}] id={tbl_id} class={tbl_cls} rows={len(rows)}")
                        for j in range(min(5, len(rows))):
                            cells = await rows[j].query_selector_all("th, td")
                            cell_texts = []
                            for cell in cells:
                                t = await safe_text(cell, 25)
                                cell_texts.append(t)
                            print(f"      row[{j}]: {cell_texts}")

        # ==== Step 10: Room detail page ====
        print(f"\n{'='*60}")
        print("[STEP 10] 部屋詳細ページの構造")
        print(f"{'='*60}")

        if rid_patterns:
            rid = rid_patterns[0]
            room_url = f"https://www.realnetpro.com/main.php?method=estate&display=room_detail&room_id={rid}"
            print(f"  試行URL: {room_url}")
            await page.goto(room_url, wait_until="load", timeout=30000)
            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(SCREENSHOT_DIR / "08_room_detail.png"), full_page=True)
            print(f"  URL: {page.url}")

            body_text = (await page.inner_text("body")).replace("\xa0", " ")

            # ステータス表示
            print("\n  --- 部屋情報 ---")
            for kw in ["号室", "賃料", "管理費", "共益費", "敷金", "礼金",
                        "間取り", "面積", "階", "方角", "築年",
                        "募集中", "空室", "入居中", "退去予定", "即入居"]:
                if kw in body_text:
                    idx = body_text.find(kw)
                    context = body_text[max(0, idx - 20):idx + 40].replace("\n", " ").strip()
                    print(f"    [{kw}]: ...{context}...")

            html = await page.content()
            (SCREENSHOT_DIR / "08_room_detail.html").write_text(html, encoding="utf-8")

        # ==== Step 11: Explore "building_detail" display mode ====
        # Alternatively try the building view that shows all rooms
        print(f"\n{'='*60}")
        print("[STEP 11] 「全部屋を表示」モードの確認")
        print(f"{'='*60}")

        # Go back to search results and switch to "全部屋を表示"
        await page.goto(
            "https://www.realnetpro.com/main.php?method=estate&display=building",
            wait_until="load", timeout=30000)
        await page.wait_for_timeout(2000)

        # Set keyword and search
        keyword_input = page.locator("input[name='keyword']")
        await keyword_input.fill("ハイツ")
        try:
            async with page.expect_navigation(timeout=30000):
                await page.evaluate("document.getElementById('main_form').submit()")
        except Exception:
            await page.wait_for_timeout(5000)
        await page.wait_for_timeout(3000)

        # Check for "全部屋を表示" option
        all_rooms_btn = page.locator("text=全部屋を表示")
        cnt = await all_rooms_btn.count()
        print(f"  「全部屋を表示」ボタン: {cnt}個")
        if cnt > 0:
            try:
                await all_rooms_btn.first.click()
                await page.wait_for_timeout(3000)
                await page.screenshot(path=str(SCREENSHOT_DIR / "09_all_rooms.png"), full_page=True)
                print(f"  URL: {page.url}")
            except Exception as e:
                print(f"  クリックエラー: {e}")

        # Check "5部屋まで表示" vs "全部屋を表示"
        for sel_text in ["5部屋まで表示", "全部屋を表示", "建物ごと", "部屋ごと"]:
            loc = page.locator(f"text={sel_text}")
            c = await loc.count()
            if c > 0:
                print(f"  「{sel_text}」: {c}個")

        # ==== Step 12: Deep dive into result HTML structure ====
        print(f"\n{'='*60}")
        print("[STEP 12] 結果HTMLの深層解析")
        print(f"{'='*60}")

        html = await page.content()

        # Extract key CSS class names and data attributes
        class_patterns = re.findall(r'class="([^"]*(?:building|room|estate|status|vacancy|recruit|boshu)[^"]*)"', html, re.I)
        if class_patterns:
            unique_classes = set(class_patterns)
            print(f"  物件関連のCSSクラス:")
            for cls in sorted(unique_classes):
                print(f"    .{cls}")

        # data- attributes
        data_patterns = re.findall(r'(data-[a-z_-]+)=', html, re.I)
        if data_patterns:
            unique_data = set(data_patterns)
            print(f"\n  data-属性:")
            for d in sorted(unique_data):
                print(f"    {d}")

        # Status-related elements
        for kw in ["boshu", "status", "vacancy", "kuushitsu", "nyuukyo", "taikyo"]:
            els_by_class = re.findall(rf'class="[^"]*{kw}[^"]*"', html, re.I)
            if els_by_class:
                print(f"\n  {kw}関連class: {set(els_by_class)}")

        # room_status / building_status patterns
        status_els = await page.query_selector_all("[class*='status'], [class*='boshu']")
        if status_els:
            print(f"\n  ステータス要素: {len(status_els)}個")
            for el in status_els[:5]:
                cls = await el.get_attribute("class") or ""
                t = await safe_text(el, 40)
                print(f"    class={cls}: [{t}]")

        # ==== Final Summary ====
        print(f"\n{'='*60}")
        print("[FINAL] 偵察完了 - 保存ファイル一覧")
        print(f"{'='*60}")

        files = sorted(SCREENSHOT_DIR.glob("*"))
        for f in files:
            size = f.stat().st_size
            print(f"  {f.name} ({size:,} bytes)")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(recon())
