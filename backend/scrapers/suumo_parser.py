"""SUUMOページ解析モジュール

SUUMOの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年2月確認）:

[jnc_ ページ] /chintai/jnc_XXXXXXXXXX/
- h1.section_h1-header-title: 「路線 駅 N階建 築N年」（物件名でない場合あり）
- th/td テーブル: 所在地, 専有面積, 間取り, 築年数, 築年月 etc.
- .property_view_note-emphasis: 賃料（10.9万円）

[bc_ ページ] /chintai/bc_XXXXXXXXXXXX/
- h1.section_h1-header-title: 「物件名 N号室 - 不動産会社名が提供する賃貸物件情報」
- .property_data (.property_data-title + .property_data-body): 間取り, 専有面積, 築年数 etc.
- .property_view_main-emphasis: 賃料（11万円）
- .property_view_detail-text: 住所（東京都千代田区麹町２）
- th/td: 築年月, 構造, 階建 etc.
"""

import re
import asyncio
import subprocess
import httpx
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _curl_fetch_sync(url: str) -> bytes:
    """curlで同期的にHTMLをバイト列取得"""
    result = subprocess.run(
        ["curl", "-sL", "--max-time", "30", "-H", f"User-Agent: {USER_AGENT}", url],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr.decode('utf-8', errors='replace')}")
    return result.stdout


async def _fetch_html_bytes(url: str) -> bytes:
    """URLからHTMLをバイト列で取得（httpx→curlフォールバック）"""
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content
    except (httpx.ConnectError, httpx.ConnectTimeout):
        # Windows環境でasyncソケットが接続できない場合、スレッドプールでcurl実行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _curl_fetch_sync, url)


async def parse_suumo_url(url: str) -> dict:
    """SUUMOの物件詳細URLを解析して物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    html_bytes = await _fetch_html_bytes(url)
    soup = BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")

    # --- 物件名 ---
    h1 = soup.select_one("h1.section_h1-header-title")
    if h1:
        h1_text = h1.get_text(strip=True)
        # 「路線 駅 N階建 築N年」パターンは物件名ではない (jnc_ページ)
        if not re.match(r".*駅\s+\d+階建\s+築\d+年$", h1_text):
            # bc_ページ: 「物件名 4F号室 - 不動産会社が提供する賃貸物件情報」を清掃
            # ※ ー（長音符）を含めると物件名中のカタカナ長音にマッチするため除外
            name = re.sub(r'\s+[-\-–—]\s+.+(?:提供|が提供する).+$', '', h1_text)
            name = re.sub(r'\s+\d+F?号室$', '', name)
            result["property_name"] = name.strip()

    # --- データ収集（複数ソースから統合） ---
    table_data = {}

    # 1) th/td テーブル（jnc_ / bc_ 共通）
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if td:
            table_data[label] = td.get_text(strip=True)

    # 2) dt/dd ペア（jnc_ページ）
    for dt_tag in soup.find_all("dt"):
        label = dt_tag.get_text(strip=True)
        dd = dt_tag.find_next_sibling("dd")
        if dd:
            val = dd.get_text(strip=True)
            if label not in table_data:
                table_data[label] = val

    # 3) .property_data ペア（bc_ページ: .property_data-title + .property_data-body）
    for pd in soup.select(".property_data"):
        title_el = pd.select_one(".property_data-title")
        body_el = pd.select_one(".property_data-body")
        if title_el and body_el:
            label = title_el.get_text(strip=True)
            val = body_el.get_text(strip=True)
            if label not in table_data:
                table_data[label] = val

    # --- 住所 ---
    for key in ["所在地", "住所"]:
        if key in table_data:
            result["address"] = table_data[key]
            break
    # bc_ページ: .property_view_detail-text に住所がある（駅・路線情報を除外）
    if not result["address"]:
        for el in soup.select(".property_view_detail-text"):
            text = el.get_text(strip=True)
            # 駅・路線情報は住所ではない
            if '駅' in text or '線/' in text or '歩' in text:
                continue
            if re.search(r'[都道府県]', text) or re.search(r'[区市町村].+\d', text):
                result["address"] = text
                break

    # --- 賃料 ---
    # jnc_: .property_view_note-emphasis / bc_: .property_view_main-emphasis
    for selector in [".property_view_note-emphasis", ".property_view_main-emphasis"]:
        rent_el = soup.select_one(selector)
        if rent_el:
            result["rent"] = rent_el.get_text(strip=True)
            break
    if not result["rent"]:
        for key in ["賃料", "家賃"]:
            if key in table_data:
                result["rent"] = table_data[key]
                break

    # --- 面積 ---
    for key in ["専有面積", "面積"]:
        if key in table_data:
            result["area"] = table_data[key]
            break

    # --- 間取り ---
    for key in ["間取り"]:
        if key in table_data:
            result["layout"] = table_data[key]
            break

    # --- 築年数 ---
    for key in ["築年月", "築年数", "完成年月"]:
        if key in table_data:
            result["build_year"] = table_data[key]
            break

    # --- 正規化 ---
    # 賃料: 万円→円
    rent = result["rent"]
    if rent:
        m = re.search(r"([\d.]+)\s*万円", rent)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass

    return result
