"""CHINTAIページ解析モジュール

CHINTAIの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- div.mod_h2Box h2: 「ZOOM九段下 5階／東京都千代田区九段北１丁目の賃貸物件詳細」
- div.detail_basicInfo table th/td: 住所, 間取り, 専有面積, 築年 etc.
- span.rent: 賃料（15.2万円）
- サーバーサイドレンダリング（JSP）、SPAではない

URL形式: https://www.chintai.net/detail/bk-{PROPERTY_ID}/
"""

import re
import asyncio
import subprocess
import httpx
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _curl_fetch_sync(url: str) -> bytes:
    result = subprocess.run(
        ["curl", "-sL", "--max-time", "30", "-H", f"User-Agent: {USER_AGENT}", url],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr.decode('utf-8', errors='replace')}")
    return result.stdout


async def _fetch_html_bytes(url: str) -> bytes:
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
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _curl_fetch_sync, url)


async def parse_chintai_url(url: str) -> dict:
    """CHINTAIの物件詳細URLを解析して物件情報を抽出"""
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
    # h2: 「ZOOM九段下 5階／東京都千代田区九段北１丁目の賃貸物件詳細」
    h2 = soup.select_one("div.mod_h2Box h2")
    if h2:
        h2_text = h2.get_text(strip=True)
        h2_text = re.sub(r'の賃貸物件詳細$', '', h2_text)
        parts = h2_text.split("／", 1)
        if len(parts) == 2:
            result["property_name"] = parts[0].strip()
        # partsが1つの場合は住所のみ（物件名なし）

    # titleタグからのフォールバック
    if not result["property_name"] and soup.title:
        title_text = soup.title.get_text(strip=True)
        # 「ZOOM九段下 5階／東京都...（家賃15.2万円/1K）の賃貸物件情報 | CHINTAI」
        m = re.match(r'^(.+?)[\s　]*[／/]', title_text)
        if m:
            name = m.group(1).strip()
            if not re.match(r'^[都道府県]', name):
                result["property_name"] = name

    # --- detail_basicInfo テーブルからデータ取得 ---
    basic_info = soup.select_one("div.detail_basicInfo")
    if basic_info:
        # 賃料
        rent_span = basic_info.select_one("span.rent")
        if rent_span:
            result["rent"] = rent_span.get_text(strip=True)

        # th/td ペアから各フィールド
        for th in basic_info.select("th"):
            label = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if not td:
                continue

            if label == "住所":
                # 「地図で確認」リンクを除外
                texts = list(td.stripped_strings)
                if texts:
                    addr = texts[0]
                    addr = re.sub(r'地図.*$', '', addr).strip()
                    result["address"] = addr

            elif label == "間取り":
                bold = td.select_one("span.bold")
                if bold:
                    result["layout"] = bold.get_text(strip=True)
                else:
                    layout_text = td.get_text(strip=True)
                    m = re.match(r'(ワンルーム|\d+[LDKSR]+)', layout_text)
                    result["layout"] = m.group(1) if m else layout_text

            elif label == "専有面積":
                bold = td.select_one("span.bold")
                result["area"] = (bold or td).get_text(strip=True)

            elif label == "築年":
                bold = td.select_one("span.bold")
                result["build_year"] = (bold or td).get_text(strip=True)

    # --- 正規化 ---
    rent = result["rent"]
    if rent:
        m = re.search(r"([\d.]+)\s*万円", rent)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass

    return result
