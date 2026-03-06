"""goodroom（グッドルーム）ページ解析モジュール

goodroomの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- Rails + jQuery（従来型SSR、Playwright不要）
- h1.detail-page-h1: 「物件名/住所/駅/間取り - ...」スラッシュ区切り
- #basic-info table: 家賃, 管理費, 間取, 広さ etc.
- div.address: 住所
- tr.note td 備考欄: 築年月（フリーテキスト内）
- httpxで取得可能

URL形式: https://www.goodrooms.jp/{region}/detail/{category}/{property_id}/
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


async def parse_goodroom_url(url: str) -> dict:
    """goodroomの物件詳細URLを解析して物件情報を抽出"""
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
    # h1.detail-page-h1: 「物件名/住所/駅/間取り - グッドルーム」
    h1 = soup.select_one("h1.detail-page-h1")
    if h1:
        h1_text = h1.get_text(strip=True)
        # スラッシュ区切りの最初が物件名
        parts = h1_text.split("/")
        if parts:
            name = parts[0].strip()
            # 号室が含まれる場合: 「物件名203号室」→ 物件名のみ
            name = re.sub(r'\d+号室$', '', name).strip()
            result["property_name"] = name

    # --- 住所 ---
    addr_div = soup.select_one("div.address")
    if addr_div:
        result["address"] = addr_div.get_text(strip=True)

    # --- #basic-info テーブルから取得 ---
    basic_info = soup.select_one("#basic-info table")
    if basic_info:
        for tr in basic_info.find_all("tr"):
            h3 = tr.find("h3")
            td = tr.find("td")
            if not h3 or not td:
                continue
            label = h3.get_text(strip=True)
            val = td.get_text(strip=True)

            if label == "家賃" and not result["rent"]:
                result["rent"] = val

            elif label in ("間取", "間取り") and not result["layout"]:
                m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', val)
                result["layout"] = m.group(1) if m else val

            elif label == "広さ" and not result["area"]:
                result["area"] = val

    # --- 築年月（備考欄からregex） ---
    note_td = soup.select_one("tr.note td")
    if note_td:
        note_text = note_td.get_text()
        m = re.search(r'築年月[:：]?\s*(\d{4}年\d{1,2}月)', note_text)
        if m:
            result["build_year"] = m.group(1)

    # th/td フォールバック
    if not result["address"] or not result["build_year"]:
        for th in soup.find_all("th"):
            label = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if not td:
                continue
            val = td.get_text(strip=True)

            if ("住所" in label or "所在地" in label) and not result["address"]:
                result["address"] = val
            elif ("築年" in label or "完成年月" in label) and not result["build_year"]:
                result["build_year"] = val

    # --- 正規化 ---
    rent = result["rent"]
    if rent:
        # カンマ区切り: 「97,000円」→ 97000円
        rent_clean = rent.replace(",", "")
        m = re.search(r"([\d.]+)\s*万円", rent_clean)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass
        else:
            m2 = re.search(r"([\d,]+)\s*円", rent)
            if m2:
                result["rent"] = f"{m2.group(1).replace(',', '')}円"

    return result
