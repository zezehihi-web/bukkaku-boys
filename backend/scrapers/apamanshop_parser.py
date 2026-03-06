"""アパマンショップページ解析モジュール

アパマンショップの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- サーバーサイドレンダリング（従来型HTML）
- h1: 「物件名/住所」の形式
- th/td テーブルで各フィールド
- httpxで取得可能（Playwright不要）

URL形式: https://www.apamanshop.com/detail/{PropertyId}/
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


async def parse_apamanshop_url(url: str) -> dict:
    """アパマンショップの物件詳細URLを解析して物件情報を抽出"""
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
    # h1: 「物件名/住所」または「物件名」
    h1 = soup.select_one("h1")
    if h1:
        h1_text = h1.get_text(strip=True)
        # 「物件名/住所」パターン
        parts = re.split(r'[/／]', h1_text, maxsplit=1)
        if len(parts) >= 1:
            result["property_name"] = parts[0].strip()

    # --- th/td テーブルからデータ取得 ---
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if not td:
            continue
        val = td.get_text(strip=True)

        if "住所" in label and not result["address"]:
            # 「地図を見る」等のリンクテキストを除去
            val = re.sub(r'地図.*$', '', val).strip()
            result["address"] = val

        elif "賃料" in label or "家賃" in label:
            if not result["rent"]:
                result["rent"] = val

        elif label == "間取り" and not result["layout"]:
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', val)
            result["layout"] = m.group(1) if m else val

        elif "面積" in label and not result["area"]:
            result["area"] = val

        elif ("築年" in label or "築年月" in label or "完成年月" in label) and not result["build_year"]:
            result["build_year"] = val

    # --- 賃料の代替取得 ---
    if not result["rent"]:
        # span.rent や div.rent などの賃料要素
        for selector in ["span.rent", ".rent", ".price"]:
            rent_el = soup.select_one(selector)
            if rent_el:
                result["rent"] = rent_el.get_text(strip=True)
                break

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
