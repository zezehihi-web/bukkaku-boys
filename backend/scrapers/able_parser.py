"""エイブルページ解析モジュール

エイブルの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- サーバーサイドレンダリング（Playwright不要）
- section.main-ttlBox h2: 物件名
- div.price-area span.price: 賃料
- th/td テーブル: 住所, 間取り, 面積, 築年 etc.

URL形式: https://www.able.co.jp/rent/{PropertyId}/
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


async def parse_able_url(url: str) -> dict:
    """エイブルの物件詳細URLを解析して物件情報を抽出"""
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
    # section.main-ttlBox h2 または h1
    for selector in ["section.main-ttlBox h2", "h1.property-name", "h1"]:
        name_el = soup.select_one(selector)
        if name_el:
            name = name_el.get_text(strip=True)
            # 「物件名 | エイブル」のような suffix を除去
            name = re.sub(r'\s*[|｜]\s*エイブル.*$', '', name)
            # 「の賃貸物件情報」を除去
            name = re.sub(r'の賃貸.*$', '', name)
            if name and not re.match(r'^エイブル', name):
                result["property_name"] = name.strip()
                break

    # --- 賃料 ---
    # div.price-area span.price
    for selector in ["div.price-area span.price", "span.price", ".rent-price", ".price"]:
        price_el = soup.select_one(selector)
        if price_el:
            result["rent"] = price_el.get_text(strip=True)
            break

    # --- th/td テーブルからデータ取得 ---
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if not td:
            continue
        val = td.get_text(strip=True)

        if ("住所" in label or "所在地" in label) and not result["address"]:
            val = re.sub(r'地図.*$', '', val).strip()
            result["address"] = val

        elif label == "間取り" and not result["layout"]:
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', val)
            result["layout"] = m.group(1) if m else val

        elif ("面積" in label or "専有面積" in label) and not result["area"]:
            result["area"] = val

        elif ("築年" in label or "完成年月" in label) and not result["build_year"]:
            result["build_year"] = val

        elif ("賃料" in label or "家賃" in label) and not result["rent"]:
            result["rent"] = val

    # --- dl/dt/dd パターン（エイブルの一部ページ） ---
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True)
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        val = dd.get_text(strip=True)

        if ("住所" in label or "所在地" in label) and not result["address"]:
            result["address"] = val
        elif label == "間取り" and not result["layout"]:
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', val)
            result["layout"] = m.group(1) if m else val
        elif "面積" in label and not result["area"]:
            result["area"] = val
        elif "築年" in label and not result["build_year"]:
            result["build_year"] = val

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
