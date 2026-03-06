"""at homeページ解析モジュール

at homeの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- JSON-LD（@graph内の Apartment + Product）で物件名・住所・賃料・面積を取得
- 間取り・築年月はJSON-LDに含まれないためHTMLテーブルから取得
- サーバーサイドレンダリング（Apache）
- User-Agent必須（ないと405エラー）

URL形式: https://www.athome.co.jp/chintai/{PROPERTY_ID}/
"""

import re
import json
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


async def parse_athome_url(url: str) -> dict:
    """at homeの物件詳細URLを解析して物件情報を抽出"""
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

    # === JSON-LDからデータ取得（物件名・住所・賃料・面積） ===
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        graph = data.get("@graph", [])
        if not graph:
            # @graphがない場合、単体のJSON-LD
            if data.get("@type") == "Apartment":
                graph = [data]
            elif data.get("@type") == "Product":
                graph = [data]
            else:
                continue

        for item in graph:
            item_type = item.get("@type", "")

            if item_type == "Apartment":
                # 物件名
                name = item.get("name", "")
                if name:
                    # 「エンブレムＫ 202 ２ＤＫ」→ 間取り部分を除去
                    name = re.sub(r'\s+[０-９0-9]*[LDKSR]+\s*$', '', name)
                    result["property_name"] = name.strip()
                # 住所
                addr = item.get("address", {})
                if isinstance(addr, dict):
                    region = addr.get("addressRegion", "")
                    locality = addr.get("addressLocality", "")
                    result["address"] = f"{region}{locality}"
                # 面積
                floor_size = item.get("floorSize", {})
                if isinstance(floor_size, dict):
                    val = floor_size.get("value", "")
                    if val:
                        result["area"] = f"{val}㎡"

            elif item_type == "Product":
                # 賃料
                offers = item.get("offers", {})
                price = offers.get("price", "")
                if price:
                    result["rent"] = f"{price}円"

    # === HTMLテーブルから間取り・築年月を取得（JSON-LDに含まれない） ===
    # H1からの物件名フォールバック
    if not result["property_name"]:
        name_span = soup.select_one("#item-detail_header h1 span.name")
        if name_span:
            name = name_span.get_text(strip=True)
            name = re.sub(r'\s+[０-９0-9]*[LDKSR]+\s*$', '', name)
            result["property_name"] = name.strip()

    # 概要テーブル（div.bukkenOverviewInfo table.dataTbl）
    overview = soup.select_one("div.mainItemInfo.bukkenOverviewInfo")
    if overview:
        for th in overview.find_all("th"):
            label = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if not td:
                continue

            if label == "間取り" and not result["layout"]:
                layout_text = td.get_text(strip=True)
                m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', layout_text)
                result["layout"] = m.group(1) if m else layout_text

            elif label == "築年月" and not result["build_year"]:
                result["build_year"] = td.get_text(strip=True)

            elif label == "住所" and not result["address"]:
                addr_span = td.select_one("span.text-with-button")
                if addr_span:
                    result["address"] = addr_span.get_text(strip=True)
                else:
                    result["address"] = td.get_text(strip=True)

            elif label == "面積" and not result["area"]:
                result["area"] = td.get_text(strip=True)

    # 賃料HTMLフォールバック
    if not result["rent"]:
        rent_span = soup.select_one("div.bukkenOverviewInfo span.rent")
        if rent_span:
            rent_text = rent_span.get_text(strip=True)
            m = re.search(r"([\d.]+)\s*万円", rent_text)
            if m:
                try:
                    result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
                except ValueError:
                    result["rent"] = rent_text

    # 賃料の正規化（万円→円）
    rent = result["rent"]
    if rent and "万円" in rent:
        m = re.search(r"([\d.]+)\s*万円", rent)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass

    return result
