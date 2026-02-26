"""HOMESページ解析モジュール

LIFULL HOME'Sの物件詳細ページから物件名・住所・賃料・面積・間取りを取得する。
"""

import re
import httpx
from bs4 import BeautifulSoup


async def parse_homes_url(url: str) -> dict:
    """HOMESの物件詳細URLを解析して物件情報を抽出

    Returns:
        dict with keys: property_name, address, rent, area, layout
    """
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
    }

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        },
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # --- 物件名 ---
    for sel in [
        "h1.heading--b1",
        "h1[itemprop='name']",
        ".bukkenName",
        "h1",
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get_text(strip=True):
            result["property_name"] = tag.get_text(strip=True)
            break

    # --- テーブルから詳細情報を抽出 ---
    table_data = {}
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if td:
            table_data[label] = td.get_text(strip=True)

    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True)
        dd = dt.find_next_sibling("dd")
        if dd:
            table_data[label] = dd.get_text(strip=True)

    # 住所
    for key in ["所在地", "住所"]:
        if key in table_data:
            result["address"] = table_data[key]
            break
    if not result["address"]:
        addr_el = soup.select_one("[itemprop='address']")
        if addr_el:
            result["address"] = addr_el.get_text(strip=True)

    # 賃料
    for key in ["賃料", "家賃"]:
        if key in table_data:
            result["rent"] = table_data[key]
            break
    if not result["rent"]:
        rent_el = soup.select_one(".priceLabel")
        if rent_el:
            result["rent"] = rent_el.get_text(strip=True)

    # 面積
    for key in ["専有面積", "面積"]:
        if key in table_data:
            result["area"] = table_data[key]
            break

    # 間取り
    for key in ["間取り"]:
        if key in table_data:
            result["layout"] = table_data[key]
            break

    # 賃料の正規化
    rent = result["rent"]
    if rent:
        m = re.search(r"([\d.]+)\s*万円", rent)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass

    return result
