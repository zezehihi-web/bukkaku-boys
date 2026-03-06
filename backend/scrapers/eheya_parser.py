"""いい部屋ネットページ解析モジュール

いい部屋ネット（大東建託）の物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- Next.jsアプリ（__NEXT_DATA__ にJSON形式で全データ埋め込み）
- HTMLパース不要。scriptタグからJSON抽出するだけ
- httpxで取得可能（Playwright不要）

URL形式: https://www.eheya.net/detail/{PropertyId}/
"""

import re
import json
import asyncio
import subprocess
import httpx

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _curl_fetch_sync(url: str) -> str:
    result = subprocess.run(
        ["curl", "-sL", "--max-time", "30", "-H", f"User-Agent: {USER_AGENT}", url],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr.decode('utf-8', errors='replace')}")
    return result.stdout.decode("utf-8", errors="replace")


async def _fetch_html_text(url: str) -> str:
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
    except (httpx.ConnectError, httpx.ConnectTimeout):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _curl_fetch_sync, url)


def _extract_next_data(html: str) -> dict | None:
    """<script id="__NEXT_DATA__"> からJSONデータを抽出"""
    match = re.search(
        r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


async def parse_eheya_url(url: str) -> dict:
    """いい部屋ネットの物件詳細URLを解析して物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    html = await _fetch_html_text(url)
    data = _extract_next_data(html)

    if not data:
        return result

    # pageProps.property にデータが格納されている
    props = data.get("props", {}).get("pageProps", {})
    prop = props.get("property", {})
    if not prop:
        # 別構造の場合: props.pageProps直下にフィールドがある可能性
        prop = props

    # --- 物件名 ---
    for key in ["buildingName", "BuildingName", "name"]:
        val = prop.get(key, "")
        if val:
            result["property_name"] = val
            break

    # --- 住所 ---
    for key in ["address", "Address", "addressName"]:
        val = prop.get(key, "")
        if val:
            result["address"] = val
            break

    # --- 賃料 ---
    price_obj = prop.get("price", {})
    if isinstance(price_obj, dict):
        price_num = price_obj.get("number") or price_obj.get("value")
        if price_num:
            result["rent"] = f"{price_num}円"
    elif isinstance(price_obj, (int, float)):
        result["rent"] = f"{int(price_obj)}円"

    if not result["rent"]:
        for key in ["rent", "Rent", "price", "Price"]:
            val = prop.get(key)
            if val and isinstance(val, (int, float)):
                result["rent"] = f"{int(val)}円"
                break
            elif val and isinstance(val, str):
                m = re.search(r"([\d.]+)\s*万円", val)
                if m:
                    try:
                        result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
                    except ValueError:
                        result["rent"] = val
                break

    # --- 面積 ---
    for key in ["roomArea", "RoomArea", "area", "exclusiveArea"]:
        val = prop.get(key)
        if val:
            if isinstance(val, (int, float)):
                result["area"] = f"{val}㎡"
            else:
                result["area"] = str(val)
            break

    # --- 間取り ---
    for key in ["housePlan", "HousePlan", "layout", "roomLayout", "floorPlan"]:
        val = prop.get(key, "")
        if val:
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', str(val))
            result["layout"] = m.group(1) if m else str(val)
            break

    # --- 築年月 ---
    for key in ["constructionDate", "ConstructionDate", "builtOn", "builtYear"]:
        val = prop.get(key, "")
        if val:
            result["build_year"] = str(val).replace("-", "/")
            break

    # --- 賃料正規化 ---
    rent = result["rent"]
    if rent and "万円" in rent:
        m = re.search(r"([\d.]+)\s*万円", rent)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                pass

    return result
