"""Yahoo!不動産ページ解析モジュール

Yahoo!不動産の物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- SSRだが、window.__SERVER_SIDE_CONTEXT__ にJSオブジェクトで全データが埋め込まれている
- 主にJSオブジェクトを抽出してJSON化する方式（brace-counting方式）
- フォールバック: HTMLテーブル（DetailSummaryTable）からも抽出可能
- httpxで取得可能（Playwright不要）

URL形式: https://realestate.yahoo.co.jp/rent/detail/{PropertyId}/
"""

import re
import json
import asyncio
import subprocess
import httpx

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


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


def _extract_brace_block(html: str, start_pos: int) -> str | None:
    """start_posの位置にある'{' から対応する'}' までをbrace-countingで抽出"""
    if start_pos >= len(html) or html[start_pos] != '{':
        return None

    depth = 0
    in_string = False
    escape = False
    i = start_pos

    while i < len(html):
        ch = html[i]

        if escape:
            escape = False
            i += 1
            continue

        if ch == '\\' and in_string:
            escape = True
            i += 1
            continue

        if ch == '"' and not escape:
            in_string = not in_string
            i += 1
            continue

        if not in_string:
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return html[start_pos:i + 1]

        i += 1

    return None


def _extract_server_context(html: str) -> dict | None:
    """window.__SERVER_SIDE_CONTEXT__ からJSONデータを抽出（brace-counting方式）"""
    marker = "window.__SERVER_SIDE_CONTEXT__"
    idx = html.find(marker)
    if idx == -1:
        return None

    # '=' の後の '{' を見つける
    eq_idx = html.find('=', idx + len(marker))
    if eq_idx == -1:
        return None

    brace_idx = html.find('{', eq_idx)
    if brace_idx == -1:
        return None

    raw = _extract_brace_block(html, brace_idx)
    if not raw:
        return None

    # JSオブジェクト → JSON変換（未クォートのキーをクォート）
    fixed = re.sub(
        r'(?<=[{,\[])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:',
        r' "\1":',
        raw,
    )
    # undefinedをnullに
    fixed = re.sub(r'\bundefined\b', 'null', fixed)
    # 末尾カンマを除去 (,] や ,})
    fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        return None


def _extract_from_html(html: str) -> dict:
    """HTMLテーブル（DetailSummaryTable / DetailHeadingLarge）からフォールバック抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    # 物件名: <h1 class="DetailHeadingLarge__title"><span>NAME</span>
    m = re.search(r'DetailHeadingLarge__title[^>]*>\s*<span>([^<]+)</span>', html)
    if m:
        result["property_name"] = m.group(1).strip()

    # 賃料: <dd class="DetailSummary__price__rent">4.8<span ...>万円</span>
    m = re.search(r'DetailSummary__price__rent[^>]*>([\d.]+)\s*<span[^>]*>万円', html)
    if m:
        try:
            result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
        except ValueError:
            pass

    # DetailSummaryTable から各種情報を取得
    # 間取り
    m = re.search(r'<th>間取り</th>\s*<td>([^<]+)</td>', html)
    if m:
        result["layout"] = m.group(1).strip()

    # 専有面積
    m = re.search(r'<th>専有面積</th>\s*<td>([^<]+)', html)
    if m:
        result["area"] = m.group(1).strip().replace('²', '㎡')

    # 築年数
    m = re.search(r'<th>築年数</th>\s*<td>([^<]+)', html)
    if m:
        text = m.group(1).strip()
        # 「築23年（2004年03月）」→「2004/03」
        ym = re.search(r'(\d{4})年(\d{2})月', text)
        if ym:
            result["build_year"] = f"{ym.group(1)}/{ym.group(2)}"
        else:
            result["build_year"] = text

    # 所在地
    m = re.search(r'<th>所在地</th>\s*<td>([^<]+)', html)
    if m:
        result["address"] = m.group(1).strip()

    return result


async def parse_yahoo_url(url: str) -> dict:
    """Yahoo!不動産の物件詳細URLを解析して物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    html = await _fetch_html_text(url)

    # 方式1: window.__SERVER_SIDE_CONTEXT__ からの抽出（推奨）
    data = _extract_server_context(html)

    if data:
        prop = data.get("page", {}).get("property")
        # dataキーの下にpageがある場合も対応
        if not prop:
            prop = data.get("data", {}).get("page", {}).get("property")
        if prop:
            return _extract_from_ssc(prop)

    # 方式2: HTMLテーブルからのフォールバック抽出
    print("[Yahoo] SSC抽出失敗 → HTMLフォールバック")
    fallback = _extract_from_html(html)
    if fallback.get("property_name"):
        return fallback

    return result


def _extract_from_ssc(prop: dict) -> dict:
    """SSCのproperty辞書から物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    # --- 物件名 ---
    sv = prop.get("StructureView") or {}
    result["property_name"] = sv.get("BuildingName", "")

    # --- 住所 ---
    lv = prop.get("LocationView") or {}
    result["address"] = lv.get("AddressName", "")

    # --- 賃料 ---
    price = prop.get("Price")
    if price:
        result["rent"] = f"{price}円"
    elif prop.get("PriceLabel"):
        rent_text = prop["PriceLabel"]
        m = re.search(r"([\d.]+)\s*万円", rent_text)
        if m:
            try:
                result["rent"] = f"{int(float(m.group(1)) * 10000)}円"
            except ValueError:
                result["rent"] = rent_text

    # --- 面積 ---
    area_raw = prop.get("MonopolyArea")
    if area_raw:
        area_m2 = area_raw / 100
        result["area"] = f"{area_m2}㎡"
    elif prop.get("MonopolyAreaLabel"):
        result["area"] = re.sub(r'<[^>]+>', '', prop["MonopolyAreaLabel"])

    # --- 間取り ---
    dv = prop.get("DetailsView") or {}
    layout = dv.get("RoomLayoutBreakdown", "")
    if layout:
        m = re.match(r'(ワンルーム|\d+[LDKSR]+)', layout)
        result["layout"] = m.group(1) if m else layout
    elif prop.get("RoomLayoutName"):
        result["layout"] = prop["RoomLayoutName"]

    # --- 築年月 ---
    built_on = prop.get("BuiltOn", "")
    if built_on:
        result["build_year"] = built_on.replace("-", "/")

    return result
