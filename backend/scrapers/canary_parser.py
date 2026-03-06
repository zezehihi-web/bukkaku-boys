"""カナリー（Canary）ページ解析モジュール

カナリーの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- Next.js + styled-components（CSSクラスはハッシュ化で不安定）
- __NEXT_DATA__ にJSON形式で全データ埋め込み
- props.pageProps.room にデータ格納
- httpxで取得可能（Playwright不要）

URL形式: https://web.canary-app.jp/chintai/rooms/{UUID}/
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


async def parse_canary_url(url: str) -> dict:
    """カナリーの物件詳細URLを解析して物件情報を抽出"""
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

    room = data.get("props", {}).get("pageProps", {}).get("room", {})
    if not room:
        return result

    # --- 物件名 ---
    name = room.get("name", "")
    if name:
        # 「CLASSEUM飯田橋(クラシアムイイダバシ)」→ カッコ内の読み仮名を除去
        name = re.sub(r'\([ァ-ヶー]+\)$', '', name).strip()
        result["property_name"] = name

    # --- 住所 ---
    result["address"] = room.get("address", "")

    # --- 賃料 ---
    rent = room.get("rent")
    if rent and isinstance(rent, (int, float)):
        result["rent"] = f"{int(rent)}円"

    # --- 面積 ---
    area = room.get("square")
    if area:
        result["area"] = f"{area}㎡"

    # --- 間取り ---
    layout = room.get("layout", "")
    if layout:
        m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', layout)
        result["layout"] = m.group(1) if m else layout

    # --- 築年月 ---
    built_at = room.get("builtAtV2", {})
    if isinstance(built_at, dict):
        # Unixタイムスタンプの場合
        ts = built_at.get("value")
        if ts and isinstance(ts, (int, float)):
            from datetime import datetime
            dt = datetime.fromtimestamp(ts)
            result["build_year"] = f"{dt.year}年{dt.month}月"
    # 築年数（年数のみ）のフォールバック
    if not result["build_year"]:
        old = room.get("old")
        if old:
            result["build_year"] = f"築{old}年"

    return result
