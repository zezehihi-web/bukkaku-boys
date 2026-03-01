"""HOMESページ解析モジュール

LIFULL HOME'Sの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年2月確認）:
- h1（2つ目）: 物件名「ルカセレーノ（2階/203/ワンルーム/14.17m²）」
- dt/dd ペア: 賃料, 所在地, 築年月, 専有面積, 間取り etc.
- th/td は使われていない（ヘッダーナビのみ）
- 所在地に「地図を見る」が付くので除去必要
- 築年月は「2018年6月(築8年)」形式
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
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _curl_fetch_sync, url)


async def parse_homes_url(url: str) -> dict:
    """HOMESの物件詳細URLを解析して物件情報を抽出"""
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
    # HOMESは2つ目のh1に物件名が入る: 「ルカセレーノ（2階/203/ワンルーム/14.17m²）」
    all_h1 = soup.find_all("h1")
    for h1 in all_h1:
        text = h1.get_text(strip=True)
        # ナビ用のh1を除外（空やロゴ）
        if text and len(text) > 2 and "HOME" not in text and "LIFULL" not in text:
            # カッコ内の付加情報を除去: 「ルカセレーノ（2階/203/ワンルーム/14.17m²）」→「ルカセレーノ」
            name = re.sub(r'[（(].+[）)]$', '', text).strip()
            result["property_name"] = name
            break

    # titleタグからのフォールバック
    if not result["property_name"]:
        title = soup.title.string if soup.title else ""
        m = re.search(r'！(.+?)\s*[\[（]', title)
        if m:
            name = re.sub(r'\s*\d+階/\d+$', '', m.group(1)).strip()
            result["property_name"] = name

    # --- dt/dd からデータを取得（HOMESのメインデータソース） ---
    table_data = {}
    for dt_tag in soup.find_all("dt"):
        label = dt_tag.get_text(strip=True)
        dd = dt_tag.find_next_sibling("dd")
        if dd:
            table_data[label] = dd.get_text(strip=True)

    # th/td からも取得（フォールバック）
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if td:
            val = td.get_text(strip=True)
            if label not in table_data:
                table_data[label] = val

    # 住所（「地図を見る」を除去）
    for key in ["所在地", "住所"]:
        if key in table_data:
            addr = table_data[key]
            addr = re.sub(r'地図を見る.*$', '', addr).strip()
            result["address"] = addr
            break

    # 賃料
    for key in ["賃料", "家賃"]:
        if key in table_data:
            result["rent"] = table_data[key]
            break

    # 面積
    for key in ["専有面積", "面積"]:
        if key in table_data:
            result["area"] = table_data[key]
            break

    # 間取り（詳細部分を除去: 「ワンルーム ( 洋室 5.7帖 ... )」→「ワンルーム」）
    for key in ["間取り"]:
        if key in table_data:
            layout = table_data[key]
            # 最初の間取り名だけ取る
            m = re.match(r'(ワンルーム|\d+[LDKSR]+)', layout)
            if m:
                result["layout"] = m.group(1)
            else:
                result["layout"] = layout
            break

    # 築年数（「2018年6月(築8年)」→ そのまま保持、matcher側で解析）
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
