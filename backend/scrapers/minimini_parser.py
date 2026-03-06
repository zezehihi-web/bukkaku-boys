"""ミニミニページ解析モジュール

ミニミニの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:
- サーバーサイドレンダリング（Playwright不要）
- **Shift_JIS エンコーディング**（注意）
- table.kihon_joho: 基本情報テーブル
- td.chinryo em: 賃料
- td.madori em: 間取り
- td.ensen: 路線情報
- 物件名はh1またはtitleタグから取得

URL形式: https://minimini.jp/detail/{PropertyId}/
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


def _detect_encoding(html_bytes: bytes) -> str:
    """HTMLのmetaタグからエンコーディングを検出"""
    # charset=Shift_JIS or charset=shift_jis
    head = html_bytes[:2048].decode("ascii", errors="replace").lower()
    if "shift_jis" in head or "shift-jis" in head or "sjis" in head:
        return "shift_jis"
    if "euc-jp" in head:
        return "euc-jp"
    return "utf-8"


async def parse_minimini_url(url: str) -> dict:
    """ミニミニの物件詳細URLを解析して物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    html_bytes = await _fetch_html_bytes(url)
    encoding = _detect_encoding(html_bytes)
    soup = BeautifulSoup(html_bytes, "lxml", from_encoding=encoding)

    # --- 物件名 ---
    h1 = soup.select_one("h1")
    if h1:
        name = h1.get_text(strip=True)
        # 「物件名 | ミニミニ」のような suffix を除去
        name = re.sub(r'\s*[|｜]\s*ミニミニ.*$', '', name)
        name = re.sub(r'の賃貸.*$', '', name)
        if name:
            result["property_name"] = name.strip()

    # titleタグからのフォールバック
    if not result["property_name"] and soup.title:
        title = soup.title.get_text(strip=True)
        m = re.match(r'^(.+?)\s*[|｜/／]', title)
        if m:
            result["property_name"] = m.group(1).strip()

    # --- table.kihon_joho から特殊CSSクラスで取得 ---
    kihon = soup.select_one("table.kihon_joho")
    if kihon:
        # 賃料: td.chinryo em
        chinryo = kihon.select_one("td.chinryo em")
        if chinryo:
            result["rent"] = chinryo.get_text(strip=True)

        # 間取り: td.madori em
        madori = kihon.select_one("td.madori em")
        if madori:
            layout_text = madori.get_text(strip=True)
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', layout_text)
            result["layout"] = m.group(1) if m else layout_text

    # --- 汎用th/td テーブルからデータ取得 ---
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if not td:
            continue
        val = td.get_text(strip=True)

        if ("住所" in label or "所在地" in label) and not result["address"]:
            val = re.sub(r'地図.*$', '', val).strip()
            result["address"] = val

        elif "面積" in label and not result["area"]:
            result["area"] = val

        elif ("築年" in label or "完成年月" in label) and not result["build_year"]:
            result["build_year"] = val

        elif ("賃料" in label or "家賃" in label) and not result["rent"]:
            result["rent"] = val

        elif label == "間取り" and not result["layout"]:
            m = re.match(r'(ワンルーム|[０-９0-9]+[LDKSR]+)', val)
            result["layout"] = m.group(1) if m else val

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
