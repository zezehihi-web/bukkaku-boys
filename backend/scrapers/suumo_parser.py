"""SUUMOページ解析モジュール

SUUMOの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年3月確認）:

[jnc_ ページ] /chintai/jnc_XXXXXXXXXX/?bc=XXXX
- 2026年3月時点: jnc_ は /library/ ページに301リダイレクトされる
- ?bc= パラメータがある場合、/chintai/bc_XXXX/ に書き換えて直接取得する
- bc= が無い場合はリダイレクト先の library ページをパース

[bc_ ページ] /chintai/bc_XXXXXXXXXXXX/
- h1.section_h1-header-title: 「物件名 N号室 - 不動産会社名が提供する賃貸物件情報」
- .property_data (.property_data-title + .property_data-body): 間取り, 専有面積, 築年数 etc.
- .property_view_main-emphasis: 賃料（11万円）
- .property_view_detail-text: 住所（東京都千代田区麹町２）
- th/td: 築年月, 構造, 階建 etc.

[library ページ] /library/tf_XX/sc_XXXXX/to_XXXXXXXXXX/
- jnc_ リダイレクト先。建物単位の情報（部屋単位の賃料・面積は無い）
- h1（クラス無し）: 「物件名の賃貸物件情報」
- og:title: 「物件名の賃貸物件・価格情報【SUUMO】」
- th/td テーブル: 住所, 最寄駅, 築年月, 構造, 階建 etc.
"""

import re
import asyncio
import logging
import subprocess
from urllib.parse import urlparse, parse_qs
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

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
        # Windows環境でasyncソケットが接続できない場合、スレッドプールでcurl実行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _curl_fetch_sync, url)


def _clean_parsed_name(name: str) -> str:
    """パース後の物件名から先頭のゴミ文字を除去

    SUUMOのjnc_ページ等で「・　レッドアイ」のように
    先頭に中点+全角スペースが付くケースを正規化する。
    """
    # 先頭のゴミ文字を除去（中点・ドット・ビュレット・ダッシュ・空白類）
    name = re.sub(r'^[・·•．.‐‑‒–—―\-_/／,、;；:：\s\u3000]+', '', name)
    return name.strip()


def _extract_room_from_url(url: str) -> str:
    """URLのbcパラメータ等から部屋識別情報を抽出

    jnc_ページでは同じ建物名に対して ?bc=XXXX で部屋を区別する。
    bcパラメータの値自体は部屋番号ではないが、異なるbcは異なる部屋を意味する。
    """
    m = re.search(r'[?&]bc=([^&]+)', url)
    if m:
        return m.group(1)
    return ""


def _rewrite_jnc_to_bc(url: str) -> str | None:
    """jnc_ URL に ?bc= パラメータがあれば bc_ URL に変換する。

    jnc_ ページは /library/ にリダイレクトされ、部屋単位の賃料・面積等が
    取得できなくなるため、bc= があれば直接 bc_ ページを取得する。
    例: /chintai/jnc_000105280906/?bc=100492117886
      → /chintai/bc_100492117886/
    """
    if "/jnc_" not in url:
        return None
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    bc_codes = qs.get("bc", [])
    if not bc_codes:
        return None
    bc_code = bc_codes[0]
    return f"https://suumo.jp/chintai/bc_{bc_code}/"


async def parse_suumo_url(url: str) -> dict:
    """SUUMOの物件詳細URLを解析して物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
        "room": "",  # 号室番号（抽出できた場合）
    }

    # jnc_ + ?bc= の場合、bc_ URL に書き換えて部屋単位の情報を取得
    bc_url = _rewrite_jnc_to_bc(url)
    fetch_url = bc_url if bc_url else url

    # URLからページ種別を判定
    is_jnc = "/jnc_" in url and bc_url is None  # リダイレクト先がlibraryになるケース

    html_bytes = await _fetch_html_bytes(fetch_url)
    soup = BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")

    # --- データ収集（複数ソースから統合。物件名フォールバックでも使うため先に実行） ---
    table_data = {}

    # 1) th/td テーブル（jnc_ / bc_ 共通）
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        td = th.find_next_sibling("td")
        if td:
            table_data[label] = td.get_text(strip=True)

    # 2) dt/dd ペア（jnc_ページ）
    for dt_tag in soup.find_all("dt"):
        label = dt_tag.get_text(strip=True)
        dd = dt_tag.find_next_sibling("dd")
        if dd:
            val = dd.get_text(strip=True)
            if label not in table_data:
                table_data[label] = val

    # 3) .property_data ペア（bc_ページ: .property_data-title + .property_data-body）
    for pd in soup.select(".property_data"):
        title_el = pd.select_one(".property_data-title")
        body_el = pd.select_one(".property_data-body")
        if title_el and body_el:
            label = title_el.get_text(strip=True)
            val = body_el.get_text(strip=True)
            if label not in table_data:
                table_data[label] = val

    # --- 物件名 ---
    h1 = soup.select_one("h1.section_h1-header-title")
    room_from_h1 = ""
    if h1:
        h1_text = h1.get_text(strip=True)
        # 「路線 駅 N階建 築N年」パターンは物件名ではない (jnc_ページ)
        if not re.match(r".*駅\s+\d+階建\s+築\d+年$", h1_text):
            # bc_ページ: 「物件名 4F号室 - 不動産会社が提供する賃貸物件情報」を清掃
            # ※ ー（長音符）を含めると物件名中のカタカナ長音にマッチするため除外
            name = re.sub(r'\s+[-\-–—]\s+.+(?:提供|が提供する).+$', '', h1_text)
            # 号室番号を抽出してから除去
            room_m = re.search(r'\s+(\d+F?号室)$', name)
            if room_m:
                room_from_h1 = re.sub(r'[F号室]', '', room_m.group(1))
                name = name[:room_m.start()]
            result["property_name"] = _clean_parsed_name(name)

    # --- library ページのフォールバック ---
    # jnc_ が /library/ にリダイレクトされた場合、h1にクラスがなく
    # 「物件名の賃貸物件情報」形式になっている
    if not result["property_name"]:
        # og:title から抽出:
        # パターン1: 「物件名の賃貸物件・価格情報【SUUMO】」
        # パターン2: 「物件名/東京都港区の物件情報【SUUMO】」
        og = soup.find("meta", {"property": "og:title"})
        if og and og.get("content"):
            og_text = og["content"]
            for og_pat in [
                r'^(.+?)/[^/]+の物件情報',   # 「物件名/地域の物件情報【SUUMO】」
                r'^(.+?)の賃貸物件',           # 「物件名の賃貸物件・価格情報【SUUMO】」
                r'^(.+?)の物件情報',           # 「物件名の物件情報【SUUMO】」
            ]:
                m = re.match(og_pat, og_text)
                if m:
                    name = _clean_parsed_name(m.group(1))
                    if name and len(name) > 1:
                        result["property_name"] = name
                        break
        # og:title が無い場合、h1（クラス無し）から抽出
        if not result["property_name"]:
            h1_plain = soup.find("h1")
            if h1_plain:
                h1_text = h1_plain.get_text(strip=True)
                m = re.match(r'^(.+?)の賃貸物件情報$', h1_text)
                if m:
                    result["property_name"] = _clean_parsed_name(m.group(1))

    # --- title タグフォールバック ---
    # libraryページの<title>: 「物件名/東京都港区の物件情報【SUUMO】」等
    if not result["property_name"]:
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # パターン候補（優先度順）:
            # - 「物件名/地域の物件情報【SUUMO】」→ 物件名を / の前から抽出
            # - 「物件名の賃貸物件情報」
            # - 「物件名 - SUUMO」
            for pat in [
                r'^(.+?)/[^/]+の物件情報',  # library: 「物件名/東京都港区の物件情報【SUUMO】」
                r'^(.+?)の賃貸',
                r'^(.+?)\s*[-\-–—]\s*SUUMO',
                r'^(.+?)\s*[|｜]\s*SUUMO',
                r'^【SUUMO】\s*(.+?)の',
            ]:
                m = re.match(pat, title_text)
                if m:
                    name = _clean_parsed_name(m.group(1))
                    if name and len(name) > 1:
                        result["property_name"] = name
                        logger.warning(f"SUUMO: titleタグから物件名抽出成功: {name}")
                        break

    # --- 建物名テーブルフォールバック ---
    # libraryページのth/tdに「建物名」フィールドがあるケースもある
    if not result["property_name"]:
        for key in ["建物名", "物件名", "マンション名", "アパート名"]:
            if key in table_data:
                name = _clean_parsed_name(table_data[key])
                if name:
                    result["property_name"] = name
                    break

    # --- meta description フォールバック ---
    if not result["property_name"]:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            desc = meta_desc["content"]
            # 「物件名の物件情報。SUUMO...」
            m = re.match(r'^(.+?)の(?:物件情報|賃貸)', desc)
            if m:
                name = _clean_parsed_name(m.group(1))
                if name and len(name) > 1:
                    result["property_name"] = name

    if not result["property_name"]:
        logger.warning(f"SUUMO: 物件名抽出失敗 url={fetch_url}")

    # --- 住所 ---
    for key in ["所在地", "住所"]:
        if key in table_data:
            result["address"] = table_data[key]
            break
    # bc_ページ: .property_view_detail-text に住所がある（駅・路線情報を除外）
    if not result["address"]:
        for el in soup.select(".property_view_detail-text"):
            text = el.get_text(strip=True)
            # 駅・路線情報は住所ではない
            if '駅' in text or '線/' in text or '歩' in text:
                continue
            if re.search(r'[都道府県]', text) or re.search(r'[区市町村].+\d', text):
                result["address"] = text
                break

    # --- 賃料 ---
    # jnc_: .property_view_note-emphasis / bc_: .property_view_main-emphasis
    for selector in [".property_view_note-emphasis", ".property_view_main-emphasis"]:
        rent_el = soup.select_one(selector)
        if rent_el:
            result["rent"] = rent_el.get_text(strip=True)
            break
    if not result["rent"]:
        for key in ["賃料", "家賃"]:
            if key in table_data:
                result["rent"] = table_data[key]
                break

    # --- 面積 ---
    for key in ["専有面積", "面積"]:
        if key in table_data:
            result["area"] = table_data[key]
            break

    # --- 間取り ---
    for key in ["間取り"]:
        if key in table_data:
            result["layout"] = table_data[key]
            break

    # --- 築年数 ---
    for key in ["築年月", "築年数", "完成年月"]:
        if key in table_data:
            result["build_year"] = table_data[key]
            break

    # --- 号室抽出 ---
    room = ""
    # 1) bc_ページのh1から抽出済み
    if room_from_h1:
        room = room_from_h1
    # 2) テーブルデータから号室/部屋番号を取得
    if not room:
        for key in ["号室", "部屋番号", "部屋No"]:
            if key in table_data:
                room = re.sub(r'[号室F階]', '', table_data[key]).strip()
                break
    # 3) jnc_ページ: 「階建/階」や「階」フィールドから居住階を取得
    if not room and is_jnc:
        for key in ["階建/階", "階"]:
            if key in table_data:
                # "5階建 / 3階" → "3階" (居住階)
                floor_m = re.search(r'/\s*(\d+)階', table_data[key])
                if floor_m:
                    room = floor_m.group(1) + "階"
                    break
                # "3階" (単独) — 階建でないことを確認
                floor_m = re.search(r'^(\d+)階$', table_data[key].strip())
                if floor_m:
                    room = floor_m.group(1) + "階"
                    break

    result["room"] = room

    # --- 物件名に号室を付加（下流の区別用） ---
    # vacancy_checkerが "物件名/号室" 形式で分離するため、
    # 同じ建物名の異なる部屋をURLごとに区別できるようにする
    if room and result["property_name"]:
        result["property_name"] = f"{result['property_name']}/{room}"

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

    # 物件名の最終クリーンアップ（先頭ゴミ文字除去）
    if result["property_name"]:
        if "/" in result["property_name"]:
            parts = result["property_name"].split("/", 1)
            cleaned = _clean_parsed_name(parts[0])
            result["property_name"] = f"{cleaned}/{parts[1]}" if cleaned else parts[1]
        else:
            result["property_name"] = _clean_parsed_name(result["property_name"])

    return result
