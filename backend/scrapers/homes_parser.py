"""HOMESページ解析モジュール

LIFULL HOME'Sの物件詳細ページから物件名・住所・賃料・面積・間取り・築年数を取得する。

実DOM構造（2026年2月確認）:
- h1（2つ目）: 物件名「ルカセレーノ（2階/203/ワンルーム/14.17m²）」
- dt/dd ペア: 賃料, 所在地, 築年月, 専有面積, 間取り etc.
- th/td は使われていない（ヘッダーナビのみ）
- 所在地に「地図を見る」が付くので除去必要
- 築年月は「2018年6月(築8年)」形式

WAF対策（2026年3月追加）:
- HOMES (CloudFront) は連続アクセスでAWS WAFチャレンジを返す
  - HTTP 202 + Content-Length: 0 + x-amzn-waf-action: challenge
- 対策: リトライ（指数バックオフ）→ Playwrightフォールバック
"""

import re
import asyncio
import logging
import subprocess
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# WAF検出の最小コンテンツサイズ（WAFチャレンジ応答は通常0バイトまたは非常に小さい）
_MIN_CONTENT_SIZE = 1000

# リトライ設定
_MAX_RETRIES = 3
_BASE_DELAY = 5  # 初回リトライ待機（秒）


class WAFBlockedError(Exception):
    """AWS WAFによるブロックを検出した場合のエラー"""
    pass


def _is_waf_blocked(status_code: int, content: bytes, headers: dict = None) -> bool:
    """レスポンスがWAFブロックかどうかを判定

    AWS WAFチャレンジの特徴:
    - HTTP 202 Accepted + Content-Length: 0
    - x-amzn-waf-action: challenge ヘッダー
    - コンテンツが空または極端に小さい
    """
    # ヘッダーにWAFアクション明示
    if headers:
        waf_action = headers.get("x-amzn-waf-action", "")
        if waf_action:
            return True

    # HTTP 202 + 空コンテンツ（WAFチャレンジの典型パターン）
    if status_code == 202 and len(content) == 0:
        return True

    # コンテンツが極端に小さい（通常の物件ページは100KB以上）
    if len(content) < _MIN_CONTENT_SIZE and status_code != 404:
        # 本当に小さなHTMLでないか確認（タイトルすらない = WAF）
        if b"<title" not in content.lower():
            return True

    return False


def _curl_fetch_sync(url: str) -> tuple[bytes, int]:
    """curlで同期的にHTMLをバイト列取得

    Returns:
        (content_bytes, http_status_code)
    """
    result = subprocess.run(
        [
            "curl", "-sL", "--max-time", "30",
            "-H", f"User-Agent: {USER_AGENT}",
            "-w", "\n%{http_code}",
            url,
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr.decode('utf-8', errors='replace')}")

    # 最後の行がHTTPステータスコード
    stdout = result.stdout
    last_newline = stdout.rfind(b"\n")
    if last_newline >= 0:
        status_str = stdout[last_newline + 1:].strip()
        content = stdout[:last_newline]
        try:
            status_code = int(status_str)
        except ValueError:
            status_code = 200
            content = stdout
    else:
        status_code = 200
        content = stdout

    return content, status_code


async def _fetch_html_bytes(url: str) -> bytes:
    """URLからHTMLをバイト列で取得（WAFリトライ付き）

    フロー:
    1. httpx で取得を試みる
    2. WAFブロック検出 → 指数バックオフでリトライ（最大3回）
    3. httpx接続エラー → curlフォールバック（WAFチェック付き）
    4. 全てWAFブロック → Playwrightフォールバック
    5. Playwright失敗 → WAFBlockedError送出
    """
    last_exception = None

    # --- httpx リトライ ---
    for attempt in range(_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
                timeout=30.0,
            ) as client:
                resp = await client.get(url)

                # WAFチェック
                headers_dict = dict(resp.headers)
                if _is_waf_blocked(resp.status_code, resp.content, headers_dict):
                    delay = _BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        f"HOMES WAF検出 (attempt {attempt + 1}/{_MAX_RETRIES}): "
                        f"status={resp.status_code}, size={len(resp.content)}, "
                        f"{delay}秒後リトライ"
                    )
                    await asyncio.sleep(delay)
                    continue

                resp.raise_for_status()
                return resp.content

        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            last_exception = e
            logger.warning(f"HOMES httpx接続エラー: {e}")
            break  # curlフォールバックへ

    # --- curl フォールバック ---
    try:
        loop = asyncio.get_event_loop()
        content, status_code = await loop.run_in_executor(None, _curl_fetch_sync, url)

        if _is_waf_blocked(status_code, content):
            logger.warning(
                f"HOMES WAF検出 (curl): status={status_code}, size={len(content)}"
            )
        else:
            return content
    except Exception as e:
        logger.warning(f"HOMES curlフォールバック失敗: {e}")

    # --- Playwright フォールバック ---
    logger.info("HOMES: Playwrightフォールバックを試行")
    try:
        content = await _fetch_with_playwright(url)
        if content and len(content) >= _MIN_CONTENT_SIZE:
            return content
        logger.warning(f"HOMES Playwright: コンテンツ不足 size={len(content) if content else 0}")
    except Exception as e:
        logger.warning(f"HOMES Playwrightフォールバック失敗: {e}")

    raise WAFBlockedError(
        f"HOMES WAFブロック: 全取得方法が失敗 (httpx {_MAX_RETRIES}回 + curl + Playwright)"
    )


async def _fetch_with_playwright(url: str) -> bytes:
    """Playwrightで実ブラウザ経由HTMLを取得（WAFチャレンジをJS実行で突破）

    注意: browser_manager.pyの共有ブラウザは使わず、独立した一時コンテキストを使用。
    パーサーは読み取り専用なので排他ロック不要。
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        import sys
        is_linux = sys.platform.startswith("linux")
        browser = await pw.chromium.launch(
            headless=is_linux,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        try:
            context = await browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()
            page.set_default_timeout(30000)

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # WAFチャレンジのJS実行を待つ（最大10秒）
            # WAFが解決すると通常のページに遷移する
            for _ in range(10):
                content = await page.content()
                if len(content) > _MIN_CONTENT_SIZE:
                    break
                await page.wait_for_timeout(1000)

            html_bytes = content.encode("utf-8")
            await context.close()
            return html_bytes
        finally:
            await browser.close()


def _extract_from_html(soup: BeautifulSoup) -> dict:
    """BeautifulSoupオブジェクトから物件情報を抽出"""
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

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
        # パターン1: 「【ホームズ】物件名[1LDK/...]」
        m = re.search(r'[】](.+?)\s*[\[（\[]', title)
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


def _extract_from_meta(soup: BeautifulSoup) -> dict:
    """og:title / meta descriptionからの最終フォールバック抽出

    WAFブロック後にごく一部のメタデータだけ取れるケースの救済用。
    通常は _extract_from_html で十分なため、物件名のみ抽出対象。
    """
    result = {
        "property_name": "",
        "address": "",
        "rent": "",
        "area": "",
        "layout": "",
        "build_year": "",
    }

    # og:title: 「【ホームズ】リベル文京白山[1LDK/2階/2階/59.62㎡]の賃貸マンション住宅情報」
    og_title = soup.find("meta", property="og:title")
    if og_title:
        content = og_title.get("content", "")
        m = re.search(r'[】](.+?)\s*[\[（\[]', content)
        if m:
            result["property_name"] = m.group(1).strip()

    # meta description から住所抽出: 「所在地:東京都文京区白山1丁目の物件」
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        content = meta_desc.get("content", "")
        m = re.search(r'所在地[:：](.+?)の', content)
        if m:
            result["address"] = m.group(1).strip()

    return result


async def parse_homes_url(url: str) -> dict:
    """HOMESの物件詳細URLを解析して物件情報を抽出

    WAF対策:
    - httpx → リトライ（指数バックオフ）→ curl → Playwright の3段階フォールバック
    - WAFBlockedError時はメタタグからの部分抽出を試みる
    """
    try:
        html_bytes = await _fetch_html_bytes(url)
    except WAFBlockedError:
        logger.error(f"HOMES WAFブロック: {url}")
        raise RuntimeError(
            f"HOMES WAFブロック: 連続アクセスによりCloudFront WAFにブロックされました。"
            f"しばらく時間を置いてから再試行してください。"
        )

    soup = BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")

    # メイン抽出
    result = _extract_from_html(soup)

    # 物件名が取れなかった場合、メタタグフォールバック
    if not result["property_name"]:
        meta_result = _extract_from_meta(soup)
        for key, val in meta_result.items():
            if val and not result[key]:
                result[key] = val

    return result
