"""リアルネットプロ (realnetpro.com) 空室確認チェッカー

main.php の top_free 検索で物件名検索。
判定: div.one_building 内の span.st で空室ステータスを判定。
"""

from playwright.async_api import Page

from backend.config import REALPRO_URL, REALPRO_ID, REALPRO_PASS
from backend.scrapers.browser_manager import get_page

MAIN_URL = "https://www.realnetpro.com/main.php"


async def login(page: Page) -> bool:
    """リアルネットプロにログイン"""
    if not REALPRO_ID or not REALPRO_PASS:
        raise ValueError("REALPRO_ID/REALPRO_PASS が未設定です")

    await page.goto(REALPRO_URL, wait_until="load", timeout=60000)
    await page.wait_for_timeout(2000)

    await page.locator("input#login_input").fill(REALPRO_ID)
    await page.locator("input#password_input").fill(REALPRO_PASS)
    await page.locator('button:has-text("ログイン")').click()
    await page.wait_for_timeout(3000)

    return "main.php" in page.url


async def is_logged_in(page: Page) -> bool:
    """ログイン済みか確認"""
    return "realnetpro.com" in page.url and "index.php" not in page.url


async def ensure_logged_in(page: Page) -> bool:
    """ログイン状態を確認し、必要ならログイン"""
    if await is_logged_in(page):
        return True
    return await login(page)


async def check_vacancy(property_name: str, room_number: str = "") -> str:
    """リアルネットプロで物件の空室状況を確認

    top_free フィールドで建物名検索し、結果を判定。

    Returns:
        '募集中' / '申込あり' / '該当なし'
    """
    page = await get_page("realpro")
    await ensure_logged_in(page)

    # トップページへ（検索フィールドがある）
    await page.goto(MAIN_URL, wait_until="load", timeout=30000)
    await page.wait_for_timeout(2000)

    # 検索フィールドに物件名入力してEnter
    search_input = page.locator("input#top_free")
    if await search_input.count() == 0:
        raise RuntimeError("リアルネットプロ: 検索フィールド(top_free)が見つかりません")

    await search_input.fill(property_name)
    await search_input.press("Enter")
    await page.wait_for_timeout(5000)

    # 結果確認: div.one_building
    buildings = page.locator("div.one_building")
    building_count = await buildings.count()

    if building_count == 0:
        return "該当なし"

    # 部屋番号指定がある場合、特定の部屋の行を探す
    if room_number:
        room_status = await page.evaluate("""(roomNum) => {
            const buildings = document.querySelectorAll('div.one_building');
            for (const bldg of buildings) {
                const text = bldg.innerText;
                if (!text.includes(roomNum)) continue;

                // 行ごとに分割して部屋番号を含む行のステータスを確認
                const rows = bldg.querySelectorAll('tr');
                for (const row of rows) {
                    const rowText = row.innerText;
                    if (!rowText.includes(roomNum)) continue;

                    const statuses = row.querySelectorAll('span.st');
                    const statusTexts = Array.from(statuses).map(s => s.textContent.trim());
                    return {
                        found: true,
                        statuses: statusTexts,
                        row_text: rowText.substring(0, 200)
                    };
                }
            }
            return { found: false, statuses: [], row_text: '' };
        }""", room_number)

        if not room_status["found"]:
            return "該当なし"

        statuses = room_status["statuses"]
        if any(kw in s for s in statuses for kw in ["申込", "成約", "契約済"]):
            return "申込あり"
        if any(kw in s for s in statuses for kw in ["建築中"]):
            return "募集中"  # 新築建築中 = 入居前の空室
        return "募集中"

    # 部屋番号なし: 全体で判定
    all_statuses = await page.evaluate("""() => {
        const buildings = document.querySelectorAll('div.one_building');
        const result = { has_available: false, has_applied: false, total_rooms: 0 };

        for (const bldg of buildings) {
            const rows = bldg.querySelectorAll('tr');
            for (const row of rows) {
                const spans = row.querySelectorAll('span.st');
                if (spans.length === 0) continue;
                result.total_rooms++;

                const texts = Array.from(spans).map(s => s.textContent.trim());
                const hasApply = texts.some(t =>
                    t.includes('申込') || t.includes('成約') || t.includes('契約済')
                );
                if (hasApply) {
                    result.has_applied = true;
                } else {
                    result.has_available = true;
                }
            }
        }
        return result;
    }""")

    if all_statuses["has_available"]:
        return "募集中"
    if all_statuses["has_applied"]:
        return "申込あり"
    if building_count > 0:
        return "募集中"  # 物件はあるがステータス不明 → 募集中

    return "該当なし"
