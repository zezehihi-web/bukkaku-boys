"""SUUMOから物件名が重複しない100件のURLを収集

東京の複数区からSUUMO検索結果を取得し、
JNC(建物)IDが異なるURLを100件集める。
"""
import asyncio
import re
import httpx
import json
import time
from datetime import datetime

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# 東京23区のSUUMOコード
AREAS = [
    ("13101", "千代田区"), ("13102", "中央区"), ("13103", "港区"),
    ("13104", "新宿区"), ("13105", "文京区"), ("13106", "台東区"),
    ("13107", "墨田区"), ("13108", "江東区"), ("13109", "品川区"),
    ("13110", "目黒区"), ("13111", "大田区"), ("13112", "世田谷区"),
    ("13113", "渋谷区"), ("13114", "中野区"), ("13115", "杉並区"),
    ("13116", "豊島区"), ("13117", "北区"), ("13118", "荒川区"),
    ("13119", "板橋区"), ("13120", "練馬区"), ("13121", "足立区"),
    ("13122", "葛飾区"), ("13123", "江戸川区"),
]

TARGET = 105  # 余裕を持って105件
PER_AREA = 5  # 1区あたり最大5件（23区×5=115で十分）


async def fetch_search_page(client: httpx.AsyncClient, sc: str, page: int = 1) -> str:
    """SUUMO検索結果ページを取得"""
    url = (
        f"https://suumo.jp/jj/chintai/ichiran/FR301FC001/"
        f"?ar=030&bs=040&ta=13&sc={sc}&page={page}&pc=50"
    )
    resp = await client.get(url)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def extract_urls(html: str) -> list[tuple[str, str]]:
    """検索結果HTMLからJNCのURLと物件名を抽出

    Returns:
        list of (url, jnc_id)
    """
    # jnc URL を抽出
    links = re.findall(r'href="(/chintai/jnc_(\d+)/(?:\?bc=\d+)?)"', html)
    return [(f"https://suumo.jp{path}", jnc_id) for path, jnc_id in links]


async def main():
    print(f"=== SUUMO 重複なし100物件URL収集 ===")
    print(f"開始: {datetime.now()}")

    collected = []  # (url, area_name, jnc_id)
    seen_jnc = set()

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        for sc, area_name in AREAS:
            if len(collected) >= TARGET:
                break

            print(f"\n[{area_name}] 検索中...")
            try:
                html = await fetch_search_page(client, sc, page=1)
            except Exception as e:
                print(f"  エラー: {e}")
                await asyncio.sleep(2)
                continue

            urls = extract_urls(html)
            added = 0
            for url, jnc_id in urls:
                if jnc_id not in seen_jnc and added < PER_AREA:
                    seen_jnc.add(jnc_id)
                    collected.append((url, area_name, jnc_id))
                    added += 1

            print(f"  {len(urls)}件中 {added}件追加 (累計: {len(collected)}件)")
            await asyncio.sleep(1.5)  # レート制限

    # 100件に切り詰め
    collected = collected[:100]

    print(f"\n=== 収集完了: {len(collected)}件 ===")

    # URLリストを保存
    url_list = [item[0] for item in collected]

    # rapid_test.py 用のPythonリスト形式で出力
    lines = ['TEST_URLS = [']
    current_area = None
    for url, area, jnc_id in collected:
        if area != current_area:
            lines.append(f'    # === {area} ===')
            current_area = area
        lines.append(f'    "{url}",')
    lines.append(']')

    with open("results/unique_100_urls.py", "w", encoding="utf-8") as f:
        f.write('\n'.join(lines))
    print(f"保存: results/unique_100_urls.py")

    # JSON形式でも保存
    json_data = [
        {"url": url, "area": area, "jnc_id": jnc_id}
        for url, area, jnc_id in collected
    ]
    with open("results/unique_100_urls.json", "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"保存: results/unique_100_urls.json")

    # エリア分布
    area_counts = {}
    for _, area, _ in collected:
        area_counts[area] = area_counts.get(area, 0) + 1
    print(f"\nエリア分布:")
    for area, cnt in sorted(area_counts.items(), key=lambda x: -x[1]):
        print(f"  {area}: {cnt}件")


if __name__ == "__main__":
    asyncio.run(main())
