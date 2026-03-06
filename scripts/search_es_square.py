"""残り9社の物件をいい生活スクエアで検索

itanjiに登録がなかった9社の物件名でes_squareを検索し、
どの物件がes_squareに存在するか確認する。
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.scrapers.es_square_checker import check_vacancy

# itanjiに未登録の9社 + 物件名
SEARCH_TARGETS = [
    ("京王不動産", "Coeur Blanc八幡山"),
    ("小寺商店", "ザ・パークハウス西麻布レジデンス"),
    ("六耀", "ニューハイム上板橋"),
    ("ポルンガ", "エクセリア高島平"),
    ("ポルンガ", "Okapi 亀有"),
    ("内田物産", "ロングエイト"),
    ("ドリームコネクション", "ピアース高田馬場"),
    ("まいら", "アークマーク中野鷺宮"),
    ("愛三土地建物", ""),  # 物件名なし
    ("栗原建設", "ルミエール平井"),
]


async def main():
    print("=== いい生活スクエア 管理会社検索 ===\n", flush=True)

    for company, prop_name in SEARCH_TARGETS:
        if not prop_name:
            print(f"  {company:20s} -> SKIP (物件名なし)", flush=True)
            continue

        try:
            result = await check_vacancy(prop_name)
            status = "FOUND" if result != "該当なし" else "NOT FOUND"
            print(f"  {company:20s} [{prop_name}] -> {status} ({result})", flush=True)
        except Exception as e:
            print(f"  {company:20s} [{prop_name}] -> ERROR: {e}", flush=True)

        await asyncio.sleep(2)

    print("\n=== 完了 ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
