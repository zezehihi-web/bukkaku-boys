"""未対応管理会社のierabu_bb/es_square一括検索

Phase 2監査の拡張版: 未対応上位社を ierabu_bb と es_square で
物件名検索し、ヒットする管理会社を特定する。
"""
import asyncio
import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from backend.credentials_map import get_platform_key

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")


def get_unknown_companies(limit: int = 200) -> list[dict]:
    """未対応管理会社を物件数降順で取得（代表物件名付き）"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "backend", "akikaku.db")
    db = sqlite3.connect(db_path)
    rows = db.execute("""
        SELECT management_company, COUNT(*) as cnt,
               GROUP_CONCAT(DISTINCT name) as prop_names
        FROM atbb_properties
        WHERE management_company IS NOT NULL AND management_company != ''
        GROUP BY management_company
        ORDER BY cnt DESC
    """).fetchall()
    db.close()

    result = []
    for company, cnt, prop_names_str in rows:
        # COMPANY_MAP + itanji fallback で既に対応済みならスキップ
        if get_platform_key(company):
            continue
        # 代表物件名を取得
        names = prop_names_str.split(",") if prop_names_str else []
        sample = ""
        for n in names[:10]:
            n = n.strip()
            if 3 <= len(n) <= 30:
                sample = n
                break
        if not sample and names:
            sample = names[0].strip()[:30]

        result.append({
            "company": company,
            "count": cnt,
            "sample_property": sample,
        })
        if len(result) >= limit:
            break
    return result


async def search_ierabu_bb(property_name: str) -> str:
    """いえらぶBBで物件名検索"""
    from backend.scrapers.ierabu_bb_checker import check_vacancy
    try:
        return await check_vacancy(property_name)
    except Exception as e:
        return f"ERROR:{e}"


async def search_es_square(property_name: str) -> str:
    """いい生活スクエアで物件名検索"""
    from backend.scrapers.es_square_checker import check_vacancy
    try:
        return await check_vacancy(property_name)
    except Exception as e:
        return f"ERROR:{e}"


async def main():
    start = time.time()
    print("=" * 70, flush=True)
    print("未対応管理会社 ierabu_bb / es_square 拡張検索", flush=True)
    print("=" * 70, flush=True)

    targets = get_unknown_companies(limit=200)
    # サンプル物件がある会社のみ
    targets = [t for t in targets if t["sample_property"]]
    print(f"\nテスト対象: {len(targets)}社（未対応上位・物件名あり）", flush=True)

    ierabu_found = []
    es_found = []
    errors = []

    for i, co in enumerate(targets, 1):
        prop = co["sample_property"]
        print(f"\n[{i}/{len(targets)}] {co['company'][:50]} ({co['count']}件)", flush=True)
        print(f"  物件: {prop}", flush=True)

        # ierabu_bb検索
        try:
            ib_result = await search_ierabu_bb(prop)
            ib_hit = ib_result not in ("該当なし",) and "ERROR" not in ib_result
            print(f"  ierabu_bb: {'HIT' if ib_hit else 'miss'} ({ib_result[:50]})", flush=True)
            if ib_hit:
                ierabu_found.append(co)
        except Exception as e:
            print(f"  ierabu_bb: ERROR ({e})", flush=True)
            errors.append(("ierabu_bb", co["company"], str(e)))

        await asyncio.sleep(3)

        # es_square検索
        try:
            es_result = await search_es_square(prop)
            es_hit = es_result not in ("該当なし",) and "ERROR" not in es_result
            print(f"  es_square: {'HIT' if es_hit else 'miss'} ({es_result[:50]})", flush=True)
            if es_hit:
                es_found.append(co)
        except Exception as e:
            print(f"  es_square: ERROR ({e})", flush=True)
            errors.append(("es_square", co["company"], str(e)))

        await asyncio.sleep(3)

        # 10社ごとに中間サマリ
        if i % 10 == 0:
            print(f"\n  --- 中間サマリ ({i}社完了) ---", flush=True)
            print(f"  ierabu_bb: {len(ierabu_found)}社ヒット", flush=True)
            print(f"  es_square: {len(es_found)}社ヒット", flush=True)
            print(f"  エラー: {len(errors)}件", flush=True)

    # 最終サマリ
    elapsed = int(time.time() - start)
    print(f"\n{'='*70}", flush=True)
    print(f"最終結果 ({len(targets)}社テスト / {elapsed}秒)", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"\nierabu_bb ヒット: {len(ierabu_found)}社", flush=True)
    for co in ierabu_found:
        print(f"  {co['company'][:50]:50s} {co['count']:5d}件", flush=True)
    print(f"\nes_square ヒット: {len(es_found)}社", flush=True)
    for co in es_found:
        print(f"  {co['company'][:50]:50s} {co['count']:5d}件", flush=True)
    if errors:
        print(f"\nエラー: {len(errors)}件", flush=True)
        for platform, company, err in errors[:10]:
            print(f"  [{platform}] {company[:40]}: {err[:50]}", flush=True)

    # 結果保存
    result_path = os.path.join(RESULTS_DIR, "expanded_search_results.json")
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({
            "tested": len(targets),
            "elapsed_seconds": elapsed,
            "ierabu_found": [{"company": c["company"], "count": c["count"]} for c in ierabu_found],
            "es_found": [{"company": c["company"], "count": c["count"]} for c in es_found],
            "errors": errors,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n結果保存: {result_path}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
