"""ATBB全管理会社のプラットフォーム対応状況監査

Phase 1: ローカル照合（高速）
 - COMPANY_MAP既登録チェック
 - イタンジBB全社リスト(41,308社)との照合

Phase 2: プラットフォーム実検索（低速・上位社のみ）
 - いい生活スクエア: 物件名検索
 - いえらぶBB: 物件名検索
"""
import asyncio
import json
import os
import re
import sqlite3
import sys
import unicodedata
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")


def normalize(s: str) -> str:
    return unicodedata.normalize("NFKC", s).strip().lower()


def load_atbb_companies(db_path: str) -> list[dict]:
    """ATBBから管理会社別の物件数と代表物件名を取得"""
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
        # 代表物件名を1つ取得
        names = prop_names_str.split(",") if prop_names_str else []
        # 短すぎず長すぎない名前を優先
        sample = ""
        for n in names[:10]:
            n = n.strip()
            if 3 <= len(n) <= 30 and not re.search(r'[（(]', n):
                sample = n
                break
        if not sample and names:
            sample = names[0].strip()[:30]

        result.append({
            "company": company,
            "count": cnt,
            "sample_property": sample,
        })
    return result


def load_company_map() -> list[tuple[str, str]]:
    """COMPANY_MAPのキーとプラットフォームを取得"""
    from backend.credentials_map import COMPANY_MAP
    return [(normalize(key), platform) for key, platform, _ in COMPANY_MAP]


def load_itanji_companies(json_path: str) -> set[str]:
    """イタンジBB全社リストを正規化して読み込み"""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {normalize(c["name"].strip()) for c in data["companies"]}


def check_company_map(company: str, cmap: list[tuple[str, str]]) -> str | None:
    """COMPANY_MAPで既知のプラットフォームを返す"""
    norm = normalize(company)
    for key, platform in cmap:
        if key in norm:
            return platform
    return None


def build_itanji_index(itanji_set: set[str]) -> tuple[set[str], list[str]]:
    """イタンジBBのトークンインデックスを構築（高速照合用）"""
    # 正規化済みの全社名をリストで持つ
    names_list = sorted(itanji_set)
    # 2-gramトークンのインデックス（近似検索用）
    token_index: dict[str, set[int]] = {}
    for idx, name in enumerate(names_list):
        for i in range(len(name) - 1):
            bigram = name[i:i+2]
            if bigram not in token_index:
                token_index[bigram] = set()
            token_index[bigram].add(idx)
    return token_index, names_list


def check_itanji_list(company: str, token_index: dict, names_list: list[str]) -> bool:
    """イタンジBB全社リストに部分一致するか（高速版）"""
    norm = normalize(company)
    # 会社名から電話番号と法人格を除去
    clean = re.sub(r'[\d\-]+$', '', norm).strip()
    clean = re.sub(r'^\(株\)|^株式会社|^\(有\)|^有限会社', '', clean).strip()
    clean = re.sub(r'　.*$', '', clean).strip()  # 支店名除去
    # スペースの前だけ（支店名除去）
    clean = clean.split()[0] if clean else clean

    if not clean or len(clean) < 2:
        return False

    # cleanの2-gramでitanji候補を絞り込み
    bigrams = [clean[i:i+2] for i in range(len(clean) - 1)]
    if not bigrams:
        return False

    # 最初のbigramで候補を取得し、残りで絞り込み
    candidates = token_index.get(bigrams[0], set()).copy()
    for bg in bigrams[1:]:
        candidates &= token_index.get(bg, set())
        if not candidates:
            return False

    # 候補に対して実際の部分一致確認
    for idx in candidates:
        itanji_name = names_list[idx]
        if clean in itanji_name:
            return True

    return False


async def search_es_square(property_name: str) -> str:
    """いい生活スクエアで物件名検索"""
    from backend.scrapers.es_square_checker import check_vacancy
    try:
        return await check_vacancy(property_name)
    except Exception as e:
        return f"ERROR:{e}"


async def search_ierabu_bb(property_name: str) -> str:
    """いえらぶBBで物件名検索"""
    from backend.scrapers.ierabu_bb_checker import check_vacancy
    try:
        return await check_vacancy(property_name)
    except Exception as e:
        return f"ERROR:{e}"


async def main():
    start = time.time()
    print("=" * 70, flush=True)
    print("ATBB全管理会社 プラットフォーム対応状況監査", flush=True)
    print("=" * 70, flush=True)

    # データ読み込み
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "backend", "akikaku.db")
    itanji_path = os.path.join(RESULTS_DIR, "itanji_companies.json")

    print("\nデータ読み込み中...", flush=True)
    atbb = load_atbb_companies(db_path)
    cmap = load_company_map()
    itanji_set = load_itanji_companies(itanji_path)

    print(f"  ATBB管理会社: {len(atbb)}社 ({sum(a['count'] for a in atbb)}件)", flush=True)
    print(f"  COMPANY_MAP: {len(cmap)}エントリ", flush=True)
    print(f"  イタンジBB全社: {len(itanji_set)}社", flush=True)

    # インデックス構築
    print("  イタンジBBインデックス構築中...", flush=True)
    token_index, names_list = build_itanji_index(itanji_set)
    print(f"  インデックス: {len(token_index)}トークン", flush=True)

    # ===== Phase 1: ローカル照合 =====
    print(f"\n{'='*70}", flush=True)
    print("Phase 1: ローカル照合（COMPANY_MAP + イタンジBB全社リスト）", flush=True)
    print(f"{'='*70}", flush=True)

    mapped = []      # COMPANY_MAP登録済み
    itanji_new = []   # itanjiに存在するがCOMPANY_MAP未登録
    unknown = []      # どちらにもない

    for i, co in enumerate(atbb):
        if (i + 1) % 1000 == 0:
            print(f"  照合中... {i+1}/{len(atbb)}", flush=True)
        platform = check_company_map(co["company"], cmap)
        if platform:
            mapped.append({**co, "platform": platform})
        elif check_itanji_list(co["company"], token_index, names_list):
            itanji_new.append(co)
        else:
            unknown.append(co)

    mapped_props = sum(c["count"] for c in mapped)
    itanji_new_props = sum(c["count"] for c in itanji_new)
    unknown_props = sum(c["count"] for c in unknown)
    total_props = mapped_props + itanji_new_props + unknown_props

    print(f"\n■ COMPANY_MAP登録済み: {len(mapped)}社 ({mapped_props}件 = {mapped_props/total_props*100:.1f}%)", flush=True)
    print(f"■ イタンジBB新規発見: {len(itanji_new)}社 ({itanji_new_props}件 = {itanji_new_props/total_props*100:.1f}%)", flush=True)
    print(f"■ 未対応: {len(unknown)}社 ({unknown_props}件 = {unknown_props/total_props*100:.1f}%)", flush=True)

    # プラットフォーム別内訳
    plat_counts = {}
    for c in mapped:
        p = c["platform"]
        plat_counts[p] = plat_counts.get(p, 0) + c["count"]
    print(f"\n■ COMPANY_MAP登録済みの内訳:", flush=True)
    for p, cnt in sorted(plat_counts.items(), key=lambda x: -x[1]):
        print(f"  {p:25s}: {cnt:6d}件", flush=True)

    # イタンジ新規上位20社
    print(f"\n■ イタンジBB新規発見 上位20社（COMPANY_MAP追加推奨）:", flush=True)
    for i, co in enumerate(itanji_new[:20], 1):
        print(f"  {i:3d}. {co['company']:50s} {co['count']:5d}件", flush=True)

    # 未対応上位30社
    print(f"\n■ 未対応 上位30社:", flush=True)
    for i, co in enumerate(unknown[:30], 1):
        print(f"  {i:3d}. {co['company']:50s} {co['count']:5d}件  物件例: {co['sample_property']}", flush=True)

    # ===== Phase 2: 未対応上位社のes_square/ierabu_bb検索 =====
    print(f"\n{'='*70}", flush=True)
    print("Phase 2: 未対応上位社をes_square/ierabu_bbで検索", flush=True)
    print(f"{'='*70}", flush=True)

    # 上位20社でサンプル物件がある会社を検索
    test_targets = [co for co in unknown[:30] if co["sample_property"]][:20]

    es_found = []
    ierabu_found = []

    for i, co in enumerate(test_targets, 1):
        prop = co["sample_property"]
        print(f"\n  [{i}/{len(test_targets)}] {co['company'][:40]} ({co['count']}件)", flush=True)
        print(f"    物件: {prop}", flush=True)

        # es_square
        es_result = await search_es_square(prop)
        es_status = "FOUND" if es_result not in ("該当なし",) and "ERROR" not in es_result else "NOT FOUND"
        print(f"    es_square: {es_status} ({es_result})", flush=True)
        if es_status == "FOUND":
            es_found.append(co)

        await asyncio.sleep(3)

        # ierabu_bb
        ib_result = await search_ierabu_bb(prop)
        ib_status = "FOUND" if ib_result not in ("該当なし",) and "ERROR" not in ib_result else "NOT FOUND"
        print(f"    ierabu_bb: {ib_status} ({ib_result})", flush=True)
        if ib_status == "FOUND":
            ierabu_found.append(co)

        await asyncio.sleep(3)

    # ===== 最終サマリ =====
    print(f"\n{'='*70}", flush=True)
    print("最終サマリ", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"\n全{len(atbb)}社 / {total_props}件", flush=True)
    print(f"  COMPANY_MAP登録済: {len(mapped):5d}社 ({mapped_props:6d}件 {mapped_props/total_props*100:.1f}%)", flush=True)
    print(f"  イタンジBB追加可 : {len(itanji_new):5d}社 ({itanji_new_props:6d}件 {itanji_new_props/total_props*100:.1f}%)", flush=True)
    print(f"  es_square発見   : {len(es_found):5d}社", flush=True)
    print(f"  ierabu_bb発見   : {len(ierabu_found):5d}社", flush=True)
    print(f"  残り未対応      : {len(unknown) - len(es_found) - len(ierabu_found):5d}社 ({unknown_props:6d}件 {unknown_props/total_props*100:.1f}%)", flush=True)
    print(f"\n所要時間: {int(time.time()-start)}秒", flush=True)

    # 結果をファイル保存
    result_path = os.path.join(RESULTS_DIR, "platform_audit.json")
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_companies": len(atbb),
            "total_properties": total_props,
            "mapped": {"count": len(mapped), "properties": mapped_props},
            "itanji_new": {
                "count": len(itanji_new),
                "properties": itanji_new_props,
                "companies": [{"company": c["company"], "count": c["count"]} for c in itanji_new[:100]],
            },
            "unknown": {
                "count": len(unknown),
                "properties": unknown_props,
                "top_companies": [{"company": c["company"], "count": c["count"],
                                   "sample": c["sample_property"]} for c in unknown[:50]],
            },
            "es_square_found": [c["company"] for c in es_found],
            "ierabu_found": [c["company"] for c in ierabu_found],
        }, f, ensure_ascii=False, indent=2)
    print(f"\n結果JSON: {result_path}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
