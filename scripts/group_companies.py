"""未対応管理会社を系列グループに統合して実数を把握"""
import os
import re
import sqlite3
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from backend.credentials_map import get_platform_key


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip()


def extract_group_name(company: str) -> str:
    """会社名から系列グループ名を抽出（支店名・電話番号除去）"""
    n = normalize(company)
    # 電話番号除去
    n = re.sub(r'\s*[\d\-]{8,}$', '', n).strip()
    # 法人格の正規化
    n = re.sub(r'^[\(（]株[\)）]\s*', '', n)
    n = re.sub(r'^[\(（]有[\)）]\s*', '', n)
    n = re.sub(r'^[\(（]同[\)）]\s*', '', n)
    n = re.sub(r'^株式会社\s*', '', n)
    n = re.sub(r'^有限会社\s*', '', n)
    n = re.sub(r'\s*株式会社$', '', n)
    n = re.sub(r'\s*[\(（]株[\)）]$', '', n)
    # 支店名除去（全角スペース以降）
    n = n.split('\u3000')[0].strip()
    # 半角スペース以降で支店名パターン
    parts = n.split()
    if len(parts) > 1:
        suffixes = ['店', '支店', '営業所', '事業部', '本店', '本社', '部', '室',
                    'エリア', 'センター', 'パートナー']
        if any(parts[-1].endswith(s) for s in suffixes):
            n = ' '.join(parts[:-1])
    n = n.strip()
    return n


def main():
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "backend", "akikaku.db")
    db = sqlite3.connect(db_path)
    rows = db.execute("""
        SELECT management_company, COUNT(*) as cnt
        FROM atbb_properties
        WHERE management_company IS NOT NULL AND management_company != ''
        GROUP BY management_company
        ORDER BY cnt DESC
    """).fetchall()
    db.close()

    # 全社のプラットフォーム判定
    all_groups = {}  # group_name -> {platform, companies, total_count}
    unknown_groups = {}

    for company, cnt in rows:
        platform = get_platform_key(company)
        group = extract_group_name(company)

        if not platform:
            if group not in unknown_groups:
                unknown_groups[group] = {"companies": [], "total_count": 0}
            unknown_groups[group]["companies"].append((company, cnt))
            unknown_groups[group]["total_count"] += cnt

        if group not in all_groups:
            all_groups[group] = {"platform": platform, "companies": [], "total_count": 0}
        all_groups[group]["companies"].append((company, cnt))
        all_groups[group]["total_count"] += cnt
        if platform:
            all_groups[group]["platform"] = platform

    # 統合結果
    total_unknown_companies = sum(1 for co, cnt in rows if not get_platform_key(co))
    total_unknown_groups = len(unknown_groups)

    print(f"=== 未対応会社の系列統合 ===")
    print(f"未対応会社数（個別）: {total_unknown_companies}社")
    print(f"未対応グループ数（統合後）: {total_unknown_groups}グループ")
    print()

    # 対応済みも含めた全体
    known_companies = sum(1 for co, cnt in rows if get_platform_key(co))
    known_groups = sum(1 for g, v in all_groups.items() if v["platform"])
    print(f"=== 全体 ===")
    print(f"全会社数: {len(rows)}社 → 統合後: {len(all_groups)}グループ")
    print(f"対応済み: {known_companies}社 → {known_groups}グループ")
    print(f"未対応:   {total_unknown_companies}社 → {total_unknown_groups}グループ")
    print()

    # 未対応グループ上位50（物件数順）
    sorted_groups = sorted(unknown_groups.items(), key=lambda x: -x[1]["total_count"])
    print(f"=== 未対応グループ 上位50 ===")
    for i, (group, data) in enumerate(sorted_groups[:50], 1):
        branches = len(data["companies"])
        suffix = f" ({branches}支店)" if branches > 1 else ""
        print(f"  {i:3d}. {group[:50]:50s} {data['total_count']:5d}件{suffix}")
        if branches > 1:
            for co, cnt in data["companies"][:3]:
                print(f"       └ {co[:60]:60s} {cnt:5d}件")
            if branches > 3:
                print(f"       └ ... 他{branches-3}支店")

    # 同系列で対応済みプラットフォームがある場合の統合候補
    print(f"\n=== 対応済み系列の未登録支店（統合候補） ===")
    merge_candidates = []
    for group, data in all_groups.items():
        if not data["platform"]:
            continue
        # この系列に未対応の支店があるか
        unmatched = [(co, cnt) for co, cnt in data["companies"] if not get_platform_key(co)]
        if unmatched:
            merge_candidates.append((group, data["platform"], unmatched, sum(c for _, c in unmatched)))

    merge_candidates.sort(key=lambda x: -x[3])
    total_mergeable = sum(c[3] for c in merge_candidates)
    print(f"統合候補: {len(merge_candidates)}グループ / {total_mergeable}件")
    for group, platform, unmatched, total in merge_candidates[:30]:
        print(f"  {group[:40]:40s} → {platform:20s} {total:5d}件 ({len(unmatched)}支店)")
        for co, cnt in unmatched[:3]:
            print(f"    └ {co[:60]:60s} {cnt:5d}件")


if __name__ == "__main__":
    main()
