"""COMPANY_MAPの冗長エントリと欠落親会社名を特定"""
import os
import sys
import sqlite3
import unicodedata
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from backend.credentials_map import COMPANY_MAP, get_platform_key


def normalize(text):
    return unicodedata.normalize("NFKC", text).strip()


def extract_parent(company):
    """ATBB会社名から親会社名を抽出"""
    n = normalize(company)
    n = re.sub(r'\s*[\d\-]{8,}$', '', n).strip()
    n = re.sub(r'^[\(（]株[\)）]\s*', '', n)
    n = re.sub(r'^[\(（]有[\)）]\s*', '', n)
    n = re.sub(r'^[\(（]同[\)）]\s*', '', n)
    n = re.sub(r'^株式会社\s*', '', n)
    n = re.sub(r'^有限会社\s*', '', n)
    n = re.sub(r'\s*株式会社$', '', n)
    n = re.sub(r'\s*[\(（]株[\)）]$', '', n)
    n = n.split('\u3000')[0].strip()
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

    # 1. COMPANY_MAP内の冗長エントリ（親キーが既にあるのに支店キーもある）
    print("=== COMPANY_MAP内の冗長エントリ ===")
    map_keys = [(k, p, c) for k, p, c in COMPANY_MAP]
    redundant = []
    for i, (key, platform, cred) in enumerate(map_keys):
        for j, (other_key, other_platform, other_cred) in enumerate(map_keys):
            if i != j and other_key in key and len(other_key) < len(key) and platform == other_platform:
                redundant.append((key, platform, other_key))
    for key, platform, parent in sorted(set(redundant)):
        print(f"  冗長: {key:40s} ({platform}) ← 親キー「{parent}」でカバー済み")

    # 2. 未対応会社のうち、親会社名をCOMPANY_MAPに追加すれば複数支店をカバーできるケース
    print(f"\n=== 親会社名追加で複数支店カバー可能 ===")
    parent_groups = {}
    for company, cnt in rows:
        if get_platform_key(company):
            continue
        parent = extract_parent(company)
        if parent not in parent_groups:
            parent_groups[parent] = {"companies": [], "total": 0}
        parent_groups[parent]["companies"].append((company, cnt))
        parent_groups[parent]["total"] += cnt

    # 2支店以上ある親グループ
    multi_branch = {k: v for k, v in parent_groups.items() if len(v["companies"]) > 1}
    multi_branch_sorted = sorted(multi_branch.items(), key=lambda x: -x[1]["total"])

    total_recoverable = sum(v["total"] for v in multi_branch.values())
    print(f"親名追加で統合可能: {len(multi_branch)}グループ / {total_recoverable}件")
    for parent, data in multi_branch_sorted[:30]:
        n = len(data["companies"])
        print(f"  {parent:40s} {data['total']:5d}件 ({n}支店)")

    # 3. 対応済み会社で、同じ親名の未対応支店があるケース
    print(f"\n=== 対応済み親会社の未対応支店 ===")
    # 対応済みの親名を収集
    known_parents = {}
    for company, cnt in rows:
        pk = get_platform_key(company)
        if pk:
            parent = extract_parent(company)
            if parent not in known_parents:
                known_parents[parent] = pk

    # 未対応で、親名が対応済みにあるケース
    missed_branches = []
    for company, cnt in rows:
        if get_platform_key(company):
            continue
        parent = extract_parent(company)
        if parent in known_parents:
            missed_branches.append((company, cnt, parent, known_parents[parent]))

    missed_branches.sort(key=lambda x: -x[1])
    total_missed = sum(c for _, c, _, _ in missed_branches)
    print(f"対応済み親の未対応支店: {len(missed_branches)}社 / {total_missed}件")
    for company, cnt, parent, platform in missed_branches[:30]:
        print(f"  {company[:55]:55s} {cnt:5d}件 → {parent} ({platform})")


if __name__ == "__main__":
    main()
