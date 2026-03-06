"""カバレッジ再計算（イタンジBBフォールバック込み）"""
import sys
import os
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from backend.credentials_map import get_platform_key

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

stats = {}
total_props = 0
total_companies = 0
unknown_top = []

for company, cnt in rows:
    total_companies += 1
    total_props += cnt
    key = get_platform_key(company)
    platform = key.split(":")[0] if key else "unknown"
    if platform not in stats:
        stats[platform] = {"companies": 0, "properties": 0}
    stats[platform]["companies"] += 1
    stats[platform]["properties"] += cnt
    if platform == "unknown" and len(unknown_top) < 50:
        unknown_top.append((company, cnt))

print(f"全{total_companies}社 / {total_props}件")
print()
for p, s in sorted(stats.items(), key=lambda x: -x[1]["properties"]):
    pct = s["properties"] / total_props * 100
    print(f"  {p:20s}: {s['companies']:5d}社  {s['properties']:6d}件  ({pct:.1f}%)")

unk = stats.get("unknown", {"companies": 0, "properties": 0})
print(f"\n自動対応率: {(1 - unk['properties']/total_props)*100:.1f}%")
print(f"  (COMPANY_MAP + イタンジ全社フォールバック)")

print(f"\n■ 未対応 上位50社:")
for i, (company, cnt) in enumerate(unknown_top, 1):
    print(f"  {i:3d}. {company:60s} {cnt:5d}件")
