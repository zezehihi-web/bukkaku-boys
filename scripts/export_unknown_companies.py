"""未対応管理会社を全件CSVで書き出し"""
import csv
import os
import sqlite3
import sys
import unicodedata
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from backend.credentials_map import get_platform_key


def extract_parent(company):
    n = unicodedata.normalize("NFKC", company).strip()
    n = re.sub(r'\s*[\d\-]{8,}$', '', n).strip()
    n = re.sub(r'^[\(（](?:株|有|同|合)[\)）]\s*', '', n)
    n = re.sub(r'^株式会社\s*', '', n)
    n = re.sub(r'^有限会社\s*', '', n)
    n = re.sub(r'\s*株式会社$', '', n)
    n = re.sub(r'\s*[\(（](?:株|有|同)[\)）]$', '', n)
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

    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "results", "unknown_companies.csv")

    count = 0
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["管理会社名", "物件数", "親会社名(推定)"])
        for company, cnt in rows:
            if get_platform_key(company):
                continue
            parent = extract_parent(company)
            writer.writerow([company, cnt, parent])
            count += 1

    print(f"{count}社を書き出しました: {out_path}")


if __name__ == "__main__":
    main()
