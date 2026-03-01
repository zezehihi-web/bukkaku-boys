"""既存JSON → SQLite移行スクリプト

results/properties_database_list.json のデータを
backend/akikaku.db の atbb_properties テーブルに一括インポートする。

Usage:
    python migrate_json_to_sqlite.py
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

import aiosqlite

# パス設定
BASE_DIR = Path(__file__).resolve().parent
JSON_PATH = BASE_DIR / "results" / "properties_database_list.json"
DB_PATH = BASE_DIR / "backend" / "akikaku.db"


def make_property_key(prop: dict) -> str:
    """物件の一意キーを生成（名前|号室|所在地）"""
    name = prop.get("名前", "")
    room = prop.get("号室", "")
    addr = prop.get("所在地", "")
    return f"{name}|{room}|{addr}"


async def migrate():
    if not JSON_PATH.exists():
        print(f"❌ JSONファイルが見つかりません: {JSON_PATH}")
        sys.exit(1)

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        properties = json.load(f)

    print(f"📂 JSONデータを読み込みました: {len(properties)}件")

    if not DB_PATH.exists():
        print(f"❌ データベースが見つかりません: {DB_PATH}")
        print("   先に start.bat でサーバーを起動してテーブルを作成してください")
        sys.exit(1)

    now = datetime.now().isoformat()
    inserted = 0
    skipped = 0
    duplicates = 0

    async with aiosqlite.connect(str(DB_PATH)) as db:
        # テーブル存在確認
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='atbb_properties'"
        )
        if not await cursor.fetchone():
            print("❌ atbb_properties テーブルが存在しません")
            print("   先に start.bat でサーバーを起動してテーブルを作成してください")
            sys.exit(1)

        seen_keys = set()

        for prop in properties:
            key = make_property_key(prop)
            if not key or key == "||":
                skipped += 1
                continue

            # JSON内の重複をスキップ
            if key in seen_keys:
                duplicates += 1
                continue
            seen_keys.add(key)

            # 抽出日時があればfirst_seen/last_seenに使う
            extraction_date = prop.get("抽出日時", now)
            if not extraction_date:
                extraction_date = now

            try:
                await db.execute(
                    """INSERT OR IGNORE INTO atbb_properties (
                        property_key, name, room_number, rent, management_fee,
                        deposit, key_money, layout, area, floors, address,
                        build_year, transport, structure, transaction_type,
                        management_company, publish_date, property_id,
                        prefecture, status, first_seen, last_seen
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        key,
                        prop.get("名前", ""),
                        prop.get("号室", ""),
                        prop.get("賃料", ""),
                        prop.get("管理費等", ""),
                        prop.get("敷金", ""),
                        prop.get("礼金", ""),
                        prop.get("間取り", ""),
                        prop.get("専有面積", ""),
                        prop.get("階建/階", ""),
                        prop.get("所在地", ""),
                        prop.get("築年月", ""),
                        prop.get("交通", ""),
                        prop.get("建物構造", ""),
                        prop.get("取引態様", ""),
                        prop.get("管理会社情報", ""),
                        prop.get("公開日", ""),
                        prop.get("物件番号", ""),
                        prop.get("抽出県", ""),
                        "募集中",
                        extraction_date,
                        extraction_date,
                    ),
                )
                inserted += 1
            except Exception as e:
                print(f"  ⚠️ INSERT エラー ({key[:30]}...): {e}")
                skipped += 1

        await db.commit()

    print(f"\n✅ 移行完了!")
    print(f"   INSERT: {inserted}件")
    print(f"   重複スキップ: {duplicates}件")
    print(f"   不正データスキップ: {skipped}件")

    # 検証
    async with aiosqlite.connect(str(DB_PATH)) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM atbb_properties")
        count = (await cursor.fetchone())[0]
        print(f"   DB内レコード数: {count}件")


if __name__ == "__main__":
    asyncio.run(migrate())
