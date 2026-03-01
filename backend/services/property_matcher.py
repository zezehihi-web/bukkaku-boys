"""ATBB物件マッチングエンジン（SQLite版）

マッチング優先順位:
1. 物件名が一致 → 即確定（ベスト）
2. 住所（丁目レベル）+ 専有面積 → 98%の精度
3. 築年数で追加検証 → ほぼ100%で特定
"""

import re
from datetime import datetime
from difflib import SequenceMatcher

import aiosqlite

from backend.config import DB_PATH

# 漢数字→アラビア数字
_KANJI_NUM = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
              "六": "6", "七": "7", "八": "8", "九": "9", "十": "10"}


def _normalize(text: str) -> str:
    """正規化: 全角→半角、空白除去"""
    text = text.strip()
    result = []
    for ch in text:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            result.append(chr(cp - 0xFEE0))
        elif ch == '\u3000':
            result.append(' ')
        else:
            result.append(ch)
    return "".join(result).replace(" ", "").replace("　", "")


def _extract_address_district(address: str) -> str:
    """住所を丁目レベルまで正規化して抽出

    例:
    - "東京都板橋区高島平2丁目15-10" → "板橋区高島平2"
    - "板橋区高島平 2-15-10"         → "板橋区高島平2"
    - "千代田区一番町 2-1"            → "千代田区一番町2"
    - "中央区日本橋中洲 ４－１３"     → "中央区日本橋中洲4"
    """
    if not address:
        return ""
    addr = _normalize(address)

    # 都道府県を除去
    addr = re.sub(r'^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)', '', addr)

    # 漢数字の丁目を変換: "二丁目" → "2丁目"
    for kanji, num in _KANJI_NUM.items():
        addr = addr.replace(f"{kanji}丁目", f"{num}丁目")

    # パターン1: "N丁目" の後ろを切る
    m = re.match(r'(.+?\d+)丁目', addr)
    if m:
        return m.group(1)

    # パターン2: "地名 N-M-L" → 地名+最初の数字
    m = re.match(r'(.+?)(\d+)[-ー−]', addr)
    if m:
        return m.group(1) + m.group(2)

    # パターン3: "地名 N" (数字で終わる)
    m = re.match(r'(.+?)(\d+)', addr)
    if m:
        return m.group(1) + m.group(2)

    return addr


def _extract_area_m2(text: str) -> float | None:
    """面積テキストから㎡数値を抽出"""
    if not text:
        return None
    m = re.search(r"([\d.]+)", _normalize(text))
    if m:
        return float(m.group(1))
    return None


def _extract_build_year(text: str) -> int | None:
    """築年数/築年月テキストから築年（西暦）を抽出

    対応形式:
    - ATBB: "1973/04", "2002/02"
    - SUUMO: "築48年", "築3年", "新築"
    - HOMES: "1995年3月", "2020年築"
    """
    if not text:
        return None
    text = _normalize(text)

    # "YYYY/MM" or "YYYY年" 形式（ATBB）
    m = re.search(r"((?:19|20)\d{2})", text)
    if m:
        return int(m.group(1))

    # "築N年" 形式（SUUMO/HOMES）
    m = re.search(r"築(\d+)年", text)
    if m:
        current_year = datetime.now().year
        return current_year - int(m.group(1))

    # "新築"
    if "新築" in text:
        return datetime.now().year

    return None


def _similarity(a: str, b: str) -> float:
    """2つの文字列の類似度(0-1)"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


def _clean_property_name(name: str) -> str:
    """物件名からノイズを除去（部屋番号、カッコ内読み仮名など）"""
    if not name:
        return ""
    name = _normalize(name)
    # "部屋番号：905" を除去
    name = re.sub(r'部屋番号[：:]?\s*\S+', '', name)
    # カッコ内のカタカナ読み仮名を除去
    name = re.sub(r'[（(][ァ-ヶー・]+[）)]', '', name)
    # 末尾の号室表記を除去
    name = re.sub(r'\s*\d+号室?\s*$', '', name)
    return name.strip()


def _row_to_dict(row) -> dict:
    """aiosqlite.Row → dict（ATBBフォーマット互換）"""
    return {
        "名前": row["name"] or "",
        "号室": row["room_number"] or "",
        "賃料": row["rent"] or "",
        "管理費等": row["management_fee"] or "",
        "敷金": row["deposit"] or "",
        "礼金": row["key_money"] or "",
        "間取り": row["layout"] or "",
        "専有面積": row["area"] or "",
        "階建/階": row["floors"] or "",
        "所在地": row["address"] or "",
        "築年月": row["build_year"] or "",
        "交通": row["transport"] or "",
        "建物構造": row["structure"] or "",
        "取引態様": row["transaction_type"] or "",
        "管理会社情報": row["management_company"] or "",
        "公開日": row["publish_date"] or "",
        "物件番号": row["property_id"] or "",
        "抽出県": row["prefecture"] or "",
    }


async def match_property(
    property_name: str,
    address: str,
    rent: str,
    area: str,
    layout: str,
    build_year_text: str = "",
) -> dict | None:
    """物件情報をATBBデータベース(SQLite)から検索

    マッチング戦略:
    1. 物件名マッチ → 即確定
    2. 住所（丁目レベル）+ 専有面積 → 候補絞り込み
    3. 築年数で追加検証 → 確定

    Returns:
        マッチしたATBBレコード(dict) or None
    """
    target_name = _clean_property_name(property_name)
    target_district = _extract_address_district(address)
    target_area = _extract_area_m2(area)
    target_build_year = _extract_build_year(build_year_text)

    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row

        # === 戦略1: 物件名マッチ ===
        if target_name:
            # まず完全一致を試す
            cursor = await db.execute(
                "SELECT * FROM atbb_properties WHERE status='募集中' AND name = ?",
                (target_name,)
            )
            exact = await cursor.fetchone()
            if exact:
                return _row_to_dict(exact)

            # LIKE検索で候補を取得
            cursor = await db.execute(
                "SELECT * FROM atbb_properties WHERE status='募集中' AND name LIKE ?",
                (f"%{target_name}%",)
            )
            candidates = await cursor.fetchall()

            # 逆方向のLIKE検索も実施（target_nameがATBB名に含まれるケースもカバー）
            if not candidates and len(target_name) >= 4:
                cursor = await db.execute(
                    "SELECT * FROM atbb_properties WHERE status='募集中' AND ? LIKE '%' || name || '%'",
                    (target_name,)
                )
                candidates = await cursor.fetchall()

            best_name_match = None
            best_name_score = 0.0

            for row in candidates:
                atbb_name = _clean_property_name(row["name"] or "")
                if not atbb_name:
                    continue

                if target_name == atbb_name:
                    return _row_to_dict(row)

                sim = _similarity(target_name, atbb_name)
                if sim >= 0.85 and sim > best_name_score:
                    best_name_score = sim
                    best_name_match = row

                elif len(target_name) >= 4 and len(atbb_name) >= 4:
                    if target_name in atbb_name or atbb_name in target_name:
                        inclusion_score = min(len(target_name), len(atbb_name)) / max(len(target_name), len(atbb_name))
                        if inclusion_score >= 0.5 and inclusion_score > best_name_score:
                            best_name_score = inclusion_score
                            best_name_match = row

            if best_name_match and best_name_score >= 0.85:
                return _row_to_dict(best_name_match)

        # === 戦略2: 住所（丁目）+ 専有面積 ===
        if target_district and target_area is not None:
            # 住所にtarget_districtを含む物件を検索
            cursor = await db.execute(
                "SELECT * FROM atbb_properties WHERE status='募集中' AND address LIKE ?",
                (f"%{target_district}%",)
            )
            addr_candidates = await cursor.fetchall()

            area_matches = []
            for row in addr_candidates:
                atbb_district = _extract_address_district(row["address"] or "")
                if target_district != atbb_district:
                    continue

                atbb_area = _extract_area_m2(row["area"] or "")
                if atbb_area is None:
                    continue

                area_diff = abs(target_area - atbb_area)
                if area_diff <= 1.0:
                    area_matches.append((row, area_diff))

            if len(area_matches) == 1:
                return _row_to_dict(area_matches[0][0])

            if len(area_matches) > 1:
                if target_build_year is not None:
                    for row, area_diff in area_matches:
                        atbb_build_year = _extract_build_year(row["build_year"] or "")
                        if atbb_build_year and abs(target_build_year - atbb_build_year) <= 1:
                            return _row_to_dict(row)

                area_matches.sort(key=lambda x: x[1])
                return _row_to_dict(area_matches[0][0])

        # === 戦略3: フォールバック（スコアリング） ===
        # 全件スキャンは避け、名前/住所の部分一致で候補を絞る
        fallback_candidates = []

        if target_name and len(target_name) >= 3:
            # 名前の最初の3文字で候補を絞る
            prefix = target_name[:3]
            cursor = await db.execute(
                "SELECT * FROM atbb_properties WHERE status='募集中' AND name LIKE ?",
                (f"%{prefix}%",)
            )
            fallback_candidates.extend(await cursor.fetchall())

        if target_district:
            # 住所district中のキーワードで候補追加
            cursor = await db.execute(
                "SELECT * FROM atbb_properties WHERE status='募集中' AND address LIKE ?",
                (f"%{target_district[:5]}%",)
            )
            for row in await cursor.fetchall():
                # 重複を避ける
                if not any(r["id"] == row["id"] for r in fallback_candidates):
                    fallback_candidates.append(row)

        best_fallback = None
        best_fallback_score = 0.0

        for row in fallback_candidates:
            score = 0.0
            atbb_name = _clean_property_name(row["name"] or "")

            if target_name and atbb_name:
                name_sim = _similarity(target_name, atbb_name)
                score += 50.0 * name_sim

            atbb_district = _extract_address_district(row["address"] or "")
            if target_district and atbb_district:
                if target_district == atbb_district:
                    score += 25.0
                elif _similarity(target_district, atbb_district) >= 0.7:
                    score += 15.0

            if target_area is not None:
                atbb_area = _extract_area_m2(row["area"] or "")
                if atbb_area is not None and abs(target_area - atbb_area) <= 1.0:
                    score += 10.0

            if target_build_year is not None:
                atbb_build_year = _extract_build_year(row["build_year"] or "")
                if atbb_build_year and abs(target_build_year - atbb_build_year) <= 1:
                    score += 10.0

            if score > best_fallback_score:
                best_fallback_score = score
                best_fallback = row

        if best_fallback_score >= 35.0:
            return _row_to_dict(best_fallback)

    return None
