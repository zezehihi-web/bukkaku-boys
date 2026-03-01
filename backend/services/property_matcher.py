"""ATBB物件マッチングエンジン

マッチング優先順位:
1. 物件名が一致 → 即確定（ベスト）
2. 住所（丁目レベル）+ 専有面積 → 98%の精度
3. 築年数で追加検証 → ほぼ100%で特定
"""

import json
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

from backend.config import ATBB_JSON_PATH

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


def load_atbb_data() -> list[dict]:
    """ATBBデータベースを読み込み"""
    path = Path(ATBB_JSON_PATH)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def match_property(
    property_name: str,
    address: str,
    rent: str,
    area: str,
    layout: str,
    build_year_text: str = "",
) -> dict | None:
    """物件情報をATBBデータベースから検索

    マッチング戦略:
    1. 物件名マッチ → 即確定
    2. 住所（丁目レベル）+ 専有面積 → 候補絞り込み
    3. 築年数で追加検証 → 確定

    Returns:
        マッチしたATBBレコード or None
    """
    atbb_data = load_atbb_data()
    if not atbb_data:
        return None

    target_name = _clean_property_name(property_name)
    target_district = _extract_address_district(address)
    target_area = _extract_area_m2(area)
    target_build_year = _extract_build_year(build_year_text)

    # === 戦略1: 物件名マッチ ===
    if target_name:
        best_name_match = None
        best_name_score = 0.0

        for prop in atbb_data:
            atbb_name = _clean_property_name(prop.get("名前", ""))
            if not atbb_name:
                continue

            # 完全一致
            if target_name == atbb_name:
                return prop

            # 高い類似度
            sim = _similarity(target_name, atbb_name)
            if sim >= 0.85 and sim > best_name_score:
                best_name_score = sim
                best_name_match = prop

            # 部分一致（短い方が長い方に含まれる）
            elif len(target_name) >= 4 and len(atbb_name) >= 4:
                if target_name in atbb_name or atbb_name in target_name:
                    inclusion_score = min(len(target_name), len(atbb_name)) / max(len(target_name), len(atbb_name))
                    if inclusion_score >= 0.5 and inclusion_score > best_name_score:
                        best_name_score = inclusion_score
                        best_name_match = prop

        if best_name_match and best_name_score >= 0.85:
            return best_name_match

    # === 戦略2: 住所（丁目）+ 専有面積 ===
    if target_district and target_area is not None:
        candidates = []

        for prop in atbb_data:
            atbb_district = _extract_address_district(prop.get("所在地", ""))
            if not atbb_district:
                continue

            # 丁目レベルで一致確認
            if target_district != atbb_district:
                continue

            # 同じ丁目の物件を発見 → 面積で絞り込み
            atbb_area = _extract_area_m2(prop.get("専有面積", ""))
            if atbb_area is None:
                continue

            area_diff = abs(target_area - atbb_area)
            if area_diff <= 1.0:  # ±1㎡以内
                candidates.append((prop, area_diff))

        if len(candidates) == 1:
            # 候補が1件 → 確定
            return candidates[0][0]

        if len(candidates) > 1:
            # 候補が複数 → 築年数で絞り込み
            if target_build_year is not None:
                for prop, area_diff in candidates:
                    atbb_build_year = _extract_build_year(prop.get("築年月", ""))
                    if atbb_build_year and abs(target_build_year - atbb_build_year) <= 1:
                        return prop

            # 築年数で絞れなかった場合 → 面積が最も近いものを返す
            candidates.sort(key=lambda x: x[1])
            return candidates[0][0]

    # === 戦略3: フォールバック（スコアリング） ===
    best_fallback = None
    best_fallback_score = 0.0

    for prop in atbb_data:
        score = 0.0
        atbb_name = _clean_property_name(prop.get("名前", ""))

        # 名前の類似度
        if target_name and atbb_name:
            name_sim = _similarity(target_name, atbb_name)
            score += 50.0 * name_sim

        # 住所の類似度
        atbb_district = _extract_address_district(prop.get("所在地", ""))
        if target_district and atbb_district:
            if target_district == atbb_district:
                score += 25.0
            elif _similarity(target_district, atbb_district) >= 0.7:
                score += 15.0

        # 面積
        if target_area is not None:
            atbb_area = _extract_area_m2(prop.get("専有面積", ""))
            if atbb_area is not None and abs(target_area - atbb_area) <= 1.0:
                score += 10.0

        # 築年数
        if target_build_year is not None:
            atbb_build_year = _extract_build_year(prop.get("築年月", ""))
            if atbb_build_year and abs(target_build_year - atbb_build_year) <= 1:
                score += 10.0

        if score > best_fallback_score:
            best_fallback_score = score
            best_fallback = prop

    # 最低35点（物件名の類似度0.7相当 or 住所+面積+築年数）
    if best_fallback_score >= 35.0:
        return best_fallback

    return None
