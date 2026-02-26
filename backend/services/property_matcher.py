"""ATBB物件マッチングエンジン

SUUMO/HOMESから取得した物件情報をATBBデータベースと照合する。
"""

import json
import re
from difflib import SequenceMatcher
from pathlib import Path

from backend.config import ATBB_JSON_PATH


def _normalize(text: str) -> str:
    """正規化: 全角→半角、空白除去"""
    text = text.strip()
    # 全角英数→半角
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


def _extract_number(text: str) -> int | None:
    """賃料・面積などから数値を抽出（円単位）"""
    if not text:
        return None
    text = _normalize(text)
    # 万円 → 円
    m = re.search(r"([\d.]+)万円", text)
    if m:
        return int(float(m.group(1)) * 10000)
    # 円
    m = re.search(r"([\d,]+)円", text)
    if m:
        return int(m.group(1).replace(",", ""))
    # 数値のみ
    m = re.search(r"([\d,]+)", text)
    if m:
        val = int(m.group(1).replace(",", ""))
        if val > 1000:
            return val
    return None


def _extract_area_m2(text: str) -> float | None:
    """面積テキストから㎡数値を抽出"""
    if not text:
        return None
    m = re.search(r"([\d.]+)", _normalize(text))
    if m:
        return float(m.group(1))
    return None


def _similarity(a: str, b: str) -> float:
    """2つの文字列の類似度(0-1)"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


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
) -> dict | None:
    """物件情報をATBBデータベースから検索

    マッチング優先順位:
    1. 物件名の完全/部分一致
    2. 住所 + 面積 + 間取り
    3. 賃料 + 面積 + 間取り

    Returns:
        マッチしたATBBレコード or None
    """
    atbb_data = load_atbb_data()
    if not atbb_data:
        return None

    target_rent = _extract_number(rent)
    target_area = _extract_area_m2(area)
    target_layout = _normalize(layout) if layout else ""
    target_name = _normalize(property_name) if property_name else ""
    target_address = _normalize(address) if address else ""

    best_match = None
    best_score = 0.0

    for prop in atbb_data:
        score = 0.0
        atbb_name = _normalize(prop.get("名前", ""))
        atbb_address = _normalize(prop.get("所在地", ""))
        atbb_layout = _normalize(prop.get("間取り", ""))
        atbb_rent = _extract_number(prop.get("賃料", ""))
        atbb_area = _extract_area_m2(prop.get("専有面積", ""))

        # 1. 物件名マッチ
        if target_name and atbb_name:
            name_sim = _similarity(target_name, atbb_name)
            if name_sim >= 0.8:
                score += 50.0 * name_sim
            elif target_name in atbb_name or atbb_name in target_name:
                score += 40.0

        # 2. 住所マッチ
        if target_address and atbb_address:
            addr_sim = _similarity(target_address, atbb_address)
            if addr_sim >= 0.7:
                score += 25.0 * addr_sim
            elif target_address in atbb_address or atbb_address in target_address:
                score += 20.0

        # 3. 間取りマッチ
        if target_layout and atbb_layout and target_layout == atbb_layout:
            score += 10.0

        # 4. 面積マッチ（±2㎡以内）
        if target_area is not None and atbb_area is not None:
            diff = abs(target_area - atbb_area)
            if diff <= 0.5:
                score += 10.0
            elif diff <= 2.0:
                score += 5.0

        # 5. 賃料マッチ（±5000円以内）
        if target_rent is not None and atbb_rent is not None:
            diff = abs(target_rent - atbb_rent)
            if diff <= 1000:
                score += 10.0
            elif diff <= 5000:
                score += 5.0

        if score > best_score:
            best_score = score
            best_match = prop

    # スコア閾値: 最低35点（物件名or住所+αのマッチが必要）
    if best_score >= 35.0:
        return best_match

    return None
