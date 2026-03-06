"""R2物件インデックス検索サービス

スピ賃.comのCloudflare R2に保存されたスクレイピングDBを参照し、
物件名＋号室で検索してプラットフォームの詳細URLを直接取得する。
（読み取り専用 — R2への書き込みは一切行わない）

フロー:
  1. 個別インデックス（itanji/es_square/ierabu_bb）をR2から取得し統合キャッシュ
     - itanji_index.json (21,920件)
     - es_square_index.json (9,328件)
     - ierabu_bb_index.json (1,325件)
     → 合計 32,573件
  2. building_name + room_number でマッチング（正規化＋あいまい一致）
  3. ヒット時は detail_url と source（itanji/es_square/ierabu_bb）を返却

これにより、プラットフォーム上での検索ステップをスキップし、
詳細ページに直接アクセスして空室確認が行える。
"""

import os
import json
import time
import re
import unicodedata
from difflib import SequenceMatcher

import boto3
from botocore.config import Config

# R2接続設定（環境変数から取得）
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "heyamatch-properties")
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else ""

# インデックスキャッシュ（TTL: 5分）
_index_cache = {
    "data": None,
    "fetched_at": 0,
}
INDEX_CACHE_TTL = 300  # 5分

# R2が設定されているか
R2_CONFIGURED = bool(R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY)


def _get_s3_client():
    """S3互換クライアントを取得（タイムアウト付き）"""
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            connect_timeout=10,
            read_timeout=30,
            retries={"max_attempts": 2},
        ),
        region_name="auto",
    )


def _normalize(text: str) -> str:
    """物件名を正規化（全角→半角、スペース除去、カッコ除去等）"""
    if not text:
        return ""
    # NFKC正規化（全角英数→半角、半角カナ→全角等）
    text = unicodedata.normalize("NFKC", text)
    # 小文字化
    text = text.lower()
    # カッコと中身を除去（フリガナ等）
    text = re.sub(r'[（(][ァ-ヶー]+[）)]', '', text)
    # 特殊文字を除去
    text = re.sub(r'[\s　・\-\.\/_\(\)（）【】「」『』]', '', text)
    return text.strip()


def _normalize_room(room: str) -> str:
    """号室番号を正規化"""
    if not room:
        return ""
    room = unicodedata.normalize("NFKC", room)
    # 「号室」「号」「F」等を除去し、数字部分のみ
    room = re.sub(r'(号室|号|F|階)$', '', room.strip())
    # 先頭の0を除去
    room = room.lstrip('0') or room
    return room.strip()


INDIVIDUAL_INDEXES = [
    ("itanji_index.json", "itanji"),
    ("es_square_index.json", "es_square"),
    ("ierabu_bb_index.json", "ierabu_bb"),
]


def _parse_json_flexible(raw: str) -> list[dict]:
    """JSON配列またはJSONL形式をパース"""
    # まず標準JSON配列を試す
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []
    except json.JSONDecodeError:
        pass

    # JSONL形式（1行1オブジェクト）を試す
    items = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                items.append(obj)
            elif isinstance(obj, list):
                items.extend(obj)
        except json.JSONDecodeError:
            continue
    return items


def _fetch_index() -> list[dict]:
    """R2から個別インデックスを取得して統合（キャッシュ付き）

    3つの個別インデックス（itanji/es_square/ierabu_bb）を読み込み統合。
    合計約32,573件（旧properties_index.jsonの11,257件から約3倍）。
    """
    now = time.time()

    # キャッシュが有効ならそのまま返す
    if _index_cache["data"] is not None and (now - _index_cache["fetched_at"]) < INDEX_CACHE_TTL:
        return _index_cache["data"]

    if not R2_CONFIGURED:
        print("[R2] R2が設定されていません（環境変数を確認してください）")
        return []

    s3 = _get_s3_client()
    merged = []

    for index_key, source_name in INDIVIDUAL_INDEXES:
        try:
            obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
            raw_bytes = obj["Body"].read()

            # エンコーディング: UTF-8 → Shift-JIS フォールバック
            for enc in ("utf-8", "shift_jis", "cp932"):
                try:
                    raw = raw_bytes.decode(enc)
                    break
                except (UnicodeDecodeError, ValueError):
                    continue
            else:
                raw = raw_bytes.decode("utf-8", errors="replace")

            # フォーマット: JSON配列 → JSONL フォールバック
            items = _parse_json_flexible(raw)

            # sourceフィールドがない場合はファイル名から付与
            for item in items:
                if not item.get("source"):
                    item["source"] = source_name

            merged.extend(items)
            print(f"[R2] {index_key}: {len(items)}件取得")
        except s3.exceptions.NoSuchKey:
            print(f"[R2] {index_key}: 存在しません（スキップ）")
        except Exception as e:
            print(f"[R2] {index_key}: 取得エラー: {e}")

    if merged:
        _index_cache["data"] = merged
        _index_cache["fetched_at"] = now
        print(f"[R2] インデックス統合完了: 合計{len(merged)}件")
        return merged

    # 個別インデックスが全て失敗 → 古いキャッシュがあればそれを返す
    if _index_cache["data"] is not None:
        print("[R2] 古いキャッシュを使用します")
        return _index_cache["data"]

    # 最後の手段: 旧properties_index.jsonを試す
    try:
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key="properties_index.json")
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        _index_cache["data"] = data
        _index_cache["fetched_at"] = now
        print(f"[R2] フォールバック: properties_index.json {len(data)}件")
        return data
    except Exception as e:
        print(f"[R2] フォールバックも失敗: {e}")
        return []


def _fetch_individual_file(building_name: str, room_number: str) -> dict | None:
    """個別ファイル data/{building_name}_{room_number}.json を直接取得（インデックスにない場合のフォールバック）"""
    if not R2_CONFIGURED:
        return None

    # ファイル名パターンを試す
    candidates = []
    if room_number:
        candidates.append(f"data/{building_name}_{room_number}.json")
        # 号室フォーマットの違いを考慮
        room_norm = _normalize_room(room_number)
        if room_norm != room_number:
            candidates.append(f"data/{building_name}_{room_norm}.json")
    else:
        candidates.append(f"data/{building_name}.json")

    s3 = _get_s3_client()

    for key in candidates:
        try:
            obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
            raw = obj["Body"].read().decode("utf-8")
            data = json.loads(raw)
            print(f"[R2] 個別ファイルヒット: {key}")
            return data
        except Exception:
            continue

    return None


async def search_property(
    property_name: str,
    room_number: str = "",
    address: str = "",
) -> dict | None:
    """R2インデックスで物件を検索

    Args:
        property_name: 物件名（ATBBまたはSUUMO/HOMESから取得したもの）
        room_number: 号室番号
        address: 住所（補助的なマッチング用）

    Returns:
        マッチした物件の辞書 { detail_url, source, building_name, room_number, ... }
        見つからない場合は None
    """
    if not R2_CONFIGURED:
        return None

    name_norm = _normalize(property_name)
    room_norm = _normalize_room(room_number)

    if not name_norm:
        return None

    # 1. インデックスから検索
    index = _fetch_index()
    if not index:
        return None

    best_match = None
    best_score = 0

    for item in index:
        bname = item.get("building_name", "")
        bname_norm = _normalize(bname)

        if not bname_norm:
            continue

        # ---- 名前マッチングスコア計算 ----
        score = 0

        # 完全一致（正規化後）
        if name_norm == bname_norm:
            score = 100
        # 一方が他方を含む
        elif name_norm in bname_norm or bname_norm in name_norm:
            # 長い方を基準にした包含率
            longer = max(len(name_norm), len(bname_norm))
            shorter = min(len(name_norm), len(bname_norm))
            score = 70 + int(30 * shorter / longer)
        else:
            # あいまい一致（SequenceMatcher）
            ratio = SequenceMatcher(None, name_norm, bname_norm).ratio()
            if ratio >= 0.75:
                score = int(ratio * 80)

        if score < 60:
            continue

        # ---- 号室マッチング ----
        item_room = _normalize_room(item.get("room_number", ""))

        if room_norm and item_room:
            if room_norm == item_room:
                score += 20  # 号室完全一致ボーナス
            else:
                # 号室が違うなら大幅減点（同じ建物でも別の部屋）
                score -= 40
        elif room_norm and not item_room:
            # 号室指定があるがR2に号室なし → 微減
            score -= 5

        # ---- 住所マッチング（ボーナス） ----
        if address and item.get("address"):
            addr_norm = _normalize(address)
            item_addr_norm = _normalize(item.get("address", ""))
            if addr_norm and item_addr_norm:
                if addr_norm in item_addr_norm or item_addr_norm in addr_norm:
                    score += 10

        if score > best_score:
            best_score = score
            best_match = item

    # スコア閾値: 70以上でヒットとみなす
    if best_match and best_score >= 70:
        source = best_match.get("source", "unknown")
        detail_url = best_match.get("detail_url", "")
        bname = best_match.get("building_name", "")
        broom = best_match.get("room_number", "")

        print(f"[R2] HIT ヒット: {bname} {broom} (score={best_score}, source={source})")
        print(f"[R2]    URL: {detail_url}")

        return {
            "detail_url": detail_url,
            "source": source,  # 'itanji', 'es_square', 'ierabu_bb'
            "building_name": bname,
            "room_number": broom,
            "address": best_match.get("address", ""),
            "rent": best_match.get("rent", ""),
            "score": best_score,
        }

    # 2. インデックスにない場合、個別ファイルを直接試す
    individual = _fetch_individual_file(property_name, room_number)
    if individual and individual.get("detail_url"):
        source = individual.get("source", "unknown")
        # sourceがない場合、URLから推定
        if source == "unknown":
            url = individual.get("detail_url", "")
            if "itandi" in url:
                source = "itanji"
            elif "es-square" in url:
                source = "es_square"
            elif "ielove" in url:
                source = "ierabu_bb"

        print(f"[R2] HIT 個別ファイルヒット: {individual.get('building_name', '')} (source={source})")
        return {
            "detail_url": individual["detail_url"],
            "source": source,
            "building_name": individual.get("building_name", ""),
            "room_number": individual.get("room_number", ""),
            "address": individual.get("address", ""),
            "rent": individual.get("rent", ""),
            "score": 65,  # 個別ファイルは低めのスコア
        }

    print(f"[R2] MISS 該当なし: {property_name} {room_number}")
    return None


def invalidate_cache():
    """キャッシュを無効化（テスト用）"""
    _index_cache["data"] = None
    _index_cache["fetched_at"] = 0
