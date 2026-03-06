"""R2 ATBB DB同期サービス

R2に保存されたATBBスクレイピング済みDB（131,486件、144MB）をローカルにダウンロードし、
ローカルのATBBデータを最新の状態に保つ。

フロー:
  1. 起動時にR2の atbb/akikaku_meta.json でタイムスタンプを確認
  2. ローカルDBより新しければ atbb/akikaku.db をダウンロード
  3. 6時間ごとに定期チェック
"""

import os
import json
import time
import asyncio
from pathlib import Path

import boto3
from botocore.config import Config

from backend.config import DB_PATH

# R2接続設定（r2_property_lookup.py と共有）
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "heyamatch-properties")
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else ""

R2_CONFIGURED = bool(R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY)

# R2上のキー
R2_DB_KEY = "atbb/akikaku.db"
R2_META_KEY = "atbb/akikaku_meta.json"

# ローカルのメタファイル（最終同期タイムスタンプ記録）
LOCAL_META_PATH = DB_PATH.parent / "akikaku_sync_meta.json"

# 同期間隔: 6時間
SYNC_INTERVAL = 6 * 60 * 60

_sync_task: asyncio.Task | None = None


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            connect_timeout=10,
            read_timeout=120,  # 144MBダウンロードに十分な時間
            retries={"max_attempts": 2},
        ),
        region_name="auto",
    )


def _get_local_timestamp() -> str:
    """ローカルの最終同期タイムスタンプを取得"""
    if LOCAL_META_PATH.exists():
        try:
            meta = json.loads(LOCAL_META_PATH.read_text(encoding="utf-8"))
            return meta.get("last_updated", "")
        except Exception:
            pass
    return ""


def _save_local_timestamp(timestamp: str):
    """ローカルの同期タイムスタンプを保存"""
    LOCAL_META_PATH.write_text(
        json.dumps({"last_updated": timestamp, "synced_at": time.strftime("%Y-%m-%dT%H:%M:%S")}),
        encoding="utf-8",
    )


def _sync_db() -> bool:
    """R2からATBB DBをダウンロード（同期実行）

    Returns:
        True: ダウンロード実行, False: スキップ（最新）
    """
    if not R2_CONFIGURED:
        print("[R2sync] R2が設定されていません")
        return False

    s3 = _get_s3_client()

    # 1. R2のメタデータを取得
    try:
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=R2_META_KEY)
        r2_meta = json.loads(obj["Body"].read().decode("utf-8"))
        r2_timestamp = r2_meta.get("last_updated", "")
        r2_count = r2_meta.get("total_records", "?")
        print(f"[R2sync] R2メタデータ: timestamp={r2_timestamp}, records={r2_count}")
    except Exception as e:
        print(f"[R2sync] R2メタデータ取得失敗: {e}")
        # メタデータがなくてもDBの存在確認してダウンロードを試みる
        r2_timestamp = "force"
        r2_count = "?"

    # 2. ローカルと比較
    local_timestamp = _get_local_timestamp()
    if local_timestamp and local_timestamp == r2_timestamp and DB_PATH.exists():
        db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        print(f"[R2sync] 最新です（{local_timestamp}）ローカルDB: {db_size_mb:.1f}MB")
        return False

    print(f"[R2sync] 更新あり: ローカル={local_timestamp or '(なし)'} → R2={r2_timestamp}")

    # 3. DBダウンロード
    try:
        tmp_path = DB_PATH.with_suffix(".db.tmp")
        print(f"[R2sync] ダウンロード開始: {R2_DB_KEY} → {tmp_path}")
        start = time.time()

        s3.download_file(R2_BUCKET_NAME, R2_DB_KEY, str(tmp_path))

        elapsed = time.time() - start
        size_mb = tmp_path.stat().st_size / (1024 * 1024)
        print(f"[R2sync] ダウンロード完了: {size_mb:.1f}MB in {elapsed:.1f}s")

        # 4. 既存DBを置き換え
        if DB_PATH.exists():
            backup = DB_PATH.with_suffix(".db.bak")
            try:
                backup.unlink(missing_ok=True)
                DB_PATH.rename(backup)
            except Exception:
                # バックアップ失敗しても上書きは試みる
                pass

        tmp_path.rename(DB_PATH)
        _save_local_timestamp(r2_timestamp)

        print(f"[R2sync] DB同期完了: {r2_count}件, {size_mb:.1f}MB")
        return True

    except Exception as e:
        print(f"[R2sync] DBダウンロードエラー: {e}")
        # tmpファイルが残っていれば削除
        tmp_path = DB_PATH.with_suffix(".db.tmp")
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        return False


async def sync_once():
    """非同期ラッパー: バックグラウンドスレッドでDB同期を実行"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_db)


async def _periodic_sync():
    """定期同期ループ（6時間ごと）"""
    while True:
        await asyncio.sleep(SYNC_INTERVAL)
        try:
            print("[R2sync] 定期チェック開始")
            await sync_once()
        except Exception as e:
            print(f"[R2sync] 定期チェックエラー: {e}")


async def startup():
    """起動時に同期を実行し、定期チェックを開始"""
    global _sync_task

    if not R2_CONFIGURED:
        print("[R2sync] R2未設定 — スキップ")
        return

    # 初回同期
    try:
        await sync_once()
    except Exception as e:
        print(f"[R2sync] 初回同期エラー（起動は継続）: {e}")

    # 定期チェックタスク起動
    _sync_task = asyncio.create_task(_periodic_sync())
    print("[R2sync] 定期同期スケジューラー起動（6時間間隔）")


async def shutdown():
    """シャットダウン"""
    global _sync_task
    if _sync_task:
        _sync_task.cancel()
        try:
            await _sync_task
        except asyncio.CancelledError:
            pass
        _sync_task = None
    print("[R2sync] 停止完了")
