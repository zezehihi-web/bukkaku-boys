"""Neon PostgreSQL ポーリングリスナー

DB伝言板パターン: Vercel (akiya-tools) が INSERT した行を検出し、
ミニPC上の Playwright で空室確認を実行、結果を Neon に書き戻す。

使い方:
  python -m backend.neon_listener
"""

import asyncio
import os
import signal
import sys
import traceback
from datetime import datetime

import psycopg2
import psycopg2.extras

# ============================================================
# Neon 接続
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "")

_conn = None


def _get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        _conn.autocommit = False
    return _conn


def _reset_conn():
    global _conn
    try:
        if _conn and not _conn.closed:
            _conn.close()
    except Exception:
        pass
    _conn = None


# ============================================================
# Neon → dict 変換（vacancy_checker が record["key"] でアクセスするため）
# ============================================================
_COLUMNS = [
    "id", "submitted_url", "portal_source",
    "property_name", "property_address", "property_rent",
    "property_area", "property_layout", "property_build_year",
    "atbb_matched", "atbb_company", "platform", "platform_auto",
    "status", "vacancy_result", "error_message",
    "created_at", "completed_at", "updated_at",
    "line_user_id", "line_notified", "batch_group",
]


def _row_to_dict(row):
    """RealDictRow → dict（キーアクセス互換）"""
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(zip(_COLUMNS, row))


# ============================================================
# vacancy_checker 用の差し替え関数
# ============================================================
async def _neon_update_status(check_id: int, **kwargs):
    """Neon DB にステータスを書き戻す"""
    if not kwargs:
        return
    sets = []
    values = []
    for key, val in kwargs.items():
        sets.append(f"{key} = %s")
        values.append(val)
    sets.append("updated_at = NOW()")
    values.append(check_id)
    sql = f"UPDATE akikaku_checks SET {', '.join(sets)} WHERE id = %s"

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


async def _neon_fetch_record(check_id: int):
    """Neon DB からレコードを取得"""
    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM akikaku_checks WHERE id = %s", (check_id,))
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


# ============================================================
# ジョブ取得（FOR UPDATE SKIP LOCKED）
# ============================================================
def _pick_job():
    """処理待ちの行を1件取得してステータスを 'running' に更新"""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM akikaku_checks
                   WHERE status IN ('pending', 'matching', 'checking')
                   ORDER BY created_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED""",
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None

            cur.execute(
                "UPDATE akikaku_checks SET status = 'running', updated_at = NOW() WHERE id = %s",
                (row["id"],),
            )
        conn.commit()
        return _row_to_dict(row)
    except Exception:
        conn.rollback()
        raise


# ============================================================
# ジョブ処理
# ============================================================
async def _process_job(job: dict):
    """取得したジョブを空室確認パイプラインに流す"""
    check_id = job["id"]
    original_status = job["status"]

    print(f"[neon_listener] ジョブ取得: id={check_id}, status={original_status}, "
          f"property={job.get('property_name', '')}")

    try:
        if original_status == "pending":
            # URL → パース → マッチング → 確認
            from backend.services.vacancy_checker import run_vacancy_check
            # run_vacancy_check は _fetch_record 経由で Neon DB を読むので、
            # まず status を pending に戻す（run_vacancy_check が pending を期待）
            await _neon_update_status(check_id, status="pending")
            await run_vacancy_check(check_id)

        elif original_status == "matching":
            # 図面解析済み → ATBB照合から開始
            from backend.services.vacancy_checker import run_vacancy_check_from_property_info
            await run_vacancy_check_from_property_info(
                check_id,
                job.get("property_name", ""),
                job.get("property_address", ""),
                job.get("property_rent", ""),
                job.get("property_area", ""),
                job.get("property_layout", ""),
                job.get("property_build_year", ""),
            )

        elif original_status == "checking":
            # プラットフォーム選択済み → 空室確認のみ
            from backend.services.vacancy_checker import _step_check
            await _step_check(check_id)

    except Exception as e:
        traceback.print_exc()
        try:
            await _neon_update_status(
                check_id,
                status="error",
                error_message=f"{type(e).__name__}: {e}",
                completed_at=datetime.now().isoformat(),
            )
        except Exception:
            traceback.print_exc()


# ============================================================
# Stale recovery（起動時）
# ============================================================
def _recover_stale():
    """起動時に 'running' 状態で残っている行を 'pending' に戻す"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE akikaku_checks
                   SET status = 'pending', updated_at = NOW()
                   WHERE status = 'running'
                   RETURNING id""",
            )
            recovered = cur.fetchall()
        conn.commit()
        if recovered:
            ids = [r[0] for r in recovered]
            print(f"[neon_listener] Stale recovery: {len(ids)} 件を pending に戻しました: {ids}")
    except Exception:
        conn.rollback()
        raise


# ============================================================
# awaiting_platform タイムアウト → 電話タスク自動生成
# ============================================================
AWAITING_TIMEOUT_MINUTES = 30


async def _timeout_awaiting_platform():
    """awaiting_platform が一定時間経過した行を電話確認タスクに自動変換"""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT id, property_name, property_address, atbb_company
                   FROM akikaku_checks
                   WHERE status = 'awaiting_platform'
                     AND created_at < NOW() - INTERVAL '%s minutes'""",
                (AWAITING_TIMEOUT_MINUTES,),
            )
            rows = cur.fetchall()
        conn.commit()

        if not rows:
            return

        print(f"[neon_listener] awaiting_platform タイムアウト: {len(rows)} 件を電話タスクに変換")

        for row in rows:
            check_id = row["id"]
            company = row.get("atbb_company", "") or ""
            company_name = company.split(" ")[0] if company else ""
            company_phone = ""
            parts = company.split(" ")
            if len(parts) > 1:
                company_phone = parts[-1]

            try:
                from backend.services.vacancy_checker import _create_phone_task
                await _create_phone_task(
                    check_id, company_name, company_phone,
                    row.get("property_name", ""),
                    row.get("property_address", ""),
                    "プラットフォーム自動判定不可(タイムアウト)"
                )
                await _neon_update_status(
                    check_id,
                    status="done",
                    vacancy_result="電話確認タスク作成済み",
                    completed_at=datetime.now().isoformat(),
                )
                print(f"[neon_listener] id={check_id} → 電話タスク変換完了")
            except Exception as e:
                print(f"[neon_listener] id={check_id} 電話タスク変換エラー: {e}")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[neon_listener] タイムアウトチェックエラー: {e}")


# ============================================================
# メインループ
# ============================================================
_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    print(f"\n[neon_listener] シグナル受信 ({signum}), シャットダウン中...")
    _shutdown = True


async def main():
    global _shutdown

    if not DATABASE_URL:
        print("[neon_listener] ERROR: DATABASE_URL 環境変数が設定されていません")
        sys.exit(1)

    # vacancy_checker の DB I/O を Neon 用に差し替え
    from backend.services.vacancy_checker import set_status_updater, set_record_fetcher
    set_status_updater(_neon_update_status)
    set_record_fetcher(_neon_fetch_record)

    # Playwright イベントループを起動
    from backend.services.playwright_loop import ensure_started
    ensure_started()

    # シグナルハンドラ登録
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    print("[neon_listener] 起動完了 - Neon DB をポーリング中 (5秒間隔)")
    print(f"[neon_listener] DATABASE_URL: ...{DATABASE_URL[-30:]}")

    # Stale recovery
    try:
        _recover_stale()
    except Exception as e:
        print(f"[neon_listener] Stale recovery エラー: {e}")

    # ポーリングループ
    _timeout_check_counter = 0
    while not _shutdown:
        try:
            job = _pick_job()
            if job:
                await _process_job(job)
            else:
                await asyncio.sleep(5)

            # 60回に1回(約5分ごと)awaiting_platformのタイムアウトチェック
            _timeout_check_counter += 1
            if _timeout_check_counter >= 60:
                _timeout_check_counter = 0
                await _timeout_awaiting_platform()

        except psycopg2.OperationalError as e:
            print(f"[neon_listener] DB接続エラー: {e} -10秒後にリトライ")
            _reset_conn()
            await asyncio.sleep(10)
        except Exception as e:
            print(f"[neon_listener] 予期しないエラー: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)

    print("[neon_listener] シャットダウン完了")
    _reset_conn()


if __name__ == "__main__":
    asyncio.run(main())
