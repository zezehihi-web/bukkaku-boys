"""Neon PostgreSQL ポーリングリスナー

DB伝言板パターン: Vercel (akiya-tools) が INSERT した行を検出し、
ミニPC上の Playwright で空室確認を実行、結果を Neon に書き戻す。

cv_check_requests ブリッジ: Web CV ボタン → cv_check_requests テーブル →
akikaku_checks パイプライン → 結果を API に POST → LINE 返信

使い方:
  python -m backend.neon_listener
"""

import asyncio
import json
import os
import signal
import sys
import traceback
from datetime import datetime

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

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
# cv_check_requests ブリッジ（Web CV ボタン → 空室確認 → LINE返信）
# ============================================================
CV_WORKER_API_KEY = os.environ.get("CV_WORKER_API_KEY", "")
CV_CHECK_API_BASE = os.environ.get("CV_CHECK_API_BASE", "https://speedchintai.com")
CV_CHECK_API_BASE_JINGI = os.environ.get("CV_CHECK_API_BASE_JINGI", "https://chintai-jingi.vercel.app")


def _pick_cv_check_job():
    """cv_check_requests テーブルから pending ジョブを1件取得"""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM cv_check_requests
                   WHERE status = 'pending'
                   ORDER BY created_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED""",
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None

            cur.execute(
                "UPDATE cv_check_requests SET status = 'checking', checked_at = NOW() WHERE id = %s",
                (row["id"],),
            )
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise


def _cv_check_insert_bridge(cv_job: dict) -> int:
    """cv_check_request → akikaku_checks にブリッジレコードを挿入し、IDを返す"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO akikaku_checks
                   (submitted_url, property_name, status, created_at, updated_at,
                    line_user_id, batch_group)
                   VALUES (%s, %s, 'pending', NOW(), NOW(), %s, %s)
                   RETURNING id""",
                (
                    cv_job.get("detail_url") or "",
                    cv_job.get("property_name") or "",
                    cv_job.get("line_user_id") or "",
                    f"cv_bridge_{cv_job['id']}",
                ),
            )
            bridge_id = cur.fetchone()[0]
        conn.commit()
        return bridge_id
    except Exception:
        conn.rollback()
        raise


def _cv_check_read_result(bridge_id: int) -> dict:
    """akikaku_checks からブリッジレコードの結果を読み取る"""
    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM akikaku_checks WHERE id = %s", (bridge_id,))
        row = cur.fetchone()
    return dict(row) if row else {}


def _cv_check_map_result(akikaku_result: dict) -> dict:
    """akikaku_checks の結果を cv-check/result API のペイロードにマッピング"""
    vacancy = (akikaku_result.get("vacancy_result") or "").lower()
    status_val = akikaku_result.get("status", "")

    # 結果判定
    if status_val == "error":
        return {
            "status": "error",
            "errorMessage": akikaku_result.get("error_message", "確認中にエラー"),
        }

    if "募集終了" in vacancy or "ended" in vacancy or "終了" in vacancy:
        # 申込あり判定
        if "複数" in vacancy or "multiple" in vacancy:
            sub_status = "ended_multiple_applications"
        else:
            sub_status = "ended_one_application"
        return {
            "status": "ended",
            "checkResult": {
                "sub_status": sub_status,
                "detection_method": akikaku_result.get("platform", "unknown"),
                "availability": False,
            },
        }

    if "募集中" in vacancy or "空室" in vacancy or "available" in vacancy or status_val == "done":
        # 内見可否判定
        if "内見不可" in vacancy or "viewing_ng" in vacancy:
            sub_status = "available_viewing_ng"
        elif "要物確" in vacancy or "要確認" in vacancy or "needs_confirm" in vacancy:
            sub_status = "available_needs_confirm"
        elif "web申込不可" in vacancy or "web_apply_ng" in vacancy:
            sub_status = "available_web_apply_ng"
        else:
            sub_status = "available_viewing_ok"
        return {
            "status": "found",
            "checkResult": {
                "sub_status": sub_status,
                "detection_method": akikaku_result.get("platform", "unknown"),
                "availability": True,
            },
        }

    # 判定不能（done だが vacancy_result が不明瞭な場合）
    if status_val == "done":
        return {
            "status": "found",
            "checkResult": {
                "sub_status": "available_needs_confirm",
                "detection_method": akikaku_result.get("platform", "unknown"),
                "availability": True,
            },
        }

    return {
        "status": "error",
        "errorMessage": f"判定不能: status={status_val}, result={vacancy}",
    }


def _cv_check_post_result(cv_request_id: int, result_payload: dict, api_base: str = None):
    """cv-check/result API にPOST（LINE返信トリガー）"""
    if not CV_WORKER_API_KEY:
        print(f"[cv_bridge] WARNING: CV_WORKER_API_KEY 未設定、API POST スキップ")
        return

    base = api_base or CV_CHECK_API_BASE
    url = f"{base}/api/shinchaku/cv-check/result"

    payload = {"requestId": cv_request_id, **result_payload}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": CV_WORKER_API_KEY,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            print(f"[cv_bridge] API POST成功: {url} → {resp.status} {body[:200]}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[cv_bridge] API POSTエラー: {url} → {e.code} {body[:200]}")
    except Exception as e:
        print(f"[cv_bridge] API POST例外: {url} → {e}")


def _cv_check_determine_api_base(cv_job: dict) -> str:
    """cv_check_request の origin からどのAPIにPOSTすべきか判定

    2つのLINEアカウント/サイトが存在:
    - 賃貸の仁義 (chintai-jingi.vercel.app) → CV_CHECK_API_BASE_JINGI
    - スピード賃貸 (speedchintai.com) → CV_CHECK_API_BASE
    originカラムでどちらのサイトから来たリクエストか判定する
    """
    origin = (cv_job.get("origin") or "").strip()
    print(f"[cv_bridge] origin判定: origin='{origin}', id={cv_job.get('id')}")

    if origin:
        return origin.rstrip("/")

    # origin 未設定の場合 → 賃貸の仁義をデフォルトにする
    # (現在メインで使用中。speedchintai.comはbravoから解決不可)
    print(f"[cv_bridge] origin未設定 → デフォルト: {CV_CHECK_API_BASE_JINGI}")
    return CV_CHECK_API_BASE_JINGI


def _cv_check_update_error(cv_request_id: int, error_message: str):
    """cv_check_requests のステータスをエラーに更新"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE cv_check_requests
                   SET status = 'error', error_message = %s
                   WHERE id = %s""",
                (error_message, cv_request_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()


def _detect_platform_from_url(url: str) -> str:
    """URLからプラットフォームを推定"""
    url_lower = url.lower()
    if "itandibb.com" in url_lower or "itandi" in url_lower:
        return "itanji"
    if "es-square.net" in url_lower:
        return "es_square"
    if "ierabu" in url_lower:
        return "ierabu_bb"
    if "bukkaku" in url_lower:
        return "bukkaku"
    return ""


def _vacancy_to_api_result(vacancy_text: str, platform: str) -> dict:
    """チェッカーの結果文字列をAPI結果ペイロードに変換"""
    v = vacancy_text.lower() if vacancy_text else ""

    if "募集終了" in vacancy_text or "掲載終了" in vacancy_text:
        return {
            "status": "ended",
            "checkResult": {
                "sub_status": "ended_one_application",
                "detection_method": platform,
                "availability": False,
            },
        }

    if "申込あり" in vacancy_text or "申込" in vacancy_text:
        return {
            "status": "ended",
            "checkResult": {
                "sub_status": "ended_one_application",
                "detection_method": platform,
                "availability": False,
            },
        }

    if "該当なし" in vacancy_text:
        # プラットフォーム上に物件が存在しない = 実質的に募集終了
        return {
            "status": "ended",
            "checkResult": {
                "sub_status": "ended_one_application",
                "detection_method": platform,
                "availability": False,
                "note": "物件がプラットフォーム上に見つかりませんでした",
            },
        }

    if "募集中" in vacancy_text or "空室" in vacancy_text:
        return {
            "status": "found",
            "checkResult": {
                "sub_status": "available_viewing_ok",
                "detection_method": platform,
                "availability": True,
            },
        }

    # 不明な場合
    return {
        "status": "found",
        "checkResult": {
            "sub_status": "available_needs_confirm",
            "detection_method": platform,
            "availability": True,
        },
    }


async def _process_cv_check_job(cv_job: dict):
    """cv_check_request を直接プラットフォームチェッカーで確認する"""
    cv_id = cv_job["id"]
    property_name = cv_job.get("property_name", "")
    detail_url = cv_job.get("detail_url", "")
    cv_type = cv_job.get("cv_type", "availability")

    origin = cv_job.get("origin", "")
    print(f"[cv_bridge] CV確認開始: id={cv_id}, type={cv_type}, "
          f"property={property_name}, origin={origin}, url={detail_url[:80]}")

    if not detail_url:
        print(f"[cv_bridge] id={cv_id}: detail_url が空、エラー")
        _cv_check_update_error(cv_id, "detail_url が空です")
        _cv_check_post_result(cv_id, {
            "status": "error",
            "errorMessage": "物件URLが設定されていません",
        })
        return

    try:
        platform = _detect_platform_from_url(detail_url)
        print(f"[cv_bridge] id={cv_id}: platform={platform}")

        vacancy_result = ""

        if platform == "itanji":
            from backend.scrapers.itanji_checker import check_vacancy_by_url
            vacancy_result = await check_vacancy_by_url(detail_url)

        elif platform == "es_square":
            from backend.scrapers.es_square_checker import check_vacancy_by_url as es_check
            vacancy_result = await es_check(detail_url)

        else:
            # プラットフォーム不明 → akikaku_checks パイプラインにフォールバック
            print(f"[cv_bridge] id={cv_id}: プラットフォーム不明、パイプラインフォールバック")
            bridge_id = _cv_check_insert_bridge(cv_job)
            from backend.services.vacancy_checker import run_vacancy_check
            await _neon_update_status(bridge_id, status="pending")
            await run_vacancy_check(bridge_id)
            result = _cv_check_read_result(bridge_id)
            api_payload = _cv_check_map_result(result)
            api_base = _cv_check_determine_api_base(cv_job)
            _cv_check_post_result(cv_id, api_payload, api_base)
            print(f"[cv_bridge] id={cv_id}: フォールバック完了 → {api_payload.get('status')}")
            return

        print(f"[cv_bridge] id={cv_id}: チェック結果 = {vacancy_result}")

        # 結果をAPIにPOST → LINE返信トリガー
        api_payload = _vacancy_to_api_result(vacancy_result, platform)
        api_base = _cv_check_determine_api_base(cv_job)
        _cv_check_post_result(cv_id, api_payload, api_base)

        print(f"[cv_bridge] id={cv_id}: 完了 → {api_payload.get('status')}")

    except Exception as e:
        traceback.print_exc()
        error_msg = f"{type(e).__name__}: {e}"
        print(f"[cv_bridge] id={cv_id}: エラー → {error_msg}")
        _cv_check_update_error(cv_id, error_msg)
        _cv_check_post_result(cv_id, {
            "status": "error",
            "errorMessage": error_msg,
        })


def _recover_stale_cv_checks():
    """起動時に 'checking' で残っている cv_check_requests を 'pending' に戻す"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE cv_check_requests
                   SET status = 'pending'
                   WHERE status = 'checking'
                     AND checked_at < NOW() - INTERVAL '10 minutes'
                   RETURNING id""",
            )
            recovered = cur.fetchall()
        conn.commit()
        if recovered:
            ids = [r[0] for r in recovered]
            print(f"[cv_bridge] Stale recovery: {len(ids)} 件を pending に戻しました: {ids}")
    except Exception:
        conn.rollback()
        # テーブルが存在しない場合は無視
        print(f"[cv_bridge] Stale recovery スキップ（テーブル未作成の可能性）")


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
    print(f"[neon_listener] CV_WORKER_API_KEY: {'設定済み' if CV_WORKER_API_KEY else '未設定'}")
    print(f"[neon_listener] CV_CHECK_API_BASE: {CV_CHECK_API_BASE}")

    # Stale recovery
    try:
        _recover_stale()
    except Exception as e:
        print(f"[neon_listener] Stale recovery エラー: {e}")

    try:
        _recover_stale_cv_checks()
    except Exception as e:
        print(f"[neon_listener] CV checks stale recovery エラー: {e}")

    # ポーリングループ
    _timeout_check_counter = 0
    while not _shutdown:
        try:
            # ① 通常の akikaku_checks ジョブ
            job = _pick_job()
            if job:
                await _process_job(job)
            else:
                # ② cv_check_requests ジョブ（Web CVボタン経由）
                cv_job = None
                try:
                    cv_job = _pick_cv_check_job()
                except Exception as e:
                    # テーブル未作成等の場合は無視して続行
                    if "does not exist" not in str(e):
                        print(f"[cv_bridge] ジョブ取得エラー: {e}")

                if cv_job:
                    await _process_cv_check_job(cv_job)
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
