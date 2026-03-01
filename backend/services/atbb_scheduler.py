"""ATBBスクレイピング定期実行スケジューラー

毎日0時と12時にATBBリストスクレイピングを実行し、
properties_database_list.json を更新する。
"""

import asyncio
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from backend.config import BASE_DIR

SCRAPER_SCRIPT = BASE_DIR / "atbb_list_scraper.py"
_scheduler_task = None


def _next_run_time() -> datetime:
    """次の実行時刻（0時 or 12時）を計算"""
    now = datetime.now()
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)

    candidates = [today_midnight, today_noon, today_midnight + timedelta(days=1), today_noon + timedelta(days=1)]
    future = [t for t in candidates if t > now]
    return future[0]


def _run_scraper():
    """ATBBスクレイパーを子プロセスで実行"""
    print(f"[atbb_scheduler] スクレイピング開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        result = subprocess.run(
            [sys.executable, str(SCRAPER_SCRIPT)],
            cwd=str(BASE_DIR),
            capture_output=True,
            timeout=1800,  # 30分タイムアウト
        )
        if result.returncode == 0:
            print(f"[atbb_scheduler] スクレイピング完了: 正常終了")
        else:
            stderr = result.stderr.decode("utf-8", errors="replace")[:500]
            print(f"[atbb_scheduler] スクレイピングエラー (code={result.returncode}): {stderr}")
    except subprocess.TimeoutExpired:
        print("[atbb_scheduler] スクレイピングタイムアウト（30分）")
    except Exception as e:
        print(f"[atbb_scheduler] スクレイピング実行エラー: {e}")


async def _scheduler_loop():
    """定期実行ループ"""
    while True:
        try:
            next_time = _next_run_time()
            wait_seconds = (next_time - datetime.now()).total_seconds()
            print(f"[atbb_scheduler] 次回実行: {next_time.strftime('%Y-%m-%d %H:%M')} ({wait_seconds:.0f}秒後)")

            await asyncio.sleep(wait_seconds)

            # 別スレッドで実行（asyncioブロック回避）
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _run_scraper)

        except asyncio.CancelledError:
            print("[atbb_scheduler] 停止")
            break
        except Exception as e:
            print(f"[atbb_scheduler] エラー: {e}")
            await asyncio.sleep(60)


async def startup():
    """スケジューラー開始"""
    global _scheduler_task

    if not SCRAPER_SCRIPT.exists():
        print(f"[atbb_scheduler] スクリプトが見つかりません: {SCRAPER_SCRIPT}")
        return

    _scheduler_task = asyncio.create_task(_scheduler_loop())
    print("[atbb_scheduler] ATBBスケジューラー開始（0時・12時に実行）")


async def shutdown():
    """スケジューラー停止"""
    global _scheduler_task
    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    print("[atbb_scheduler] シャットダウン完了")
