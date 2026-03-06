"""ブラウザ常時ログイン待機サービス

サーバー起動時にイタンジBB・いい生活スクエアにログインし、
定期的にセッション生存を確認、切れていれば再ログインする。

全Playwright操作は playwright_loop の専用スレッドで実行される。
重要: platform_lock を使用して空室確認との同時操作を防止する。
"""

import asyncio
import traceback

from backend.scrapers.browser_manager import get_page, platform_lock

# セッション確認間隔（秒）
CHECK_INTERVAL = 300  # 5分ごとにセッション確認

_keep_alive_task = None


async def _login_itanji(page):
    """イタンジBBにログイン"""
    from backend.scrapers.itanji_checker import login, is_logged_in
    if not await is_logged_in(page):
        print("[session_keeper] イタンジBB: ログイン開始...")
        ok = await login(page)
        if ok:
            print("[session_keeper] イタンジBB: ログイン成功")
        else:
            print("[session_keeper] イタンジBB: ログイン失敗")
    else:
        print("[session_keeper] イタンジBB: ログイン済み")


async def _login_es_square(page):
    """いい生活スクエアにログイン"""
    from backend.scrapers.es_square_checker import login, is_logged_in
    if not await is_logged_in(page):
        print("[session_keeper] いい生活スクエア: ログイン開始...")
        ok = await login(page)
        if ok:
            print("[session_keeper] いい生活スクエア: ログイン成功")
        else:
            print("[session_keeper] いい生活スクエア: ログイン失敗")
    else:
        print("[session_keeper] いい生活スクエア: ログイン済み")


async def _keep_sessions_alive():
    """定期的にセッション生存を確認し、切れていれば再ログイン

    platform_lock を取得してから操作するため、
    空室確認の最中にページを奪うことがない。
    """
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL)

            # イタンジBB — ロックを取得してからページ操作
            try:
                async with platform_lock("itanji"):
                    page = await get_page("itanji")
                    await _login_itanji(page)
            except Exception as e:
                print(f"[session_keeper] イタンジBB セッション確認エラー: {e}")

            # いい生活スクエア — ロックを取得してからページ操作
            try:
                async with platform_lock("es_square"):
                    page = await get_page("es_square")
                    await _login_es_square(page)
            except Exception as e:
                print(f"[session_keeper] いい生活スクエア セッション確認エラー: {e}")

        except asyncio.CancelledError:
            print("[session_keeper] 停止")
            break
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(60)  # エラー時は1分待って再試行


async def _startup_internal():
    """Playwrightスレッド内で実行: 初回ログイン + セッション維持タスク開始"""
    global _keep_alive_task

    print("[session_keeper] 初回ログイン開始...")

    # イタンジBB
    try:
        page = await get_page("itanji")
        await _login_itanji(page)
    except Exception as e:
        print(f"[session_keeper] イタンジBB 初回ログインエラー: {e}")
        traceback.print_exc()

    # いい生活スクエア
    try:
        page = await get_page("es_square")
        await _login_es_square(page)
    except Exception as e:
        print(f"[session_keeper] いい生活スクエア 初回ログインエラー: {e}")
        traceback.print_exc()

    # セッション維持タスク開始（Playwrightスレッドのイベントループ上）
    _keep_alive_task = asyncio.create_task(_keep_sessions_alive())
    print("[session_keeper] セッション維持タスク開始")


def startup():
    """スケジューラー開始（Playwrightスレッドにfire-and-forget）

    初回ログインはバックグラウンドで行い、
    サーバー起動をブロックしない。
    """
    from backend.services.playwright_loop import submit_coro
    submit_coro(_startup_internal())
    print("[session_keeper] 起動タスク投入済み")


async def _shutdown_internal():
    """Playwrightスレッド内で実行: シャットダウン"""
    global _keep_alive_task
    if _keep_alive_task:
        _keep_alive_task.cancel()
        try:
            await _keep_alive_task
        except asyncio.CancelledError:
            pass

    from backend.scrapers.browser_manager import close_all
    await close_all()
    print("[session_keeper] シャットダウン完了")


def shutdown():
    """シャットダウン（Playwrightスレッドで同期的に完了を待つ）"""
    from backend.services.playwright_loop import run_coro
    try:
        run_coro(_shutdown_internal(), timeout=30)
    except Exception as e:
        print(f"[session_keeper] シャットダウンエラー: {e}")
