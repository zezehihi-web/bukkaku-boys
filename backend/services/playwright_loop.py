"""Playwright専用イベントループ (Windows ProactorEventLoop)

全Playwright操作（ブラウザ起動、ログイン、空室確認）を
この専用スレッドのイベントループで実行する。

Windowsでは Playwright が ProactorEventLoop を必要とするため、
uvicorn のメインループ（SelectorEventLoop）とは別スレッドで動かす。

自動復旧: スレッドが死んだ場合は自動的に再起動する。
"""

import asyncio
import sys
import threading
import traceback
from typing import Any, Coroutine

_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_started = threading.Event()
_lock = threading.Lock()


def _run_loop():
    """専用スレッドでイベントループを起動"""
    global _loop
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _started.set()
    try:
        _loop.run_forever()
    except Exception:
        traceback.print_exc()
        print("[playwright_loop] イベントループが異常終了しました")
    finally:
        _loop = None


def ensure_started():
    """Playwrightスレッドが起動していることを保証（死んでいれば再起動）"""
    global _thread
    with _lock:
        if _thread is not None and _thread.is_alive() and _loop is not None:
            return

        if _thread is not None and not _thread.is_alive():
            print("[playwright_loop] スレッド死亡を検出、再起動します...")

        _started.clear()
        _thread = threading.Thread(
            target=_run_loop, daemon=True, name="playwright-loop"
        )
        _thread.start()
        if not _started.wait(timeout=10):
            raise RuntimeError("Playwright専用スレッドの起動に失敗しました")
        print("[playwright_loop] スレッド起動完了")


def is_alive() -> bool:
    """Playwrightスレッドが生きているか確認"""
    return _thread is not None and _thread.is_alive() and _loop is not None


def get_loop() -> asyncio.AbstractEventLoop:
    """Playwright専用イベントループを取得（死んでいれば再起動）"""
    ensure_started()
    assert _loop is not None
    return _loop


def run_coro(coro: Coroutine, timeout: float | None = 120) -> Any:
    """コルーチンをPlaywrightスレッドで実行し結果を返す（呼び出し元をブロック）

    デフォルトタイムアウト: 120秒
    """
    loop = get_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


def submit_coro(coro: Coroutine):
    """コルーチンをPlaywrightスレッドに投入（非ブロッキング fire-and-forget）"""
    loop = get_loop()
    asyncio.run_coroutine_threadsafe(coro, loop)


def stop():
    """Playwrightスレッドを停止"""
    global _loop, _thread
    if _loop:
        _loop.call_soon_threadsafe(_loop.stop)
    if _thread:
        _thread.join(timeout=10)
    _loop = None
    _thread = None
