"""Playwright専用イベントループ (Windows ProactorEventLoop)

全Playwright操作（ブラウザ起動、ログイン、空室確認）を
この専用スレッドのイベントループで実行する。

Windowsでは Playwright が ProactorEventLoop を必要とするため、
uvicorn のメインループ（SelectorEventLoop）とは別スレッドで動かす。
"""

import asyncio
import sys
import threading
from typing import Any, Coroutine

_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_started = threading.Event()


def _run_loop():
    """専用スレッドでイベントループを起動"""
    global _loop
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _started.set()
    _loop.run_forever()


def ensure_started():
    """Playwrightスレッドが起動していることを保証"""
    global _thread
    if _thread is None or not _thread.is_alive():
        _started.clear()
        _thread = threading.Thread(
            target=_run_loop, daemon=True, name="playwright-loop"
        )
        _thread.start()
        if not _started.wait(timeout=10):
            raise RuntimeError("Playwright専用スレッドの起動に失敗しました")


def get_loop() -> asyncio.AbstractEventLoop:
    """Playwright専用イベントループを取得"""
    ensure_started()
    assert _loop is not None
    return _loop


def run_coro(coro: Coroutine, timeout: float | None = None) -> Any:
    """コルーチンをPlaywrightスレッドで実行し結果を返す（呼び出し元をブロック）"""
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
