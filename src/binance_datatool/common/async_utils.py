"""Async utilities for bridging sync code to async SDKs without event loop conflicts."""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")

_loop_thread: threading.Thread | None = None
_loop: asyncio.AbstractEventLoop | None = None
_lock = threading.Lock()


def _start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def get_shared_loop() -> asyncio.AbstractEventLoop:
    """Get or create the shared background event loop."""
    global _loop, _loop_thread
    with _lock:
        if _loop is None or not _loop.is_running():
            _loop = asyncio.new_event_loop()
            _loop_thread = threading.Thread(
                target=_start_background_loop,
                args=(_loop,),
                daemon=True,
                name="BackgroundAsyncLoop",
            )
            _loop_thread.start()
    return _loop


def sync_run(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine safely from a synchronous thread.

    Submits the coroutine to a shared background event loop and waits for the result.
    This avoids RuntimeError ("asyncio.run() cannot be called from a running event loop")
    when invoked from within Prefect tasks or other managed threads.
    """
    loop = get_shared_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()
