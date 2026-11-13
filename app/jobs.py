"""批次流水线专用作业执行器。

设计要点（对应并发修复需求）：

* **与 anyio 共享线程池完全隔离**。FastAPI 同步路由与 ``run_in_threadpool``
  默认使用 anyio 的共享线程池（默认 40 线程）。批次流水线的“等待他批次发布”
  可能长达 ``DICOM_REMAP_LOCK_WAIT`` 秒，若跑在共享池中，40 笔上传即可占满
  全部共享线程，把 health / 批次 / 映射查询一起饿死。这里使用独立的
  ``ThreadPoolExecutor``，上传等待只消耗本执行器名额，共享池始终空闲可服务查询。
* **名额即背压**。执行器大小固定为 ``DICOM_REMAP_WORKERS``，不随等待放大；
  超过名额的新作业在线程外排队，队列本身不占用任何工作线程。
* **请求断连不影响在途作业**。路由把作业提交后通过 ``asyncio.wrap_future``
  等待；客户端断连只会取消 *路由侧的等待*，执行器中的作业无法被取消，
  必然走完“发布或回滚”，最后自行释放执行名额与数据库连接。
* **可追踪、可优雅关闭**。全部在途 future 登记在 :data:`_inflight` 中，
  进程退出时等待在途批次到达终态，避免制造中断遗留。
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, TypeVar

from .config import settings

logger = logging.getLogger("dicom_remap.jobs")

T = TypeVar("T")

#: 专用执行器与在途登记表；模块级单例，测试中通过 shutdown_jobs/重置复用。
_executor: ThreadPoolExecutor | None = None
_executor_lock = threading.Lock()
_inflight: set[Future] = set()
_inflight_lock = threading.Lock()


def _get_executor() -> ThreadPoolExecutor:
    """惰性创建固定大小的批次执行器（线程名便于排障与线程转储核对）。"""
    global _executor
    if _executor is None:
        with _executor_lock:
            if _executor is None:
                workers = max(1, settings.workers)
                pool = ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix="dicom-batch",
                )
                logger.info("批次流水线执行器已创建，工作线程数=%d", workers)
                _executor = pool
    return _executor


def submit_batch_job(func: Callable[..., T], *args, **kwargs) -> Future[T]:
    """提交一个批次作业并登记，作业结束（成功/失败）后自动移出在途表。"""
    executor = _get_executor()
    future = executor.submit(func, *args, **kwargs)
    with _inflight_lock:
        _inflight.add(future)

    def _discard(done: Future) -> None:
        with _inflight_lock:
            _inflight.discard(done)
        if done.exception() is not None:
            logger.warning("批次作业以异常结束（回滚应由流水线内部完成）: %r",
                           done.exception())

    future.add_done_callback(_discard)
    return future


async def run_batch_job(func: Callable[..., T], *args, **kwargs) -> T:
    """在专用执行器中运行同步流水线，并把完成事件桥接到事件循环。

    客户端断连时，await 会被外层取消，但执行器线程内的作业不受影响：
    作业会继续执行到发布或回滚，释放执行名额与连接后才真正结束。
    """
    future = submit_batch_job(func, *args, **kwargs)
    # wrap_future 只负责桥接完成事件；取消它不会取消底层 concurrent future。
    return await asyncio.wrap_future(future)


def inflight_count() -> int:
    """当前在途（含排队与运行中）作业数，供健康检查/可观测性使用。"""
    with _inflight_lock:
        return len(_inflight)


async def shutdown_jobs(wait_seconds: float | None = None) -> None:
    """优雅关闭：等待在途批次完成发布或回滚，再关闭执行器。

    等待上限取 ``DICOM_REMAP_SHUTDOWN_WAIT``；超时后执行器仍以
    ``cancel_futures=False`` 关闭，已开始的作业所在线程会继续跑完
    （线程是非守护语义的解释器托管线程，进程退出前会完成当前工作单元）。
    """
    global _executor
    with _executor_lock:
        executor = _executor
        _executor = None
    if executor is None:
        return

    budget = settings.shutdown_wait_seconds if wait_seconds is None else wait_seconds
    with _inflight_lock:
        pending = list(_inflight)
    if pending:
        logger.info("等待 %d 个在途批次完成发布/回滚（上限 %.1fs）",
                    len(pending), budget)

    deadline = time.monotonic() + max(0.1, budget)

    def _await_all() -> None:
        # 所有在途作业共享同一个截止时间，避免 N 个作业累计等待 N 倍预算
        for future in pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                future.result(timeout=remaining)
            except Exception:  # 异常已在流水线内部转为回滚，这里只记录
                logger.debug("关闭等待期间作业异常", exc_info=True)

    await asyncio.to_thread(_await_all)
    # 不取消排队作业：让已提交的批次也能被工作线程取走并走完终态。
    executor.shutdown(wait=True, cancel_futures=False)
    with _inflight_lock:
        _inflight.clear()
