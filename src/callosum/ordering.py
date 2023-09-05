from __future__ import annotations

import abc
import asyncio
import functools
import heapq
import logging
from collections import defaultdict
from typing import Any, Awaitable, Dict, Final, List, Optional, Union

import attrs

from .abc import TaskSentinel
from .serial import serial_lt

SEQ_BITS: Final = 32


def _resolve_future(request_id, fut, result, log) -> None:
    if fut is None:
        log.warning("resolved unknown request: %r", request_id)
        return
    if fut.done():
        if fut.cancelled():
            log.debug("resolved cancelled request: %r", request_id)
        elif fut.exception() is not None:
            log.debug("resolved errored request: %r", request_id)
        return
    if isinstance(result, BaseException):
        fut.set_exception(result)
    else:
        fut.set_result(result)


class AsyncResolver:
    __slots__ = ("_log", "_futures")

    _futures: Dict[_SeqItem, asyncio.Future]

    def __init__(self) -> None:
        self._log = logging.getLogger(__name__ + ".AsyncResolver")
        self._futures = {}

    def wait(self, request_id) -> asyncio.Future:
        if request_id in self._futures:
            raise RuntimeError("duplicate request: %r", request_id)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        return fut

    def cancel(self, request_id) -> None:
        fut = self._futures.pop(request_id, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def resolve(self, request_id, result) -> None:
        fut = self._futures.pop(request_id, None)
        _resolve_future(request_id, fut, result, self._log)

    async def cleanup(self, request_id) -> None:
        pass


class AbstractAsyncScheduler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def schedule(self, request_id, coro) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_fut(self, request_id) -> Awaitable[Union[TaskSentinel, Any]]:
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel(self, request_id) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def cleanup(self, request_id) -> None:
        raise NotImplementedError


@functools.total_ordering
@attrs.define(frozen=True, slots=True, eq=False, order=False)
class _SeqItem:
    method: str
    seq: int
    ev: Optional[asyncio.Event] = None

    def __lt__(self, other: _SeqItem) -> bool:
        return serial_lt(self.seq, other.seq, SEQ_BITS)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _SeqItem):
            raise TypeError("cannot compare equality with non-SeqItem object", other)
        return self.seq == other.seq


class KeySerializedAsyncScheduler(AbstractAsyncScheduler):
    """
    A scheduler which ensures serialized returning of results within the given
    ordering key while it allows overlapped execution.
    """

    __slots__ = ("_log", "_futures", "_tasks", "_pending")

    _futures: Dict[_SeqItem, asyncio.Future]
    _tasks: Dict[_SeqItem, asyncio.Task]
    _pending: Dict[bytes, List[_SeqItem]]

    def __init__(self) -> None:
        self._log = logging.getLogger(__name__ + ".KeySerializedAsyncScheduler")
        self._futures = {}
        self._tasks = {}
        self._pending = defaultdict(list)

    async def schedule(self, request_id, coro) -> None:
        method, okey, seq = request_id
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        ev = asyncio.Event()
        heapq.heappush(self._pending[okey], _SeqItem(method, seq, ev))

        while True:
            assert len(self._pending[okey]) > 0
            head = self._pending[okey][0]
            if head.seq == seq:
                break
            # Wait until the head item finishes.
            assert head.ev is not None
            await head.ev.wait()

        task = asyncio.create_task(coro)
        self._tasks[request_id] = task

        def cb(seq_item, rqst_id, fut, task):
            _, okey, _ = rqst_id
            try:
                s = heapq.heappop(self._pending[okey])
                assert s == seq_item
                if task.cancelled():
                    # already removed from the pending heap
                    fut.cancel()
                else:
                    if task.exception() is not None:
                        _resolve_future(rqst_id, fut, task.exception(), self._log)
                    else:
                        _resolve_future(rqst_id, fut, task.result(), self._log)
            finally:
                seq_item.ev.set()

        task.add_done_callback(functools.partial(cb, head, request_id, fut))

    def get_fut(self, request_id) -> Awaitable[Union[TaskSentinel, Any]]:
        return self._futures[request_id]

    def cleanup(self, request_id) -> None:
        self._futures.pop(request_id, None)
        okey = request_id[1]
        if okey in self._pending:
            for seq_item in self._pending[okey]:
                if seq_item.seq == request_id[2]:
                    self._pending[okey].remove(seq_item)
                    break
        if len(self._pending[okey]) == 0:
            del self._pending[okey]
        else:
            heapq.heapify(self._pending[okey])
        self._tasks.pop(request_id, None)

    async def cancel(self, request_id) -> None:
        method, okey, seq = request_id
        if request_id in self._futures:
            task = self._tasks.get(request_id, None)
            if task is None:
                return
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # The task callback will notify the scheduler caller (upper).
            # The scheduler caller (upper) will call cleanup.
        else:
            self._log.warning(
                "cancellation of unknown or " "not sent yet request: %r", request_id
            )


class ExitOrderedAsyncScheduler(AbstractAsyncScheduler):
    """
    A scheduler which let the asyncio's event loop do its own scheduling.
    """

    __slots__ = ("_log", "_tasks")

    _tasks: Dict[_SeqItem, Union[TaskSentinel, asyncio.Task]]

    def __init__(self):
        self._log = logging.getLogger(__name__ + ".ExitOrderedAsyncScheduler")
        self._tasks = {}

    async def schedule(self, request_id, coro) -> None:
        task = asyncio.create_task(coro)
        if request_id in self._tasks:
            raise RuntimeError("duplicate request: %r", request_id)
        self._tasks[request_id] = task

    async def get_fut(self, request_id) -> Union[TaskSentinel, Any]:
        task = self._tasks.pop(request_id, TaskSentinel.CANCELLED)
        if isinstance(task, TaskSentinel):
            return task
        return await task

    def cleanup(self, request_id) -> None:
        self._tasks.pop(request_id, None)

    async def cancel(self, request_id) -> None:
        task = self._tasks.get(request_id, TaskSentinel.CANCELLED)
        if task is TaskSentinel.CANCELLED:
            return
        if not task.done():
            task.cancel()
        self._tasks[request_id] = TaskSentinel.CANCELLED
        await task
