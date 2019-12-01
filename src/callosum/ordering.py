from __future__ import annotations

import abc
import asyncio
from collections import defaultdict
import functools
import heapq
import logging
from typing import Final, Optional

import attr

from .serial import serial_lt
from .abc import CANCELLED

SEQ_BITS: Final = 32


def _resolve_future(request_id, fut, result, log):
    if fut is None:
        log.warning('resolved unknown request: %r', request_id)
        return
    if fut.done():
        if fut.cancelled():
            log.debug('resolved cancelled request: %r', request_id)
        if fut.exception() is not None:
            log.debug('resolved errored request: %r', request_id)
        return
    fut.set_result(result)


class AsyncResolver:

    __slots__ = ('_log', '_futures')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.AsyncResolver')
        self._futures = {}

    def wait(self, request_id) -> asyncio.Future:
        if request_id in self._futures:
            raise RuntimeError('duplicate request: %r', request_id)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        return fut

    def cancel(self, request_id):
        if request_id in self._futures:
            self._futures.pop(request_id)

    def resolve(self, request_id, result):
        fut = self._futures.pop(request_id, None)
        _resolve_future(request_id, fut, result, self._log)

    async def cleanup(self, request_id):
        pass


class AbstractAsyncScheduler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def schedule(self, request_id, scheduler, coro):
        raise NotImplementedError

    @abc.abstractmethod
    async def get_fut(self, request_id):
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel(self, request_id):
        raise NotImplementedError

    @abc.abstractmethod
    async def cleanup(self, request_id):
        raise NotImplementedError


@functools.total_ordering
@attr.dataclass(frozen=True, slots=True, eq=False, order=False)
class _SeqItem:
    method: str
    seq: int
    ev: Optional[asyncio.Event] = None

    def __lt__(self, other):
        return serial_lt(self.seq, other.seq, SEQ_BITS)

    def __eq__(self, other):
        return self.seq == other.seq


class KeySerializedAsyncScheduler(AbstractAsyncScheduler):

    __slots__ = ('_log', '_futures', '_pending')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.KeySerializedAsyncScheduler')
        self._futures = {}
        self._jobs = {}
        self._pending = defaultdict(list)

    async def schedule(self, request_id, scheduler, coro):
        method, okey, seq = request_id
        loop = asyncio.get_running_loop()
        self._futures[request_id] = loop.create_future()
        ev = asyncio.Event()
        heapq.heappush(self._pending[okey], _SeqItem(method, seq, ev))

        while True:
            s = self._pending[okey][0]
            if s.seq == seq:
                break
            await s.ev.wait()

        job = await scheduler.spawn(coro)
        self._jobs[request_id] = job

        def cb(s, rqst_id, task):
            _, okey, _ = rqst_id
            s.ev.set()
            fut = self.get_fut(rqst_id)
            if task.cancelled():
                result = CANCELLED
                _resolve_future(rqst_id, fut, result, self._log)
            else:
                _resolve_future(rqst_id, fut, task.result(), self._log)
                heapq.heappop(self._pending[okey])
            self.remove_if_empty(okey)

        job._task.add_done_callback(functools.partial(cb, s, request_id))
        await job.wait()

    def get_fut(self, request_id) -> asyncio.Future:
        return self._futures[request_id]

    async def cleanup(self, request_id):
        self._futures.pop(request_id)
        if request_id in self._jobs:
            self._jobs.pop(request_id)

    async def cancel(self, request_id):
        method, okey, seq = request_id
        if request_id in self._futures:
            if self._pending[okey]:
                pending_items = self._pending[okey]
                for seq_item in pending_items:
                    if seq_item.method == method and seq_item.seq == seq:
                        pending_items.remove(seq_item)
                        self.remove_if_empty(okey)
            job = self._jobs.get(request_id, None)
            if job:
                '''
                According to source code, calling
                "await job.close()" includes:
                - "job._task.cancel()"
                - "await job._task"
                So everything is taken care of in one command.
                '''
                await job.close()
        else:
            self._log.warning('cancellation of unknown or \
                               not sent yet request: %r', request_id)

    def remove_if_empty(self, okey):
        if len(self._pending[okey]) == 0:
            # TODO: check if pending is cleared
            del self._pending[okey]


class ExitOrderedAsyncScheduler(AbstractAsyncScheduler):

    __slots__ = ('_log', '_futures', '_results', '_sequences')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.ExitOrderedAsyncScheduler')
        self._futures = {}
        self._results = {}
        self._sequences = defaultdict(list)

    async def schedule(self, request_id, scheduler, coro):
        job = await scheduler.spawn(coro)

        def cb(task):
            self._resolve(request_id, task.result())

        job._task.add_done_callback(cb)

    def get_fut(self, request_id) -> asyncio.Future:
        if request_id in self._futures:
            raise RuntimeError('duplicate request: %r', request_id)

        method, okey, seq = request_id
        heapq.heappush(self._sequences[okey], _SeqItem(method, seq))

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        return fut

    async def cleanup(self, request_id):
        # TODO: implement
        self._futures.pop(request_id, None)

    async def cancel(self, request_id):
        # TODO: implement
        pass

    def _resolve(self, request_id, result):
        method, okey, seq = request_id
        self._results[request_id] = result
        if okey not in self._sequences:
            raise RuntimeError('unknown ordering key')
        while len(self._sequences[okey]) > 0:
            s = self._sequences[okey][0]
            rid = (s.method, okey, s.seq)
            if rid in self._results:
                heapq.heappop(self._sequences[okey])
                result = self._results.pop(rid)
                fut = self._futures.pop(rid, None)
                _resolve_future(rid, fut, result, self._log)
            else:
                break
        if len(self._sequences[okey]) == 0:
            del self._sequences[okey]
