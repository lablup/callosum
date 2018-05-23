import abc
import asyncio
from collections import defaultdict
import functools
import heapq
import logging

import attr

from .compat import current_loop


def _resolve_future(request_id, fut, result, log):
    if fut is None:
        log.warning('resolved unknown request: %r', request_id)
        return
    if fut.cancelled():
        log.debug('resolved cancelled request: %r', request_id)
        return
    # TODO: handle exceptions
    fut.set_result(result)


class AbstractAsyncResolver(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def schedule(self, request_id, scheduler, coro):
        raise NotImplementedError

    @abc.abstractmethod
    def wait(self, request_id) -> asyncio.Future:
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel(self, request_id):
        raise NotImplementedError


class AsyncResolver:

    __slots__ = ('_log', '_futures')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.AsyncResolver')
        self._futures = {}

    def wait(self, request_id) -> asyncio.Future:
        if request_id in self._futures:
            raise RuntimeError('duplicate request: %r', request_id)
        loop = current_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        return fut

    def cancel(self, request_id):
        # TODO: implement
        pass

    def resolve(self, request_id, result):
        fut = self._futures.pop(request_id, None)
        _resolve_future(request_id, fut, result, self._log)


@functools.total_ordering
@attr.dataclass(frozen=True, slots=True, cmp=False)
class _SeqItem:
    method: str
    seq: int
    ev: asyncio.Event = None

    # TODO: handle integer overflow
    def __lt__(self, other): return self.seq < other.seq   # noqa
    def __eq__(self, other): return self.seq == other.seq  # noqa


class EnterOrderedAsyncResolver(AbstractAsyncResolver):

    __slots__ = ('_log', '_futures', '_pending')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.EnterOrderedAsyncResolver')
        self._futures = {}
        self._pending = defaultdict(list)

    async def schedule(self, request_id, scheduler, coro):
        method, okey, seq = request_id
        loop = current_loop()
        self._futures[request_id] = loop.create_future()
        ev = asyncio.Event()
        heapq.heappush(self._pending[okey], _SeqItem(method, seq, ev))

        while True:
            s = self._pending[okey][0]
            if s.seq == seq:
                break
            await s.ev.wait()
            heapq.heappop(self._pending[okey])

        job = await scheduler.spawn(coro)

        def cb(s, rqst_id, task):
            _, okey, _ = rqst_id
            s.ev.set()
            fut = self._futures[rqst_id]
            _resolve_future(rqst_id, fut, task.result(), self._log)
            if len(self._pending[okey]) == 0:
                # TODO: check if pending is cleared
                del self._pending[okey]

        job._task.add_done_callback(functools.partial(cb, s, request_id))
        await job.wait()

    def wait(self, request_id) -> asyncio.Future:
        return self._futures.pop(request_id)

    def cancel(self, request_id):
        # TODO: implement
        pass


class ExitOrderedAsyncResolver(AbstractAsyncResolver):

    __slots__ = ('_log', '_futures', '_results', '_sequences')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.ExitOrderedAsyncResolver')
        self._futures = {}
        self._results = {}
        self._sequences = defaultdict(list)

    async def schedule(self, request_id, scheduler, coro):
        job = await scheduler.spawn(coro)

        def cb(task):
            self._resolve(request_id, task.result())

        job._task.add_done_callback(cb)

    def wait(self, request_id) -> asyncio.Future:
        if request_id in self._futures:
            raise RuntimeError('duplicate request: %r', request_id)

        method, okey, seq = request_id
        heapq.heappush(self._sequences[okey], _SeqItem(method, seq))

        loop = current_loop()
        fut = loop.create_future()
        self._futures[request_id] = fut
        return fut

    def cancel(self, request_id):
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
