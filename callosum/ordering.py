import asyncio
import logging

from .compat import current_loop


class AsyncResolver:

    __slots__ = ('_log', '_futures')

    def __init__(self):
        self._log = logging.getLogger(__name__ + '.AsyncResolver')
        self._futures = {}

    def wait(self, key) -> asyncio.Future:
        if key in self._futures:
            raise RuntimeError('duplicate request: %r', key)
        loop = current_loop()
        fut = loop.create_future()
        self._futures[key] = fut
        return fut

    def resolve(self, key, result):
        fut = self._futures.pop(key, None)
        if fut is None:
            self._log.warning('resolved unknown request: %r', key)
            return
        if fut.cancelled():
            self._log.debug('resolved cancelled request: %r', key)
            return
        fut.set_result(result)
