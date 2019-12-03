from __future__ import annotations

import functools

from ..rpc import Peer, RPCMessage


class BaseRPCServerAdaptor:

    __slots__ = ('peer', )

    def __init__(self, peer: Peer):
        self.peer = peer

    async def handle_function(self, request: RPCMessage) -> bytes:
        '''
        Implements upper-layer handling of incoming requests and generation of raw
        responses in bytes (= already serialized in the upper layer).
        '''
        raise NotImplementedError

    async def handle_stream(self, request: RPCMessage) -> bytes:
        raise NotImplementedError


class BaseRPCClientAdaptor:

    __slots__ = ()

    def __init__(self):
        pass

    async def _call(self, method, args, kwargs):
        '''
        Implements upper-layer generation of requests and processing responses.
        '''

        # (1) GUIDE: generate raw_request_body (bytes) here
        # (2) Let Callosum to perform send/recv using its lower transport.
        raw_response_body = yield raw_request_body  # noqa
        # (3) GUIDE: parse raw_response_body (bytes) and get the result here
        result = None
        # (4) Return the upper-processed result.
        yield result

    def __getattr__(self, name):
        def _caller(*args, **kwargs):
            return functools.partial(self._call, name, args, kwargs)
        return _caller
