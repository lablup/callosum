from __future__ import annotations

import asyncio
import functools
import logging
from typing import (
    Any, Callable, Optional, Type, Union,
    Mapping,
    List,
)

import aiojobs
from aiotools import aclosing

from ..abc import (
    Sentinel, CLOSED,
    AbstractChannel,
    AbstractDeserializer, AbstractSerializer,
)
from ..auth import AbstractAuthenticator
from ..lower import (
    AbstractAddress,
    AbstractBinder,
    AbstractConnector,
    AbstractConnection,
    BaseTransport,
)
from .message import StreamMessage

log = logging.getLogger(__name__)


class Publisher(AbstractChannel):
    '''
    Represents a unidirectional message publisher.
    '''

    _connection: Optional[AbstractConnection]
    _opener: Optional[AbstractBinder]
    _outgoing_queue: asyncio.Queue[Union[Sentinel, StreamMessage]]
    _send_task: Optional[asyncio.Task]
    _serializer: AbstractSerializer

    def __init__(
        self, *,
        serializer: AbstractSerializer,
        bind: AbstractAddress = None,
        transport: Type[BaseTransport] = None,
        authenticator: AbstractAuthenticator = None,
        transport_opts: Mapping[str, Any] = {},
    ) -> None:
        if bind is None:
            raise ValueError('You must specify the bind address.')
        self._bind = bind
        self._opener = None
        self._connection = None
        self._serializer = serializer
        if transport is None:
            raise ValueError('You must provide a transport class.')
        self._transport = transport(authenticator=authenticator,
                                    transport_opts=transport_opts)

        self._outgoing_queue = asyncio.Queue()
        self._send_task = None

        self._log = logging.getLogger(__name__ + '.Publisher')

    async def _send_loop(self) -> None:
        if self._connection is None:
            raise RuntimeError('consumer is not opened yet.')
        while True:
            try:
                msg = await self._outgoing_queue.get()
                if msg is CLOSED:
                    break
                assert not isinstance(msg, Sentinel)
                await self._connection.send_message(
                    msg.encode(self._serializer))
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error')

    async def __aenter__(self) -> Publisher:
        _opener = functools.partial(self._transport.bind,
                                    self._bind)()
        self._opener = _opener
        self._connection = await _opener.__aenter__()
        self._send_task = asyncio.create_task(self._send_loop())
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._send_task is not None:
            if self._opener is not None:
                await self._opener.__aexit__(None, None, None)
            await self._outgoing_queue.put(CLOSED)
            await self._send_task
        if self._transport is not None:
            await self._transport.close()

    def push(self, body) -> None:
        msg = StreamMessage.create(body)
        self._outgoing_queue.put_nowait(msg)


class Consumer(AbstractChannel):
    '''
    Represents a unidirectional message consumer.
    If no scheduler is provided as a parameter,
    aiojobs scheduler with maximum concurrency
    of max_concurrency will be used.
    '''

    _connection: Optional[AbstractConnection]
    _opener: Optional[AbstractConnector]
    _incoming_queue: asyncio.Queue[StreamMessage]
    _recv_task: Optional[asyncio.Task]
    _deserializer: AbstractDeserializer

    def __init__(
        self, *,
        deserializer: AbstractDeserializer,
        connect: AbstractAddress = None,
        transport: Type[BaseTransport] = None,
        authenticator: AbstractAuthenticator = None,
        transport_opts: Mapping[str, Any] = {},
        scheduler=None,
        max_concurrency: int = 100,
    ) -> None:
        if connect is None:
            raise ValueError('You must specify the connect address.')
        self._connect = connect
        self._opener = None
        self._connection = None
        self._deserializer = deserializer
        if transport is None:
            raise ValueError('You must provide a transport class.')
        self._transport = transport(authenticator=authenticator,
                                    transport_opts=transport_opts)
        self._scheduler = scheduler
        self._max_concurrency = max_concurrency

        self._incoming_queue = asyncio.Queue()
        self._handler_registry: List[Callable] = []
        self._recv_task = None

        self._log = logging.getLogger(__name__ + '.Consumer')

    def add_handler(self,
                    callback: Callable) -> None:
        self._handler_registry.append(callback)

    async def _recv_loop(self) -> None:
        if self._connection is None:
            raise RuntimeError('consumer is not opened yet.')
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        loop = asyncio.get_running_loop()
        while True:
            try:
                async with aclosing(self._connection.recv_message()) as agen:
                    async for raw_msg in agen:
                        if raw_msg is None:
                            return
                        msg = StreamMessage.decode(raw_msg, self._deserializer)
                        for handler in self._handler_registry:
                            if (asyncio.iscoroutine(handler) or
                                    asyncio.iscoroutinefunction(handler)):
                                await self._scheduler.spawn(handler(msg))
                            else:
                                handler = functools.partial(handler, msg)
                                loop.call_soon(handler)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error')

    async def __aenter__(self) -> Consumer:
        _opener = functools.partial(self._transport.connect,
                                    self._connect)()
        self._opener = _opener
        self._connection = await _opener.__aenter__()
        self._recv_task = asyncio.create_task(self._recv_loop())
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._recv_task is not None:
            if self._opener is not None:
                await self._opener.__aexit__(None, None, None)
            self._recv_task.cancel()
            await self._recv_task
        if self._transport is not None:
            await self._transport.close()
