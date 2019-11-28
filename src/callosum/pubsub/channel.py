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
from datetime import datetime
from dateutil.tz import tzutc

from ..abc import (
    Sentinel, CLOSED,
)
from ..auth import AbstractAuthenticator
from ..lower import (
    AbstractAddress,
    AbstractBinder,
    AbstractConnector,
    AbstractConnection,
    BaseTransport,
)
from .message import PubSubMessage


# TODO: refactor out
def _wrap_serializer(serializer):
    def _serialize(value):
        if serializer is not None:
            value = serializer(value)
        return value
    return _serialize


# TODO: refactor out
def _wrap_deserializer(deserializer):
    def _deserialize(value):
        if deserializer is not None:
            value = deserializer(value)
        return value
    return _deserialize


class Publisher:
    '''
    Represents a unidirectional message publisher.
    '''

    _connection: Optional[AbstractConnection]
    _opener: Optional[AbstractBinder]
    _outgoing_queue: asyncio.Queue[Union[Sentinel, PubSubMessage]]
    _send_task: Optional[asyncio.Task]

    def __init__(self, *,
                 bind: AbstractAddress = None,
                 serializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 transport_opts: Mapping[str, Any] = {}):
        if bind is None:
            raise ValueError('You must specify the bind address.')
        self._bind = bind
        self._opener = None
        self._connection = None
        self._serializer = _wrap_serializer(serializer)
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
            msg = await self._outgoing_queue.get()
            if msg is CLOSED:
                break
            assert not isinstance(msg, Sentinel)
            await self._connection.send_message(
                msg.encode(self._serializer))

    async def open(self) -> None:
        _opener = functools.partial(self._transport.bind,
                                    self._bind)()
        self._opener = _opener
        self._connection = await _opener.__aenter__()
        self._send_task = asyncio.create_task(self._send_loop())

    async def close(self) -> None:
        if self._send_task is not None:
            if self._opener is not None:
                await self._opener.__aexit__(None, None, None)
            await self._outgoing_queue.put(CLOSED)
            await self._send_task
        if self._transport is not None:
            await self._transport.close()

    def push(self,
             body,
             timestamp: datetime = datetime.now(tzutc())) -> None:
        msg = PubSubMessage.create(timestamp, body)
        self._outgoing_queue.put_nowait(msg)


class Consumer:
    '''
    Represents a unidirectional message consumer.
    If no scheduler is provided as a parameter,
    aiojobs scheduler with maximum concurrency
    of max_concurrency will be used.
    '''

    _connection: Optional[AbstractConnection]
    _opener: Optional[AbstractConnector]
    _incoming_queue: asyncio.Queue[PubSubMessage]
    _recv_task: Optional[asyncio.Task]

    def __init__(self, *,
                 connect: AbstractAddress = None,
                 deserializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 transport_opts: Mapping[str, Any] = {},
                 scheduler=None,
                 max_concurrency: int = 100):
        if connect is None:
            raise ValueError('You must specify the connect address.')
        self._connect = connect
        self._opener = None
        self._connection = None
        self._deserializer = _wrap_deserializer(deserializer)
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
        while True:
            try:
                async for raw_msg in self._connection.recv_message():
                    if raw_msg is None:
                        return
                    msg = PubSubMessage.decode(raw_msg, self._deserializer)
                    self._incoming_queue.put_nowait(msg)
            except asyncio.CancelledError:
                break

    async def open(self) -> None:
        _opener = functools.partial(self._transport.connect,
                                    self._connect)()
        self._opener = _opener
        self._connection = await _opener.__aenter__()
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def close(self) -> None:
        if self._recv_task is not None:
            if self._opener is not None:
                await self._opener.__aexit__(None, None, None)
            self._recv_task.cancel()
            await self._recv_task
        if self._transport is not None:
            await self._transport.close()

    async def listen(self) -> None:
        '''
        Fetches incoming messages and calls appropriate
        handlers from "_handler_registry".
        '''
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        loop = asyncio.get_running_loop()
        while True:
            msg = await self._incoming_queue.get()
            for handler in self._handler_registry:
                if (asyncio.iscoroutine(handler) or
                        asyncio.iscoroutinefunction(handler)):
                    await self._scheduler.spawn(handler(msg))
                else:
                    handler = functools.partial(handler, msg)
                    loop.call_soon(handler)
