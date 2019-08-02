import asyncio
import functools
import logging
from typing import (
    Callable, Type,
    Mapping, List,
    Any,
)
from collections import defaultdict
from datetime import datetime
from dateutil.tz import tzutc
import secrets

import aiojobs
from aiohttp import web
from aiojobs.aiohttp import get_scheduler_from_app
from async_timeout import timeout
import attr

from .auth import AbstractAuthenticator
from .compat import current_loop
from .exceptions import ServerError, HandlerError
from .eventmessage import (
    EventMessage,
    EventTypes,
    EventHandler,
)
from .message import RPCMessage, RPCMessageTypes
from .ordering import (
    AsyncResolver, AbstractAsyncScheduler,
    KeySerializedAsyncScheduler,
)
from .lower import (
    AbstractAddress,
    BaseTransport,
)
from .lower.redis import (
    TransportType,
    RedisStreamTransport,
)
from .ordering import SEQ_BITS


def _wrap_serializer(serializer):
    def _serialize(value):
        if serializer is not None:
            value = serializer(value)
        return value
    return _serialize


def _wrap_deserializer(deserializer):
    def _deserialize(value):
        if deserializer is not None:
            value = deserializer(value)
        return value
    return _deserialize


def _identity(val):
    return val


sentinel = object()


@attr.dataclass(frozen=True, slots=True)
class Tunnel:
    peer: 'Peer'
    tunnel_id: str
    remote_addr: str
    local_port: int

    async def close(self):
        # TODO: implement
        pass


class Publisher:
    '''
    Represents a unidirectional message publisher.
    '''

    def __init__(self, *,
                 bind: AbstractAddress = None,
                 serializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 redis_opts: Mapping[str, Any] = {},
                 transport_type: TransportType = TransportType.COMMON_STREAM):
        if bind is None:
            raise ValueError('You must specify the bind address.')
        self._bind = bind
        self._opener = None
        self._connection = None
        self._serializer = _wrap_serializer(serializer)
        if transport is None:
            raise ValueError('You must provide a transport class.')
        # transport_type only needs to be specified if RedisStreamTransport is used.
        if transport == RedisStreamTransport:
            self._transport = transport(authenticator=authenticator,
                                        transport_type=transport_type,
                                        redis_opts=redis_opts)
        else:
            self._transport = transport(authenticator=authenticator,
                                        redis_opts=redis_opts)

        self._outgoing_queue = asyncio.Queue()
        self._send_task = None

        self._log = logging.getLogger(__name__ + '.Publisher')

    async def _send_loop(self):
        while True:
            msg = await self._outgoing_queue.get()
            if msg is sentinel:
                break
            await self._connection.send_message(
                msg.encode(self._serializer))

    async def open(self):
        loop = current_loop()
        self._opener = functools.partial(self._transport.bind,
                                         self._bind)()
        self._connection = await self._opener.__aenter__()
        self._send_task = loop.create_task(self._send_loop())

    async def close(self):
        if self._send_task is not None:
            await self._outgoing_queue.put(sentinel)
            await self._send_task
        if self._transport is not None:
            await self._transport.close()

    def push(self,
             event: EventTypes,
             agent_id: str,
             timestamp: datetime = datetime.now(tzutc()),
             *args):
        msg = EventMessage.create(event,
                                  agent_id,
                                  timestamp,
                                  *args)
        self._outgoing_queue.put_nowait(msg)


class Subscriber:
    '''
    Represents a unidirectional message subscriber.
    If no scheduler is provided as a parameter,
    aiojobs scheduler with maximum concurrency
    of max_concurrency will be used.
    '''

    def __init__(self, *,
                 connect: AbstractAddress = None,
                 deserializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 redis_opts: Mapping[str, Any] = {},
                 transport_type: TransportType = TransportType.COMMON_STREAM,
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
        # transport_type only needs to be specified if RedisStreamTransport is used.
        if transport == RedisStreamTransport:
            self._transport = transport(authenticator=authenticator,
                                        transport_type=transport_type,
                                        redis_opts=redis_opts)
        else:
            self._transport = transport(authenticator=authenticator,
                                        redis_opts=redis_opts)
        self._scheduler = scheduler
        self._max_concurrency = max_concurrency

        self._incoming_queue = asyncio.Queue()
        self._handler_registry: Mapping[EventTypes, List[EventHandler]]\
             = defaultdict(list)
        self._recv_task = None

        self._log = logging.getLogger(__name__ + '.Subscriber')

    def add_handler(self,
                    event: EventTypes,
                    app: web.Application,
                    callback: Callable):
        self._handler_registry[event].append(EventHandler(app, callback))

    async def _recv_loop(self):
        while True:
            try:
                async for raw_msg in self._connection.recv_message():
                    if raw_msg is None:
                        return
                    msg = EventMessage.decode(raw_msg, self._deserializer)
                    self._incoming_queue.put_nowait(msg)
            except asyncio.CancelledError:
                break

    async def open(self):
        loop = current_loop()
        self._opener = functools.partial(self._transport.connect,
                                         self._connect)()
        self._connection = await self._opener.__aenter__()
        self._recv_task = loop.create_task(self._recv_loop())

    async def close(self):
        if self._recv_task is not None:
            await self._opener.__aexit__(None, None, None)
            self._recv_task.cancel()
            await self._recv_task
        if self._transport is not None:
            await self._transport.close()

    async def listen(self):
        '''
        Fetches incoming messages and calls appropriate
        handlers from "_handler_registry".
        '''
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        loop = current_loop()
        while True:
            msg = await self._incoming_queue.get()
            # _ stores timestamp value.
            # Later, when callbacks are customized to accept
            # timestamps, I may add it as another argument
            # to the handler.
            event, agent_id, _ = msg.header
            args = msg.body
            for handler in self._handler_registry[event]:
                cb = handler.callback
                if asyncio.iscoroutine(cb) or asyncio.iscoroutinefunction(cb):
                    await self._scheduler.spawn(cb(handler.app, agent_id, *args))
                else:
                    cb = functools.partial(cb, handler.app, agent_id, *args)
                    loop.call_soon(cb)


class Peer:
    '''
    Represents a bidirectional connection where both sides can invoke each
    other.

    In Callosum, there is no fixed server or client for a connection.
    Once the connection is established, each peer can become both
    RPC client and RPC server.
    '''

    def __init__(self, *,
                 connect: AbstractAddress = None,
                 bind: AbstractAddress = None,
                 serializer: Callable = None,
                 deserializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 redis_opts: Mapping[str, Any] = {},
                 scheduler: AbstractAsyncScheduler = None,
                 compress: bool = True,
                 max_body_size: int = 10 * (2**20),  # 10 MiBytes
                 max_concurrency: int = 100,
                 execute_timeout: float = None,
                 invoke_timeout: float = None):

        if connect is None and bind is None:
            raise ValueError('You must specify either the connect or bind address.')
        self._connect = connect
        self._bind = bind
        self._opener = None
        self._connection = None
        self._compress = compress
        self._serializer = _wrap_serializer(serializer)
        self._deserializer = _wrap_deserializer(deserializer)
        self._max_concurrency = max_concurrency
        self._exec_timeout = execute_timeout
        self._invoke_timeout = invoke_timeout

        self._scheduler = None
        if transport is None:
            raise ValueError('You must provide a transport class.')
        self._transport = transport(authenticator=authenticator,
                                    redis_opts=redis_opts)
        self._func_registry = {}
        self._stream_registry = {}

        self._seq_id = 0

        # incoming queues
        self._streaming_chunks = asyncio.Queue()
        self._function_requests = asyncio.Queue()
        self._invocation_resolver = AsyncResolver()
        if scheduler is None:
            scheduler = KeySerializedAsyncScheduler()
        self._func_scheduler = scheduler

        # there is only one outgoing queue
        self._outgoing_queue = asyncio.Queue()
        self._recv_task = None
        self._send_task = None

        self._log = logging.getLogger(__name__ + '.Peer')

    def handle_function(self, method, handler):
        self._func_registry[method] = handler

    def handle_stream(self, method, handler):
        self._stream_registry[method] = handler

    def unhandle_function(self, method):
        del self._func_registry[method]

    def unhandle_stream(self, method):
        del self._stream_registry[method]

    def _lookup(self, msgtype, method):
        if msgtype == RPCMessageTypes.FUNCTION:
            return self._func_registry[method]
        elif msgtype == RPCMessageTypes.STREAM:
            return self._stream_registry[method]
        raise ValueError('Invalid msgtype')

    async def _recv_loop(self):
        while True:
            try:
                async for raw_msg in self._connection.recv_message():
                    # TODO: flow-control in transports or peer queues?
                    if raw_msg is None:
                        return
                    msg = RPCMessage.decode(raw_msg, self._deserializer)
                    if msg.msgtype == RPCMessageTypes.FUNCTION:
                        self._function_requests.put_nowait(msg)
                    if msg.msgtype == RPCMessageTypes.CANCEL:
                        # TODO: implement cancellation
                        pass
                    elif msg.msgtype == RPCMessageTypes.STREAM:
                        self._streaming_chunks.put_nowait(msg)
                    elif msg.msgtype == RPCMessageTypes.RESULT:
                        self._invocation_resolver.resolve(msg.request_id, msg)
            except asyncio.CancelledError:
                break

    async def _send_loop(self):
        while True:
            msg = await self._outgoing_queue.get()
            if msg is sentinel:
                break
            await self._connection.send_message(
                msg.encode(self._serializer))

    async def open(self):
        loop = current_loop()
        if self._connect:
            self._opener = functools.partial(self._transport.connect,
                                             self._connect)()
        elif self._bind:
            self._opener = functools.partial(self._transport.bind,
                                             self._bind)()
        else:
            raise RuntimeError('Misconfigured opener')
        self._connection = await self._opener.__aenter__()
        self._recv_task = loop.create_task(self._recv_loop())
        self._send_task = loop.create_task(self._send_loop())

    async def close(self):
        if self._send_task is not None:
            await self._outgoing_queue.put(sentinel)
            await self._send_task
        if self._recv_task is not None:
            await self._opener.__aexit__(None, None, None)
            self._recv_task.cancel()
            await self._recv_task
        if self._scheduler is not None:
            await self._scheduler.close()
        if self._transport is not None:
            await self._transport.close()

    async def listen(self):
        '''
        Run a loop to fetch function requests from the transport connection.
        '''
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        loop = current_loop()
        while True:
            request = await self._function_requests.get()
            handler = self._lookup(request.msgtype, request.method)

            async def _func_task(request, handler):
                rqst_id = request.request_id
                try:
                    await self._func_scheduler.schedule(
                        rqst_id,
                        self._scheduler,
                        handler(request))
                    result = await self._func_scheduler.wait(rqst_id)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    raise
                except Exception as e:
                    self._log.error('Uncaught exception')
                    response = RPCMessage.error(request, e)
                else:
                    response = RPCMessage.result(request, result)
                await self._outgoing_queue.put(response)

            loop.create_task(_func_task(request, handler))

    async def invoke(self, method, body, *,
                     order_key=None, invoke_timeout=None):
        '''
        Invoke a remote function via the transport connection.
        '''
        if invoke_timeout is None:
            invoke_timeout = self._invoke_timeout
        if order_key is None:
            order_key = secrets.token_hex(8)
        self._seq_id = (self._seq_id + 1) % SEQ_BITS
        with timeout(invoke_timeout):
            try:
                request = None
                if callable(body):
                    # The user is using an upper-layer adaptor.
                    agen = body()
                    request = RPCMessage(
                        RPCMessageTypes.FUNCTION,
                        method,
                        order_key,
                        self._seq_id,
                        None,
                        await agen.asend(None),
                    )
                    await self._outgoing_queue.put(request)
                    response = await self._invocation_resolver.wait(
                        request.request_id)
                    upper_result = await agen.asend(response.body)
                    try:
                        await agen.asend(None)
                    except StopAsyncIteration:
                        pass
                else:
                    request = RPCMessage(
                        RPCMessageTypes.FUNCTION,
                        method,
                        order_key,
                        self._seq_id,
                        None,
                        body,
                    )
                    await self._outgoing_queue.put(request)
                    response = await self._invocation_resolver.wait(
                        request.request_id)
                    upper_result = response.body
                if response.msgtype == RPCMessageTypes.RESULT:
                    pass
                elif response.msgtype == RPCMessageTypes.FAILURE:
                    # TODO: encode/decode error info
                    raise HandlerError(response.body)
                elif response.msgtype == RPCMessageTypes.ERROR:
                    # TODO: encode/decode error info
                    raise ServerError(response.body)
                return upper_result
            except (asyncio.TimeoutError, asyncio.CancelledError):
                cancel_request = RPCMessage.cancel(request)
                await self._outgoing_queue.put(cancel_request)
                raise
            except Exception:
                raise

    async def send_stream(self, order_key, metadata, stream, *,
                          reporthook=None):
        raise NotImplementedError

    async def tunnel(self, remote_addr, *, inet_proto='tcp') -> Tunnel:
        '''
        Open a network tunnel via the peer connection.

        It returns a Tunnel object which contains the local port number where you can
        send packets to be delivered to the given ``remote_addr``.
        '''
        raise NotImplementedError
