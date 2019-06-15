import asyncio
import functools
import logging
from typing import Callable, Type
import secrets

import aiojobs
from async_timeout import timeout
import attr

from .auth import AbstractAuthenticator
from .compat import current_loop
from .exceptions import ServerError, HandlerError
from .message import Message, MessageTypes
from .ordering import (
    AsyncResolver, AbstractAsyncScheduler,
    KeySerializedAsyncScheduler,
)
from .lower import BaseTransport


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


@attr.dataclass(frozen=True, slots=True)
class Tunnel:
    peer: 'Peer'
    tunnel_id: str
    remote_addr: str
    local_port: int

    async def close(self):
        # TODO: implement
        pass


class Peer:
    '''
    Represents the connection peer.

    In Callosum, there is no fixed server or client for a connection.
    Once the connection is established, each peer can become both
    RPC client and RPC server.
    '''

    def __init__(self, *,
                 connect: str = None,
                 bind: str = None,
                 serializer: Callable = None,
                 deserializer: Callable = None,
                 transport_cls: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 scheduler: AbstractAsyncScheduler = None,
                 compress: bool = True,
                 max_body_size: int = 10 * (2**20),  # 10 MiBytes
                 max_concurrency: int = 100,
                 execute_timeout: float = None,
                 invoke_timeout: float = None):

        if connect is None and bind is None:
            raise ValueError('You must specify either connect or bind.')
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
        if transport_cls is None:
            raise ValueError('You must provide a transport class.')
        self._transport = transport_cls(authenticator=authenticator)
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
        if msgtype == MessageTypes.FUNCTION:
            return self._func_registry[method]
        elif msgtype == MessageTypes.STREAM:
            return self._stream_registry[method]
        raise ValueError('Invalid msgtype')

    async def _recv_loop(self):
        while True:
            try:
                # TODO: flow-control in transports or peer queues?
                raw_msg = await self._connection.recv_message()
                msg = Message.decode(raw_msg, self._deserializer)
                if msg.msgtype == MessageTypes.FUNCTION:
                    self._function_requests.put_nowait(msg)
                if msg.msgtype == MessageTypes.CANCEL:
                    # TODO: implement cancellation
                    pass
                elif msg.msgtype == MessageTypes.STREAM:
                    self._streaming_chunks.put_nowait(msg)
                elif msg.msgtype == MessageTypes.RESULT:
                    self._invocation_resolver.resolve(msg.request_id, msg)
            except asyncio.CancelledError:
                break

    async def _send_loop(self):
        while True:
            try:
                msg = await self._outgoing_queue.get()
                await self._connection.send_message(
                    msg.encode(self._serializer))
            except asyncio.CancelledError:
                break

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
        opened = False
        if self._recv_task is not None:
            opened = True
            self._recv_task.cancel()
            await self._recv_task
        if self._send_task is not None:
            opened = True
            self._send_task.cancel()
            await self._send_task
        if opened:
            await self._opener.__aexit__(None, None, None)
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
                    response = Message.error(request, e)
                else:
                    response = Message.result(request, result)
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
        self._seq_id += 1
        with timeout(invoke_timeout):
            try:
                request = None
                if callable(body):
                    # The user is using an upper-layer adaptor.
                    agen = body()
                    request = Message(
                        MessageTypes.FUNCTION,
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
                    request = Message(
                        MessageTypes.FUNCTION,
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
                if response.msgtype == MessageTypes.RESULT:
                    pass
                elif response.msgtype == MessageTypes.FAILURE:
                    # TODO: encode/decode error info
                    raise HandlerError(response.body)
                elif response.msgtype == MessageTypes.ERROR:
                    # TODO: encode/decode error info
                    raise ServerError(response.body)
                return upper_result
            except (asyncio.TimeoutError, asyncio.CancelledError):
                cancel_request = Message.cancel(request)
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
