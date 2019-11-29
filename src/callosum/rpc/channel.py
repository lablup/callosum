from __future__ import annotations

import asyncio
import functools
import logging
from typing import (
    Any, Callable, Optional, Type, Union,
    Mapping, MutableMapping,
)
import secrets

import aiojobs
from async_timeout import timeout
import attr

from ..abc import (
    Sentinel, CLOSED, CANCELLED,
    FunctionHandler, StreamHandler,
)
from ..auth import AbstractAuthenticator
from ..exceptions import ServerError, HandlerError
from ..ordering import (
    AsyncResolver, AbstractAsyncScheduler,
    KeySerializedAsyncScheduler, SEQ_BITS,
)
from ..lower import (
    AbstractAddress,
    AbstractConnection,
    BaseTransport,
)
from .message import RPCMessage, RPCMessageTypes


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
    Represents a bidirectional connection where both sides can invoke each
    other.

    In Callosum, there is no fixed server or client for a connection.
    Once the connection is established, each peer can become both
    RPC client and RPC server.
    '''

    _connection: Optional[AbstractConnection]
    _func_registry: MutableMapping[str, FunctionHandler]
    _stream_registry: MutableMapping[str, StreamHandler]
    _streaming_chunks: asyncio.Queue[RPCMessage]
    _outgoing_queue: asyncio.Queue[Union[Sentinel, RPCMessage]]
    _recv_task: Optional[asyncio.Task]
    _send_task: Optional[asyncio.Task]

    def __init__(self, *,
                 connect: AbstractAddress = None,
                 bind: AbstractAddress = None,
                 serializer: Callable = None,
                 deserializer: Callable = None,
                 transport: Type[BaseTransport] = None,
                 authenticator: AbstractAuthenticator = None,
                 transport_opts: Mapping[str, Any] = {},
                 scheduler: AbstractAsyncScheduler = None,
                 compress: bool = True,
                 max_body_size: int = 10 * (2**20),  # 10 MiBytes
                 max_concurrency: int = 100,
                 execute_timeout: float = None,
                 invoke_timeout: float = None) -> None:

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
                                    transport_opts=transport_opts)
        self._func_registry = {}
        self._stream_registry = {}

        self._seq_id = 0

        # incoming queues
        self._streaming_chunks = asyncio.Queue()
        self._invocation_resolver = AsyncResolver()
        if scheduler is None:
            scheduler = KeySerializedAsyncScheduler()
        self._func_scheduler = scheduler

        # there is only one outgoing queue
        self._outgoing_queue = asyncio.Queue()
        self._recv_task = None
        self._send_task = None

        self._log = logging.getLogger(__name__ + '.Peer')

    def handle_function(self, method: str, handler: FunctionHandler) -> None:
        self._func_registry[method] = handler

    def handle_stream(self, method: str, handler: StreamHandler) -> None:
        self._stream_registry[method] = handler

    def unhandle_function(self, method: str) -> None:
        del self._func_registry[method]

    def unhandle_stream(self, method: str) -> None:
        del self._stream_registry[method]

    def _lookup(self, msgtype, method):
        if msgtype == RPCMessageTypes.FUNCTION:
            return self._func_registry[method]
        elif msgtype == RPCMessageTypes.STREAM:
            return self._stream_registry[method]
        raise ValueError('Invalid msgtype')

    async def _recv_loop(self) -> None:
        if self._connection is None:
            raise RuntimeError('consumer is not opened yet.')
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        while True:
            try:
                async for raw_msg in self._connection.recv_message():
                    # TODO: flow-control in transports or peer queues?
                    if raw_msg is None:
                        return
                    msg = RPCMessage.decode(raw_msg, self._deserializer)
                    if msg.msgtype == RPCMessageTypes.FUNCTION:
                        handler = self._lookup(msg.msgtype, msg.method)
                        asyncio.create_task(self._func_task(msg, handler))
                    elif msg.msgtype == RPCMessageTypes.CANCEL:
                        # TODO: change "await" to "create_task"
                        # and take care of that task tracking/deleting/cancellation.
                        await self._func_scheduler.cancel(msg.request_id)
                    elif msg.msgtype == RPCMessageTypes.STREAM:
                        self._streaming_chunks.put_nowait(msg)
                    elif msg.msgtype == RPCMessageTypes.RESULT:
                        self._invocation_resolver.resolve(msg.request_id, msg)
            except asyncio.CancelledError:
                break

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

    async def __aenter__(self) -> None:
        await self.open()

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def open(self) -> None:
        if self._connect:
            _opener = functools.partial(self._transport.connect,
                                        self._connect)()
        elif self._bind:
            _opener = functools.partial(self._transport.bind,
                                        self._bind)()
        else:
            raise RuntimeError('Misconfigured opener')
        self._opener = _opener
        self._connection = await _opener.__aenter__()
        # NOTE: if we change the order of the following 2 lines of code,
        # then there will be error after "flushall" redis.
        self._send_task = asyncio.create_task(self._send_loop())
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def close(self) -> None:
        if self._send_task is not None:
            await self._outgoing_queue.put(CLOSED)
            await self._send_task
        if self._recv_task is not None:
            # TODO: pass exception description, e.g. during invoke timeout
            if self._opener is not None:
                await self._opener.__aexit__(None, None, None)
            self._recv_task.cancel()
            await self._recv_task
        if self._scheduler is not None:
            await self._scheduler.close()
        if self._transport is not None:
            await self._transport.close()
        # TODO: add proper cleanup for awaiting on
        # finishing of the "listen" coroutine's spawned tasks

    async def _func_task(self, request: RPCMessage,
                         handler: FunctionHandler):
        rqst_id = request.request_id
        try:
            await self._func_scheduler.schedule(
                rqst_id,
                self._scheduler,
                handler(request))
            result = await self._func_scheduler.get_fut(rqst_id)
            await self._func_scheduler.cleanup(rqst_id)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            raise
        except Exception as e:
            self._log.error('Uncaught exception')
            response = RPCMessage.error(request, e)
        else:
            if result is CANCELLED:
                return
            assert not isinstance(result, Sentinel)
            response = RPCMessage.result(request, result)
        await self._outgoing_queue.put(response)

    async def invoke(self, method: str, body, *,
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
                request: RPCMessage
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
                # send cancel_request to connected peer
                cancel_request = RPCMessage.cancel(request)
                await self._outgoing_queue.put(cancel_request)

                # cancel invocation within this peer
                self._invocation_resolver.cancel(request.request_id)

                await self.close()
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
