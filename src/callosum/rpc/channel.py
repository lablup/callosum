from __future__ import annotations

import asyncio
import functools
import logging
from typing import (
    Any, Optional, Type, Union,
    Mapping, MutableMapping,
    TYPE_CHECKING,
)
import secrets

import aiojobs
from aiotools import aclosing
from async_timeout import timeout
import attr

from ..abc import (
    Sentinel, CLOSED, CANCELLED,
    AbstractChannel,
    AbstractDeserializer, AbstractSerializer,
)
from ..auth import AbstractAuthenticator
from .exceptions import RPCUserError, RPCInternalError
from ..ordering import (
    AsyncResolver, AbstractAsyncScheduler,
    KeySerializedAsyncScheduler, SEQ_BITS,
)
from ..lower import (
    AbstractAddress,
    AbstractConnection,
    AbstractBinder, AbstractConnector,
    BaseTransport,
)
from .message import (
    RPCMessage, RPCMessageTypes,
)
if TYPE_CHECKING:
    from . import FunctionHandler

log = logging.getLogger(__name__)


class Peer(AbstractChannel):
    '''
    Represents a bidirectional connection where both sides can invoke each
    other.

    In Callosum, there is no fixed server or client for a connection.
    Once the connection is established, each peer can become both
    RPC client and RPC server.
    '''

    _connection: Optional[AbstractConnection]
    _deserializer: AbstractDeserializer
    _serializer: AbstractSerializer
    _func_registry: MutableMapping[str, FunctionHandler]
    _outgoing_queue: asyncio.Queue[Union[Sentinel, RPCMessage]]
    _recv_task: Optional[asyncio.Task]
    _send_task: Optional[asyncio.Task]
    _opener: Optional[Union[AbstractBinder, AbstractConnector]]
    _log: logging.Logger
    _debug_rpc: bool

    def __init__(
        self, *,
        deserializer: AbstractDeserializer,
        serializer: AbstractSerializer,
        connect: AbstractAddress = None,
        bind: AbstractAddress = None,
        transport: Type[BaseTransport] = None,
        authenticator: AbstractAuthenticator = None,
        transport_opts: Mapping[str, Any] = {},
        scheduler: AbstractAsyncScheduler = None,
        compress: bool = True,
        max_body_size: int = 10 * (2**20),  # 10 MiBytes
        max_concurrency: int = 100,
        execute_timeout: float = None,
        invoke_timeout: float = None,
        debug_rpc: bool = False,
    ) -> None:
        if connect is None and bind is None:
            raise ValueError('You must specify either the connect or bind address.')
        self._connect = connect
        self._bind = bind
        self._opener = None
        self._connection = None
        self._compress = compress
        self._deserializer = deserializer
        self._serializer = serializer
        self._max_concurrency = max_concurrency
        self._exec_timeout = execute_timeout
        self._invoke_timeout = invoke_timeout

        self._scheduler = None
        if transport is None:
            raise ValueError('You must provide a transport class.')
        self._transport = transport(authenticator=authenticator,
                                    transport_opts=transport_opts)
        self._func_registry = {}

        self._seq_id = 0

        # incoming queues
        self._invocation_resolver = AsyncResolver()
        if scheduler is None:
            scheduler = KeySerializedAsyncScheduler()
        self._func_scheduler = scheduler

        # there is only one outgoing queue
        self._outgoing_queue = asyncio.Queue()
        self._recv_task = None
        self._send_task = None

        self._log = logging.getLogger(__name__ + '.Peer')
        self._debug_rpc = debug_rpc

    def handle_function(self, method: str, handler: FunctionHandler) -> None:
        self._func_registry[method] = handler

    def unhandle_function(self, method: str) -> None:
        del self._func_registry[method]

    def _lookup_func(self, method: str) -> FunctionHandler:
        return self._func_registry[method]

    async def _recv_loop(self) -> None:
        '''
        Receive requests and schedule the request handlers.
        '''
        if self._connection is None:
            raise RuntimeError('consumer is not opened yet.')
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        while True:
            try:
                async with aclosing(self._connection.recv_message()) as agen:
                    async for raw_msg in agen:
                        # TODO: flow-control in transports or peer queues?
                        if raw_msg is None:
                            return
                        msg = RPCMessage.decode(raw_msg, self._deserializer)
                        if msg.msgtype == RPCMessageTypes.FUNCTION:
                            func_handler = self._lookup_func(msg.method)
                            asyncio.create_task(self._func_task(msg, func_handler))
                        elif msg.msgtype == RPCMessageTypes.CANCEL:
                            # TODO: change "await" to "create_task"
                            # and take care of that task cancellation.
                            await self._func_scheduler.cancel(msg.request_id)
                        elif msg.msgtype in (RPCMessageTypes.RESULT,
                                             RPCMessageTypes.FAILURE,
                                             RPCMessageTypes.ERROR):
                            self._invocation_resolver.resolve(msg.request_id, msg)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error')

    async def _send_loop(self) -> None:
        '''
        Fetches and sends out the completed task responses.
        '''
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

    async def __aenter__(self) -> Peer:
        _opener: Union[AbstractBinder, AbstractConnector]
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
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._send_task is not None:
            await self._outgoing_queue.put(CLOSED)
            await self._send_task
        if self._recv_task is not None:
            # TODO: pass exception description, e.g. during invoke timeout
            if self._opener is not None:
                await self._opener.__aexit__(*exc_info)
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
            try:
                result = await self._func_scheduler.get_fut(rqst_id)
                if result is CANCELLED:
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                # exception from user handler => failure
                if self._debug_rpc:
                    self._log.exception('RPC user error')
                response = RPCMessage.failure(request)
            else:
                assert not isinstance(result, Sentinel)
                response = RPCMessage.result(request, result)
            finally:
                await self._func_scheduler.cleanup(rqst_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._debug_rpc:
                self._log.exception('RPC internal error')
            # exception from our parts => error
            response = RPCMessage.error(request)
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
        try:
            request: RPCMessage
            with timeout(invoke_timeout):
                if callable(body):
                    # The user is using an upper-layer adaptor.
                    async with aclosing(body()) as agen:
                        request = RPCMessage(
                            None,
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
                        None,
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
                raise RPCUserError(*attr.astuple(response.metadata))
            elif response.msgtype == RPCMessageTypes.ERROR:
                raise RPCInternalError(*attr.astuple(response.metadata))
            return upper_result
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # propagate cancellation to the connected peer
            cancel_request = RPCMessage.cancel(request)
            await self._outgoing_queue.put(cancel_request)
            # cancel myself as well
            self._invocation_resolver.cancel(request.request_id)
            raise
        except Exception:
            raise
