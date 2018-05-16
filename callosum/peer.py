import asyncio
import io
from typing import Callable
import secrets

import aiojobs
from async_timeout import timeout

from .exceptions import ServerError, HandlerError
from .io import AsyncBytesIO
from .message import Message, MessageTypes
from .lower.zeromq import ZeroMQTransport


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


class Peer:

    def __init__(self, *,
                 connect: str=None,
                 bind: str=None,
                 serializer: Callable=None,
                 deserializer: Callable=None,
                 compress: bool=True,
                 max_body_size: int=10 * (2**20),  # 10 MiBytes
                 max_concurrency: int=100,
                 execute_timeout: float=None,
                 invoke_timeout: float=None):

        if connect is None and bind is None:
            raise ValueError('You must specify either connect or bind.')
        self._connect = connect
        self._bind = bind
        self._compress = compress
        self._serializer = _wrap_serializer(serializer)
        self._deserializer = _wrap_deserializer(deserializer)
        self._max_concurrency = max_concurrency
        self._exec_timeout = execute_timeout
        self._invoke_timeout = invoke_timeout

        self._scheduler = None
        self._transport = ZeroMQTransport()
        self._func_registry = {}
        self._stream_registry = {}

    # TODO: add handle_service??

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

    async def listen(self):
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        await self._transport.bind(self._bind)
        while True:
            raw_msg = await self._transport.recv_message()
            request = Message.from_zmsg(raw_msg, self._deserializer)
            try:
                handler = self._lookup(request.msgtype, request.method)
                job = await self._scheduler.spawn(
                    handler(request))
                # keep outstanding set of jobs identified by the request
                # so that the client can cancel them.
                # TODO: implement per-key ordering
                result = await job.wait()
            except (asyncio.TimeoutError, asyncio.CancelledError):
                raise
            except Exception as e:
                response = Message.error(request, e)
            else:
                response = Message.result(request, result)
            await self._transport.send_message(
                response.to_zmsg(self._serializer, self._compress))

    async def close(self):
        if self._scheduler is not None:
            await self._scheduler.close()
        if self._transport is not None:
            await self._transport.close()

    async def invoke(self, method, body, *, order_key=None, invoke_timeout=None):
        await self._transport.connect(self._connect)
        if invoke_timeout is None:
            invoke_timeout = self._invoke_timeout
        if order_key is None:
            order_key = secrets.token_hex(8)
            seq_id = 0
        try:
            request = None
            with timeout(invoke_timeout):
                if callable(body):
                    reader = AsyncBytesIO()
                    writer = AsyncBytesIO()

                    def identity(val):
                        return val

                    async def send_hook():
                        nonlocal request, response
                        request = Message(
                            MessageTypes.FUNCTION,
                            method,
                            order_key,
                            seq_id,
                            None,
                            writer.getvalue(),
                        )
                        await self._transport.send_message(
                            request.to_zmsg(identity, self._compress))
                        zmsg = await self._transport.recv_message()
                        response = Message.from_zmsg(zmsg, identity)
                        # TODO: handle "outer" protocol errors
                        reader.write(response.body)
                        reader.seek(0, io.SEEK_SET)

                    return await body(reader, writer, send_hook)
                    # TODO: how to handle "inner" protocol errors?
                else:
                    request = Message(
                        MessageTypes.FUNCTION,
                        method,
                        order_key,
                        seq_id,
                        None,
                        body,
                    )
                    await self._transport.send_message(
                        request.to_zmsg(self._serializer, self._compress))
                    zmsg = await self._transport.recv_message()
                    response = Message.from_zmsg(zmsg, self._deserializer)
                    if response.msgtype == MessageTypes.RESULT:
                        pass
                    elif response.msgtype == MessageTypes.FAILURE:
                        # TODO: encode/decode error info
                        raise HandlerError(response.body)
                    elif response.msgtype == MessageTypes.ERROR:
                        # TODO: encode/decode error info
                        raise ServerError(response.body)
                    return response.body
        except (asyncio.TimeoutError, asyncio.CancelledError):
            cancel_request = Message.cancel(request)
            await self._transport.send_message(
                cancel_request.to_zmsg(self._serializer))
            raise
        except Exception:
            raise

    async def send_stream(self, order_key, metadata, stream, *, reporthook=None):
        raise NotImplementedError
