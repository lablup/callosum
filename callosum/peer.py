import asyncio
import secrets

import aiojobs
from async_timeout import timeout
import snappy
import zmq, zmq.asyncio

from .exceptions import ServerError, HandlerError
from .message import Message, MessageTypes


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
                 connect=None,
                 bind=None,
                 serializer=None,
                 deserializer=None,
                 compress=True,
                 max_body_size=10 * (2**20),  # 10 MiBytes
                 max_concurrency=100,
                 connect_timeout=10.0,
                 invoke_timeout=30.0):

        if connect is None and bind is None:
            raise ValueError('You must specify either connect or bind.')
        self._connect = connect
        self._bind = bind
        self._compress = compress
        self._serializer = _wrap_serializer(serializer)
        self._deserializer = _wrap_deserializer(deserializer)
        self._max_concurrency = max_concurrency
        self._connect_timeout = connect_timeout
        self._invoke_timeout = invoke_timeout

        self._scheduler = None
        self._zctx = zmq.asyncio.Context()
        self._server_sock = None
        self._client_sock = None
        self._func_registry = {}
        self._stream_registry = {}

    def handle_function(self, func_id, handler):
        self._func_registry[func_id] = handler

    def handle_stream(self, stream_id, handler):
        self._func_registry[stream_id] = handler

    def unhandle_function(self, func_id):
        del self._func_registry[func_id]

    def unhandle_stream(self, stream_id):
        del self._func_registry[stream_id]

    def _lookup(self, msgtype, identifier):
        if msgtype == MessageTypes.FUNCTION:
            return self._func_registry[identifier]
        elif msgtype == MessageTypes.STREAM:
            return self._stream_registry[identifier]
        raise ValueError('Invalid msgtype')

    async def listen(self):
        if self._scheduler is None:
            self._scheduler = await aiojobs.create_scheduler(
                limit=self._max_concurrency,
            )
        if self._server_sock is None:
            self._server_sock = self._zctx.socket(zmq.PAIR)
            self._server_sock.setsockopt(zmq.LINGER, 100)
            self._server_sock.bind(self._bind)
        while True:
            raw_msg = await self._server_sock.recv_multipart()
            request = Message.from_zmsg(raw_msg, self._deserializer)
            try:
                handler = self._lookup(request.msgtype, request.identifier)
                job = await self._scheduler.spawn(
                    handler(request.identifier, request.body))
                # keep outstanding set of jobs identified by the request
                # so that the client can cancel them.
                result = await job.wait()
            except (asyncio.TimeoutError, asyncio.CancelledError):
                raise
            except Exception as e:
                response = Message.error(request, e)
            else:
                response = Message.result(request, result)
            await self._server_sock.send_multipart(
                response.to_zmsg(self._serializer, self._compress))

    async def close(self):
        if self._scheduler is not None:
            await self._scheduler.close()
        if self._server_sock is not None:
            self._server_sock.close()
        if self._client_sock is not None:
            self._client_sock.close()
        if self._zctx is not None:
            self._zctx.term()

    async def invoke(self, func_id, body, *, order_key=None, invoke_timeout=None):
        if self._client_sock is None:
            self._client_sock = self._zctx.socket(zmq.PAIR)
            self._client_sock.setsockopt(zmq.LINGER, 100)
            self._client_sock.connect(self._connect)
        if invoke_timeout is None:
            invoke_timeout = self._invoke_timeout
        if order_key is None:
            order_key = secrets.token_hex(8)
            order_val = 0
        try:
            with timeout(invoke_timeout):
                request = Message(
                    MessageTypes.FUNCTION,
                    func_id,
                    order_key,
                    order_val,
                    None,
                    body,
                )
                await self._client_sock.send_multipart(
                    request.to_zmsg(self._serializer, self._compress))
                # TODO: implement per-key ordering
                zmsg = await self._client_sock.recv_multipart()
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
            await self._client_sock.send_multipart(
                cancel_request.to_zmsg(self._serializer))
            raise

    async def send_stream(self, order_key, metadata, stream, *, reporthook=None):
        raise NotImplementedError
