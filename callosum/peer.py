import asyncio
from dataclasses import dataclass
import enum
import secrets
import struct
from typing import Optional

import aiojobs
from async_timeout import timeout
import msgpack
import zmq, zmq.asyncio

from .exceptions import ServerError, HandlerError


class MessageTypes(enum.IntEnum):
    FUNCTION = 0
    STREAM = 1
    FAILURE = 2  # error from user handlers
    ERROR = 3    # error from callosum or underlying libraries


@dataclass(frozen=True)
class StreamMetadata:
    resource_name: str
    length: int

    @classmethod
    def from_bytes(cls, buffer):
        return cls(*msgpack.unpackb(buffer, encoding='utf8', use_list=False))

    def to_bytes(self):
        return msgpack.packb((self.resource_name, self.length), use_bin_type=True)


@dataclass(frozen=True)
class Message:
    msgtype: MessageTypes
    identifier: str        # function/stream ID
    client_order_key: str  # replied back as-is
    client_order: int      # replied back as-is
    metadata: Optional[StreamMetadata]
    body: bytes

    @classmethod
    def from_zmsg(cls, zmsg, deserializer):
        assert len(zmsg) == 6
        metadata = zmsg[4]
        if len(metadata) > 0:
            metadata = StreamMetadata.from_bytes(metadata)
        else:
            metadata = None
        return cls(MessageTypes(struct.unpack('!l', zmsg[0])[0]),
                   zmsg[1].decode('utf8'),
                   zmsg[2].decode('utf8'),
                   int(struct.unpack('!Q', zmsg[3])[0]),
                   metadata,
                   deserializer(zmsg[5]))

    def to_zmsg(self, serializer):
        return (
            struct.pack('!l', self.msgtype),
            self.identifier.encode('utf8'),
            self.client_order_key.encode('utf8'),
            struct.pack('!Q', self.client_order),
            self.metadata.to_bytes() if self.metadata else b'',
            serializer(self.body),
        )


def _wrap_serializer(serializer, compress=False):
    def _serialize(value):
        if serializer is not None:
            value = serializer(value)
        if isinstance(value, str):
            return value.encode('utf8')
        return value
    return _serialize


def _wrap_deserializer(deserializer, compress=False):
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
                 order_key_timeout=60.0,
                 invoke_timeout=30.0):

        if connect is None and bind is None:
            raise ValueError('You must specify either connect or bind.')
        self._connect = connect
        self._bind = bind
        self._compress = compress
        self._serializer = _wrap_serializer(serializer, compress)
        self._deserializer = _wrap_deserializer(deserializer, compress)
        self._max_concurrency = max_concurrency
        self._connect_timeout = connect_timeout
        self._order_key_timeout = order_key_timeout
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
            except LookupError:
                # TODO: reply error
                continue
            job = await self._scheduler.spawn(
                handler(request.identifier, request.body))
            result = await job.wait()
            response = Message(
                request.msgtype,
                request.identifier,
                request.client_order_key,
                request.client_order,
                None,
                result,
            )
            await self._server_sock.send_multipart(
                response.to_zmsg(self._serializer))

    async def close(self):
        if self._scheduler:
            await self._scheduler.close()
        if self._server_sock:
            self._server_sock.close()
        if self._client_sock:
            self._client_sock.close()
        if self._zctx:
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
        with timeout(invoke_timeout):
            request = Message(
                MessageTypes.FUNCTION,
                func_id,
                order_key,
                order_val,
                None,
                body,
            )
            await self._client_sock.send_multipart(request.to_zmsg(self._serializer))
            # TODO: implement per-key ordering
            zmsg = await self._client_sock.recv_multipart()
            response = Message.from_zmsg(zmsg, self._deserializer)
            if response.msgtype == MessageTypes.FAILURE:
                # TODO: encode/decode error info
                raise HandlerError(response.body)
            elif response.msgtype == MessageTypes.ERROR:
                # TODO: encode/decode error info
                raise ServerError(response.body)
            return response.body

    async def send_stream(self, order_key, metadata, stream, *, reporthook=None):
        raise NotImplementedError
