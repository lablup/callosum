from dataclasses import dataclass
import enum
import struct
from typing import Optional

import msgpack
import snappy


def mpackb(v):
    return msgpack.packb(v, use_bin_type=True)


def munpackb(b):
    return msgpack.unpackb(b, encoding='utf8', use_list=False)


class MessageTypes(enum.IntEnum):
    FUNCTION = 0
    STREAM = 1
    RESULT = 2   # result of functions
    FAILURE = 3  # error from user handlers
    ERROR = 4    # error from callosum or underlying libraries
    CANCEL = 5   # client-side timeout/cancel


@dataclass(frozen=True)
class FunctionMetadata:
    compressed: bool

    @classmethod
    def from_bytes(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def to_bytes(self):
        return mpackb((self.compressed, ))


@dataclass
class ResultMetadata:
    compressed: bool

    @classmethod
    def from_bytes(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def to_bytes(self):
        return mpackb((self.compressed, ))


@dataclass(frozen=True)
class StreamMetadata:
    compressed: bool
    resource_name: str
    length: int

    @classmethod
    def from_bytes(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def to_bytes(self):
        return mpackb((self.resource_name, self.length))


@dataclass(frozen=True)
class ErrorMetadata:
    name: str
    stack: str

    @classmethod
    def from_bytes(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def to_bytes(self):
        return mpackb((self.name, self.stack))


@dataclass(frozen=True)
class NullMetadata:

    @classmethod
    def from_bytes(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def to_bytes(self):
        return mpackb(tuple())


metadata_types = (
    FunctionMetadata,
    StreamMetadata,
    ResultMetadata,
    ErrorMetadata,
    ErrorMetadata,
    NullMetadata,
)


@dataclass(frozen=True)
class Message:

    __slots__ = (
        'msgtype', 'method',
        'order_key', 'seq_id',
        'metadata', 'body')

    msgtype: MessageTypes
    method: str        # function/stream ID
    order_key: str  # replied back as-is
    seq_id: int      # replied back as-is
    metadata: Optional[StreamMetadata]
    body: bytes

    @classmethod
    def result(cls, request, result_body):
        return cls(
            MessageTypes.RESULT,
            request.method, request.order_key, request.seq_id,
            ResultMetadata(True),
            result_body,
        )

    @classmethod
    def failure(cls, request, exc):
        return cls(
            MessageTypes.FAILURE,
            request.method, request.order_key, request.seq_id,
            ErrorMetadata(type(exc).__name__, ''),  # TODO: format stack
            mpackb(tuple(map(str, exc.args))),
        )

    @classmethod
    def error(cls, request, exc):
        return cls(
            MessageTypes.ERROR,
            request.method, request.order_key, request.seq_id,
            ErrorMetadata(type(exc).__name__, ''),
            mpackb(tuple(map(str, exc.args))),
        )

    @classmethod
    def cancel(cls, request):
        return cls(
            MessageTypes.CANCEL,
            request.method, request.order_key, request.seq_id,
            NullMetadata(), b'',
        )

    @classmethod
    def from_zmsg(cls, zmsg, deserializer):
        header = munpackb(zmsg[0])
        assert isinstance(header['type'], int)
        assert isinstance(header['meth'], str)
        assert isinstance(header['okey'], str)
        assert isinstance(header['seq'], int)
        assert isinstance(header['zip'], bool)
        msgtype = MessageTypes(header['type'])
        compressed = header['zip']
        data = zmsg[1]
        if compressed:
            data = snappy.decompress(data)
        data = munpackb(data)
        assert isinstance(data['meta'], bytes)
        metadata = metadata_types[msgtype].from_bytes(data['meta'])
        return cls(msgtype,
                   header['meth'],
                   header['okey'],
                   header['seq'],
                   metadata,
                   deserializer(data['body']))

    def to_zmsg(self, serializer, compress=True):
        metadata = b''
        if self.metadata is not None:
            metadata = self.metadata.to_bytes()
        header = {
            'type': int(self.msgtype),
            'meth': self.method,
            'okey': self.order_key,
            'seq': self.seq_id,
            'zip': compress,
        }
        header = mpackb(header)
        if self.msgtype in (MessageTypes.FUNCTION, MessageTypes.RESULT):
            body = serializer(self.body)
        else:
            body = self.body
        data = {
            'meta': metadata,
            'body': body,
        }
        data = mpackb(data)
        if compress:
            data = snappy.compress(data)
        return (header, data)
