import enum
import sys
from typing import Optional, Tuple

import attr
import msgpack
import snappy


# TODO(FUTURE): zero-copy serialization and de-serialization


def mpackb(v):
    return msgpack.packb(v, use_bin_type=True)


def munpackb(b):
    return msgpack.unpackb(b, raw=False, use_list=False)


class TupleEncodingMixin:
    '''
    Encodes the class values in order into a msgpack tuple
    and decodes the object from such msgpack tuples.

    The class must be an attrs class.
    '''

    __slots__ = tuple()

    @classmethod
    def decode(cls, buffer):
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def encode(self):
        cls = type(self)
        values = [getattr(self, f.name) for f in attr.fields(cls)]
        return mpackb(values)


class Metadata(TupleEncodingMixin, object):
    '''
    Base type for metadata.
    '''
    pass


@attr.s(auto_attribs=True, frozen=True, slots=True)
class FunctionMetadata(Metadata):
    pass


@attr.s(auto_attribs=True, frozen=True, slots=True)
class ResultMetadata(Metadata):
    pass


@attr.s(auto_attribs=True, frozen=True, slots=True)
class StreamMetadata(Metadata):
    resource_name: str
    length: int


@attr.s(auto_attribs=True, frozen=True, slots=True)
class ErrorMetadata(Metadata):
    name: str
    stack: str


@attr.s(auto_attribs=True, frozen=True, slots=True)
class NullMetadata(Metadata):
    pass


class MessageTypes(enum.IntEnum):
    FUNCTION = 0
    STREAM = 1
    RESULT = 2   # result of functions
    FAILURE = 3  # error from user handlers
    ERROR = 4    # error from callosum or underlying libraries
    CANCEL = 5   # client-side timeout/cancel


metadata_types = (
    FunctionMetadata,
    StreamMetadata,
    ResultMetadata,
    ErrorMetadata,
    ErrorMetadata,
    NullMetadata,
)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Message:
    # header parts
    msgtype: MessageTypes
    method: str        # function/stream ID
    order_key: str  # replied back as-is
    seq_id: int      # replied back as-is

    # body parts (compressable)
    metadata: Optional[Metadata]
    body: bytes

    @classmethod
    def result(cls, request, result_body):
        return cls(
            MessageTypes.RESULT,
            request.method, request.order_key, request.seq_id,
            ResultMetadata(),
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
    def error(cls, request, exc_info=None):
        if exc_info is None:
            exc_info = sys.exc_info()
        return cls(
            MessageTypes.ERROR,
            request.method, request.order_key, request.seq_id,
            ErrorMetadata(exc_info[0].__name__, ''),
            mpackb(list(map(str, exc_info[1].args))),
        )

    @classmethod
    def cancel(cls, request):
        return cls(
            MessageTypes.CANCEL,
            request.method, request.order_key, request.seq_id,
            NullMetadata(), b'',
        )

    @classmethod
    def decode(cls, raw_msg: Tuple[bytes, bytes], deserializer):
        header = munpackb(raw_msg[0])
        msgtype = MessageTypes(header['type'])
        compressed = header['zip']
        data = raw_msg[1]
        if compressed:
            data = snappy.decompress(data)
        data = munpackb(data)
        metadata = metadata_types[msgtype].decode(data['meta'])
        return cls(msgtype,
                   header['meth'],
                   header['okey'],
                   header['seq'],
                   metadata,
                   deserializer(data['body']))

    def encode(self, serializer, compress: bool=True) -> Tuple[bytes, bytes]:
        metadata = b''
        if self.metadata is not None:
            metadata = self.metadata.encode()
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