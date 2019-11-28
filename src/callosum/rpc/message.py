from __future__ import annotations

import enum
import sys
from typing import Any, Final, Optional

import attr
try:
    import snappy  # type: ignore
    has_snappy: Final = True
except ImportError:
    has_snappy: Final = False  # type: ignore

from ..abc import (
    AbstractSerializer, AbstractDeserializer,
    AbstractMessage, RawHeaderBody,
)
from ..exceptions import ConfigurationError
from ..serialize import mpackb, munpackb


# TODO(FUTURE): zero-copy serialization and de-serialization


class TupleEncodingMixin:
    '''
    Encodes the class values in order into a msgpack tuple
    and decodes the object from such msgpack tuples.

    The class must be an attrs class.
    '''

    @classmethod
    def decode(cls, buffer: bytes) -> Any:
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def encode(self) -> bytes:
        cls = type(self)
        values = [getattr(self, f.name) for f in attr.fields(cls)]
        return mpackb(values)


class Metadata(TupleEncodingMixin, object):
    '''
    Base type for metadata.
    '''
    pass


@attr.dataclass(frozen=True, slots=True)
class FunctionMetadata(Metadata):
    pass


@attr.dataclass(frozen=True, slots=True)
class ResultMetadata(Metadata):
    pass


@attr.dataclass(frozen=True, slots=True)
class StreamMetadata(Metadata):
    resource_name: str
    length: int


@attr.dataclass(frozen=True, slots=True)
class ErrorMetadata(Metadata):
    name: str
    stack: str


@attr.dataclass(frozen=True, slots=True)
class NullMetadata(Metadata):
    pass


class RPCMessageTypes(enum.IntEnum):
    FUNCTION = 0
    STREAM = 1
    RESULT = 2   # result of functions
    FAILURE = 3  # error from user handlers
    ERROR = 4    # error from callosum or underlying libraries
    CANCEL = 5   # client-side timeout or cancel request


metadata_types = (
    FunctionMetadata,
    StreamMetadata,
    ResultMetadata,
    ErrorMetadata,
    ErrorMetadata,
    NullMetadata,
)


@attr.dataclass(frozen=True, slots=True, auto_attribs=True)
class RPCMessage(AbstractMessage):
    # header parts
    msgtype: RPCMessageTypes
    method: str        # function/stream ID
    order_key: str  # replied back as-is
    seq_id: int      # replied back as-is

    # body parts (compressable)
    metadata: Optional[Metadata]
    body: bytes

    @property
    def request_id(self):
        return (self.method, self.order_key, self.seq_id)

    @classmethod
    def result(cls, request, result_body):
        return cls(
            RPCMessageTypes.RESULT,
            request.method, request.order_key, request.seq_id,
            ResultMetadata(),
            result_body,
        )

    @classmethod
    def failure(cls, request, exc):
        return cls(
            RPCMessageTypes.FAILURE,
            request.method, request.order_key, request.seq_id,
            ErrorMetadata(type(exc).__name__, ''),  # TODO: format stack
            mpackb(tuple(map(str, exc.args))),
        )

    @classmethod
    def error(cls, request, exc_info=None):
        if exc_info is None:
            exc_info = sys.exc_info()
        return cls(
            RPCMessageTypes.ERROR,
            request.method, request.order_key, request.seq_id,
            ErrorMetadata(exc_info[0].__name__, ''),
            mpackb(list(map(str, exc_info[1].args))),
        )

    @classmethod
    def cancel(cls, request):
        return cls(
            RPCMessageTypes.CANCEL,
            request.method, request.order_key, request.seq_id,
            NullMetadata(), {},
        )

    @classmethod
    def decode(cls, raw_msg: RawHeaderBody,
               deserializer: AbstractDeserializer) -> RPCMessage:
        header = munpackb(raw_msg[0])
        msgtype = RPCMessageTypes(header['type'])
        compressed = header['zip']
        raw_data: bytes = raw_msg[1]
        if compressed:
            if not has_snappy:
                raise ConfigurationError('python-snappy is not installed')
            raw_data = snappy.decompress(raw_data)
        data = munpackb(raw_data)
        metadata = metadata_types[msgtype].decode(data['meta'])
        return cls(msgtype,
                   header['meth'],
                   header['okey'],
                   header['seq'],
                   metadata,
                   deserializer(data['body']))

    def encode(self, serializer: AbstractSerializer, compress: bool = False) \
              -> RawHeaderBody:
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
        serialized_header: bytes = mpackb(header)
        if self.msgtype in (RPCMessageTypes.FUNCTION,
                            RPCMessageTypes.RESULT,
                            RPCMessageTypes.CANCEL):
            body = serializer(self.body)
        else:
            body = self.body
        data = {
            'meta': metadata,
            'body': body,
        }
        serialized_data: bytes = mpackb(data)
        if compress:
            if not has_snappy:
                raise ConfigurationError('python-snappy is not installed')
            serialized_data = snappy.compress(serialized_data)
        return (serialized_header, serialized_data)
