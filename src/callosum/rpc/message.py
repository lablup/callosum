from __future__ import annotations

import enum
import sys
import traceback
from typing import Any, Final, Optional

import attrs

try:
    import snappy  # type: ignore

    has_snappy: Final = True
except ImportError:
    has_snappy: Final = False  # type: ignore

from ..abc import (
    AbstractDeserializer,
    AbstractMessage,
    AbstractSerializer,
    RawHeaderBody,
)
from ..exceptions import ConfigurationError
from ..serialize import mpackb, munpackb
from .types import RequestId

# TODO(FUTURE): zero-copy serialization and de-serialization


class TupleEncodingMixin:
    """
    Encodes the class values in order into a msgpack tuple
    and decodes the object from such msgpack tuples.

    The class must be an attrs class.
    """

    @classmethod
    def decode(cls, buffer: bytes) -> Any:
        if not buffer:
            return None
        return cls(*munpackb(buffer))

    def encode(self) -> bytes:
        cls = type(self)
        fields = attrs.fields(cls)  # type: ignore
        values = [getattr(self, f.name) for f in fields]
        return mpackb(values)


class Metadata(TupleEncodingMixin, object):
    """
    Base type for metadata.
    """

    pass


@attrs.define(frozen=True, slots=True)
class FunctionMetadata(Metadata):
    pass


@attrs.define(frozen=True, slots=True)
class ResultMetadata(Metadata):
    pass


@attrs.define(frozen=True, slots=True)
class ErrorMetadata(Metadata):
    name: str
    repr: str
    traceback: str


@attrs.define(frozen=True, slots=True)
class NullMetadata(Metadata):
    pass


class RPCMessageTypes(enum.IntEnum):
    NULL = 0
    FUNCTION = 1  # request for a function
    RESULT = 2  # result of functions
    FAILURE = 3  # error from user handlers
    ERROR = 4  # error from callosum or underlying libraries
    CANCEL = 5  # client-side timeout or cancel request
    CANCELLED = 6  # server-side timeout or cancel request


# mapped from RPCMessageTypes as index
metadata_types = (
    NullMetadata,
    FunctionMetadata,
    ResultMetadata,
    ErrorMetadata,
    ErrorMetadata,  # intended duplication
    NullMetadata,
    NullMetadata,
)


@attrs.define(frozen=True, slots=True, auto_attribs=True)
class RPCMessage(AbstractMessage):
    # transport-layer annotations
    peer_id: Optional[Any]

    # header parts
    msgtype: RPCMessageTypes
    method: str  # function ID
    order_key: str  # replied back as-is
    client_seq_id: int  # replied back as-is

    # body parts (compressable)
    metadata: Optional[Metadata]
    body: Optional[Any]

    @property
    def request_id(self) -> RequestId:
        return (self.method, self.order_key, self.client_seq_id)

    @classmethod
    def result(cls, request, result_body):
        """
        Creates an RPCMessage instance represents a execution result.
        """
        return cls(
            request.peer_id,
            RPCMessageTypes.RESULT,
            request.method,
            request.order_key,
            request.client_seq_id,
            ResultMetadata(),
            result_body,
        )

    @classmethod
    def failure(cls, request):
        """
        Creates an RPCMessage instance containing exception information,
        when the exception is from user-defined handlers or upper adaptation layers.

        It must be called in an exception handler context, where ``sys.exc_info()``
        returns a non-null tuple.
        """
        exc_info = sys.exc_info()
        return cls(
            request.peer_id,
            RPCMessageTypes.FAILURE,
            request.method,
            request.order_key,
            request.client_seq_id,
            ErrorMetadata(
                exc_info[0].__name__,
                repr(exc_info[1]),
                traceback.format_exc(),
            ),
            None,
        )

    @classmethod
    def error(cls, request):
        """
        Creates an RPCMessage instance containing exception information,
        when the exception is from Callosum's internals.

        It must be called in an exception handler context, where ``sys.exc_info()``
        returns a non-null tuple.
        """
        exc_info = sys.exc_info()
        return cls(
            request.peer_id,
            RPCMessageTypes.ERROR,
            request.method,
            request.order_key,
            request.client_seq_id,
            ErrorMetadata(
                exc_info[0].__name__,
                repr(exc_info[1]),
                traceback.format_exc(),
            ),
            None,
        )

    @classmethod
    def cancel(cls, request):
        """
        Creates an RPCMessage instance represents a cancellation of
        the given request triggered from the client-side.
        """
        return cls(
            request.peer_id,
            RPCMessageTypes.CANCEL,
            request.method,
            request.order_key,
            request.client_seq_id,
            NullMetadata(),
            None,
        )

    @classmethod
    def cancelled(cls, request):
        """
        Creates an RPCMessage instance represents a cancellation of
        the given request triggered from the server-side.
        """
        return cls(
            request.peer_id,
            RPCMessageTypes.CANCELLED,
            request.method,
            request.order_key,
            request.client_seq_id,
            NullMetadata(),
            None,
        )

    @classmethod
    def decode(
        cls, raw_msg: RawHeaderBody, deserializer: AbstractDeserializer
    ) -> RPCMessage:
        header = munpackb(raw_msg.header)
        msgtype = RPCMessageTypes(header["type"])
        compressed = header["zip"]
        raw_data = raw_msg.body
        if compressed:
            if not has_snappy:
                raise ConfigurationError("python-snappy is not installed")
            raw_data = snappy.decompress(raw_data)
        data = munpackb(raw_data)
        metadata = metadata_types[msgtype].decode(data["meta"])
        if msgtype in (RPCMessageTypes.FUNCTION, RPCMessageTypes.RESULT):
            body = deserializer(data["body"])
        else:
            body = data["body"]
        return cls(
            raw_msg.peer_id,
            msgtype,
            header["meth"],
            header["okey"],
            header["seq"],
            metadata,
            body,
        )

    def encode(
        self, serializer: AbstractSerializer, compress: bool = False
    ) -> RawHeaderBody:
        metadata = b""
        if self.metadata is not None:
            metadata = self.metadata.encode()
        header = {
            "type": int(self.msgtype),
            "meth": self.method,
            "okey": self.order_key,
            "seq": self.client_seq_id,
            "zip": compress,
        }
        serialized_header: bytes = mpackb(header)
        body: Optional[bytes]
        if self.msgtype in (RPCMessageTypes.FUNCTION, RPCMessageTypes.RESULT):
            body = serializer(self.body)
        else:
            body = self.body
        data = {
            "meta": metadata,
            "body": body,
        }
        serialized_data: bytes = mpackb(data)
        if compress:
            if not has_snappy:
                raise ConfigurationError("python-snappy is not installed")
            serialized_data = snappy.compress(serialized_data)
        return RawHeaderBody(
            serialized_header,
            serialized_data,
            self.peer_id,
        )
