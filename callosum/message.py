from dataclasses import dataclass
import enum
import struct
from typing import Optional

import msgpack


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
