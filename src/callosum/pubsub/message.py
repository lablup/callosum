from __future__ import annotations

from datetime import datetime
from typing import (
    Any,
)

import attr
from dateutil.tz import tzutc
import temporenc

from ..abc import (
    AbstractSerializer, AbstractDeserializer,
    AbstractMessage, RawHeaderBody,
)
from ..serialize import mpackb, munpackb


@attr.dataclass(frozen=True, slots=True, auto_attribs=True)
class StreamMessage(AbstractMessage):
    # header parts
    created_at: datetime

    # body parts
    body: Any

    @classmethod
    def create(cls, body: Any):
        created_at = datetime.now(tzutc())
        return cls(created_at, body)

    @classmethod
    def decode(cls, raw_msg: RawHeaderBody,
               deserializer: AbstractDeserializer) -> StreamMessage:
        header = munpackb(raw_msg[0])
        created_at = temporenc.unpackb(header[0]).datetime()
        return cls(created_at, deserializer(raw_msg[1]))

    def encode(self, serializer: AbstractSerializer) -> RawHeaderBody:
        header = [
            temporenc.packb(self.created_at),
        ]
        serialized_header: bytes = mpackb(header)
        serialized_body: bytes = serializer(self.body)
        return RawHeaderBody(serialized_header, serialized_body, None)
