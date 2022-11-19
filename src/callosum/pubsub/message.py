from __future__ import annotations

from datetime import datetime
from typing import Any

import attrs
import temporenc
from dateutil.tz import tzutc

from ..abc import (
    AbstractDeserializer,
    AbstractMessage,
    AbstractSerializer,
    RawHeaderBody,
)
from ..serialize import mpackb, munpackb


@attrs.define(frozen=True, slots=True, auto_attribs=True)
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
    def decode(
        cls, raw_msg: RawHeaderBody, deserializer: AbstractDeserializer
    ) -> StreamMessage:
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
