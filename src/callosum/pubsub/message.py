from __future__ import annotations

import datetime
from typing import (
    Any, Optional,
)

import attr
from ..abc import (
    AbstractSerializer, AbstractDeserializer,
    AbstractMessage, RawHeaderBody,
)
from ..serialize import mpackb, munpackb


@attr.dataclass(frozen=True, slots=True, auto_attribs=True)
class PubSubMessage(AbstractMessage):
    # header parts
    timestamp: datetime.datetime

    # body parts
    body: Any

    @property
    def header(self) -> datetime.datetime:
        return self.timestamp

    @classmethod
    def create(cls, timestamp: datetime.datetime, body: Any):
        return cls(timestamp, body)

    @classmethod
    def decode(cls, raw_msg: RawHeaderBody,
               deserializer: AbstractDeserializer) -> PubSubMessage:
        header = munpackb(raw_msg[0])
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp = datetime.datetime.strptime(header['timestamp'], fmt)
        return cls(timestamp, deserializer(raw_msg[1]))

    def encode(self, serializer: AbstractSerializer) -> RawHeaderBody:
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp: str = self.timestamp.strftime(fmt)
        header = {
            'timestamp': timestamp,
        }
        serialized_header: bytes = mpackb(header)
        serialized_body: bytes = serializer(self.body)
        return (serialized_header, serialized_body)
