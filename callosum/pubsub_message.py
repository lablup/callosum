from __future__ import annotations

import datetime
from typing import (
    Any,
)

import attr
from .abc import (
    AbstractMessage,
    RawHeaderBody,
)


@attr.dataclass(frozen=True, slots=True, auto_attribs=True)
class PubSubMessage(AbstractMessage):
    # header parts
    timestamp: datetime.datetime

    # body parts
    body: Any

    @property
    def header(self):
        return self.timestamp

    @classmethod
    def create(cls, timestamp: datetime.datetime, body: Any):
        return cls(timestamp, body)

    @classmethod
    def decode(cls, raw_msg: RawHeaderBody, deserializer) -> PubSubMessage:
        header = cls.munpackb(raw_msg[0])
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp = datetime.datetime.strptime(header['timestamp'], fmt)
        body = cls.munpackb(raw_msg[1])
        return cls(timestamp, deserializer(body))

    def encode(self, serializer) -> RawHeaderBody:
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp: str = self.timestamp.strftime(fmt)
        header = {
            'timestamp': timestamp,
        }
        serialized_header: bytes = self.mpackb(header)
        body = serializer(self.body)
        serialized_body: bytes = self.mpackb(body)
        return (serialized_header, serialized_body)
