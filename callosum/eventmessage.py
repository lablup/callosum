import enum
import datetime
from typing import Tuple, Callable
from dataclasses import dataclass

import attr
from aiohttp import web
from . import (
    AbstractMessage,
)


@dataclass
class EventHandler:
    app: web.Application
    callback: Callable


class EventTypes(enum.Enum):
    INSTANCE_STARTED = "instance_started"
    INSTANCE_TERMINATED = "instance_terminated"
    INSTANCE_HEARTBEAT = "instance_heartbeat"
    KERNEL_PREPARING = "kernel_preparing"
    KERNEL_CREATING = "kernel_creating"
    KERNEL_PULLING = "kernel_pulling"
    KERNEL_STARTED = "kernel_started"
    KERNEL_TERMINATED = "kernel_terminated"


@attr.dataclass(frozen=True, slots=True)
class EventMessage(AbstractMessage):
    # header parts
    event: EventTypes
    agent_id: str
    timestamp: datetime.datetime

    # body parts
    args: list

    @property
    def header(self):
        return (self.event, self.agent_id, self.timestamp)

    @property
    def body(self):
        return self.args

    @classmethod
    def create(cls, event: EventTypes,
               agent_id: str, timestamp: datetime.datetime, *args):
        return cls(event, agent_id,
                    timestamp, args)

    @classmethod
    def decode(cls, raw_msg: Tuple[bytes, bytes], deserializer):
        header = cls.munpackb(raw_msg[0])
        event = EventTypes(header['event'])
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp = datetime.datetime.strptime(header['timestamp'], fmt)
        body = cls.munpackb(raw_msg[1])
        return cls(event,
                   header['agent_id'],
                   timestamp,
                   deserializer(body['args']))

    def encode(self, serializer) \
              -> Tuple[bytes, bytes]:
        event = self.event.value
        # format string assumes that datetime object includes timezone!
        fmt = "%y/%m/%d, %H:%M:%S:%f, %z%Z"
        timestamp: str = self.timestamp.strftime(fmt)
        header = {
            'event': event,
            'agent_id': self.agent_id,
            'timestamp': timestamp,
        }
        serialized_header: bytes = self.mpackb(header)
        args = serializer(self.args)
        body = {
            'args': args,
        }
        serialized_body: bytes = self.mpackb(body)
        return (serialized_header, serialized_body)
