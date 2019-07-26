import enum
import datetime
from typing import Tuple

import attr
from . import (
    AbstractMessage,
)


class EventTypes(enum.IntEnum):
    INSTANCE_STARTED = 0
    INSTANCE_TERMINATED = 1
    INSTANCE_HEARTBEAT = 2
    KERNEL_PREPARING = 3
    KERNEL_CREATING = 4
    KERNEL_PULLING = 5
    KERNEL_STARTED = 6
    KERNEL_TERMINATED = 7


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

    def encode(self, serializer, compress: bool = True) \
              -> Tuple[bytes, bytes]:
        event = int(self.event)
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
