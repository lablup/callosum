from __future__ import annotations

import abc
from typing import (
    Any, Final, Optional,
    Protocol,
    NamedTuple,
)


class RawHeaderBody(NamedTuple):
    header: bytes
    body: bytes
    peer_id: Optional[bytes]


class Sentinel(object):
    '''
    A category of special singleton objects that represents
    control-plane events in data-plane RX/TX queues.
    '''
    pass


'''
A sentinel object that represents the closing event of a queue.
'''
CLOSED: Final = Sentinel()


'''
A sentinel object that represents task cancellation.
'''
CANCELLED: Final = Sentinel()


class AbstractSerializer(Protocol):

    def __call__(self, obj: Optional[Any], /) -> bytes:  # noqa: E225
        ...


class AbstractDeserializer(Protocol):

    def __call__(self, data: bytes, /) -> Optional[Any]:  # noqa: E225
        ...


class AbstractMessage(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def decode(cls, raw_msg: RawHeaderBody,
               deserializer: AbstractDeserializer) -> AbstractMessage:
        '''
        Decodes the message and applies deserializer to the body.
        Returns an instance of inheriting message class.

        Args:
            deserializer: Body deserializer.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self, serializer: AbstractSerializer) -> RawHeaderBody:
        '''
        Encodes the message and applies serializer to body.

        Args:
            serializer: Body serializer.
        '''
        raise NotImplementedError


class AbstractChannel(metaclass=abc.ABCMeta):

    async def __aenter__(self) -> AbstractChannel:
        return self

    async def __aexit__(self, *exc_info) -> None:
        pass
