from __future__ import annotations

import abc
from typing import (
    Any, Final,
    Callable,
    Tuple,
    Protocol,
)


RawHeaderBody = Tuple[bytes, bytes]


class Sentinel(object):
    pass


'''
A sentinel object that represents the closing event of a queue.
'''
CLOSED: Final = Sentinel()


'''
A sentinel object that represents cancellation during RPC requests.
'''
CANCELLED: Final = Sentinel()


class AbstractSerializer(Protocol):

    def __call__(self, obj: Any) -> bytes:
        ...


class AbstractDeserializer(Protocol):

    def __call__(self, data: bytes) -> Any:
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


FunctionHandler = Callable[..., Any]
StreamHandler = Callable[..., Any]
