from __future__ import annotations

import abc
from typing import (
    Any,
    Callable,
    Tuple,
)
try:
    from typing import Final  # type: ignore
except ImportError:
    from typing_extensions import Final
import msgpack


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


class AbstractMessage(Sentinel, metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def decode(cls, raw_msg: RawHeaderBody, deserializer) -> AbstractMessage:
        '''
        Decodes the message and applies deserializer to the body.
        Returns an instance of inheriting message class.

        Args:
            deserializer: Body deserializer.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self, serializer) -> RawHeaderBody:
        '''
        Encodes the message and applies serializer to body.

        Args:
            serializer: Body serializer.
        '''
        raise NotImplementedError

    @staticmethod
    def mpackb(v):
        return msgpack.packb(v, use_bin_type=True)

    @staticmethod
    def munpackb(b):
        return msgpack.unpackb(b, raw=False, use_list=False)


FunctionHandler = Callable[..., Any]
StreamHandler = Callable[..., Any]
