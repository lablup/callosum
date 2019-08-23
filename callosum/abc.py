import abc
from typing import Tuple
import msgpack


'''
A sentinel object that can be used
to represent cancellation during
RPC requests of for other purposes.
'''
cancelled = object()


class AbstractMessage(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def decode(cls):
        '''
        Decodes the message and applies deserializer to the body.
        Returns an instance of inheriting message class.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self) -> Tuple[bytes, bytes]:
        '''
        Encodes the message and applies serializer to body.
        '''
        raise NotImplementedError

    @staticmethod
    def mpackb(v):
        return msgpack.packb(v, use_bin_type=True)

    @staticmethod
    def munpackb(b):
        return msgpack.unpackb(b, raw=False, use_list=False)
