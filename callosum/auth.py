import abc

import attr
import zmq


@attr.dataclass(frozen=True, slots=True)
class AuthResult:
    success: bool
    user_id: str = None


def create_keypair(self):
    public_key, private_key = zmq.curve_keypair()
    return public_key, private_key


class AbstractAuthenticator(metaclass=abc.ABCMeta):
    '''
    Users of Callosum should subclass this to implement custom authentication.
    '''

    def __init__(self, transport):
        self.transport = transport

    # === Binder APIs ===

    @abc.abstractmethod
    async def server_identity(self) -> bytes:
        '''
        Return the private key of the server.
        Only used by the binder.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def check_client(self, domain: str, key: bytes) -> AuthResult:
        '''
        Check if the given domain and client public key is a valid one or not.
        Only used by the binder.
        '''
        raise NotImplementedError

    # === Connector APIs ===

    @abc.abstractmethod
    async def server_public_key(self) -> bytes:
        '''
        Return the public key of the server.
        Only used by the connector.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def client_identity(self) -> bytes:
        '''
        Return the private key of the client.
        Only used by the connector.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def client_public_key(self) -> bytes:
        '''
        Return the public key of the client.
        Only used by the connector.
        '''
        raise NotImplementedError
