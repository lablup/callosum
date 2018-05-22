import abc

import attr
import zmq


@attr.dataclass(frozen=True, slots=True)
class AuthResult:
    success: bool
    user_id: str = None


@attr.dataclass(frozen=True, slots=True)
class Identity:
    domain: str
    private_key: bytes


def create_keypair(self):
    public_key, private_key = zmq.curve_keypair()
    return public_key, private_key


class AbstractAuthenticator(metaclass=abc.ABCMeta):
    '''
    Users of Callosum should subclass this to implement custom authentication.
    '''

    # === Binder APIs ===

    @abc.abstractmethod
    async def server_identity(self) -> Identity:
        '''
        Return the identity of the server.
        Only used by the binder.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def check_client(self, client_id: Identity) -> AuthResult:
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
    async def client_identity(self) -> Identity:
        '''
        Return the identity of the client.
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
