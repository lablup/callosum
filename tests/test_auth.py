import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Tuple

import pytest
import zmq

from callosum.auth import (
    AbstractClientAuthenticator,
    AbstractServerAuthenticator,
    AuthResult,
    Credential,
    Identity,
    create_keypair,
)
from callosum.exceptions import AuthenticationError
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.ordering import ExitOrderedAsyncScheduler
from callosum.rpc import Peer


async def handle_echo(request):
    return {
        "received": request.body["sent"],
    }


server_keypair = create_keypair()
client1_keypair = create_keypair()
client2_keypair = create_keypair()


class MyServerAuthenticator(AbstractServerAuthenticator):
    def __init__(self, domain: str, server_keypair: Tuple[bytes, bytes]) -> None:
        self.domain = domain
        self._public_key = server_keypair[0]
        self._private_key = server_keypair[1]
        self.user_db = {
            client1_keypair[0]: "myuser1",
        }

    async def server_identity(self) -> Identity:
        return Identity(domain=self.domain, private_key=self._private_key)

    async def check_client(self, creds: Credential) -> AuthResult:
        if creds.domain != self.domain:
            return AuthResult(success=False)
        if user_id := self.user_db.get(creds.public_key, None):
            return AuthResult(
                success=True,
                user_id=user_id,
            )
        return AuthResult(success=False)

    async def server_public_key(self) -> bytes:
        return self._public_key

    async def client_identity(self) -> Identity:
        raise NotImplementedError

    async def client_public_key(self) -> bytes:
        raise NotImplementedError


class MyClientAuthenticator(AbstractClientAuthenticator):
    def __init__(
        self,
        domain: str,
        keypair: Tuple[bytes, bytes],
        server_public_key: bytes,
    ) -> None:
        self.domain = domain
        self._public_key = keypair[0]
        self._private_key = keypair[1]
        self._server_public_key = server_public_key

    async def server_identity(self) -> Identity:
        raise NotImplementedError

    async def check_client(self, creds: Credential) -> AuthResult:
        raise NotImplementedError

    async def server_public_key(self) -> bytes:
        return self._server_public_key

    async def client_identity(self) -> Identity:
        return Identity(domain=self.domain, private_key=self._private_key)

    async def client_public_key(self) -> bytes:
        return self._public_key


@asynccontextmanager
async def server(
    auth: Optional[AbstractServerAuthenticator],
) -> AsyncIterator[str]:
    scheduler = ExitOrderedAsyncScheduler()
    # Bind to a random port to avoid conflicts between different test cases.
    peer = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:*"),
        transport=ZeroMQRPCTransport,
        authenticator=auth,
        scheduler=scheduler,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    peer.handle_function("echo", handle_echo)
    async with peer:
        # TODO: make this a public API of callosum
        underlying_sock = peer._transport._sock  # type: ignore
        endpoint = underlying_sock.getsockopt(zmq.LAST_ENDPOINT)
        print("server started")
        yield endpoint
    print("server terminated")


async def client(
    endpoint: str,
    auth: Optional[AbstractClientAuthenticator],
) -> None:
    peer = Peer(
        connect=ZeroMQAddress(endpoint),
        transport=ZeroMQRPCTransport,
        authenticator=auth,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
        invoke_timeout=2.0,
        transport_opts={"handshake_timeout": 0.2},
    )
    async with peer:
        resp = await peer.invoke("echo", {"sent": "asdf"})
        assert resp["received"] == "asdf"


@pytest.mark.asyncio
async def test_auth_null():
    async with server(
        None,
    ) as endpoint:
        await client(endpoint, None)


@pytest.mark.asyncio
async def test_auth_good_user():
    async with server(
        MyServerAuthenticator("testing", server_keypair),
    ) as endpoint:
        await client(
            endpoint,
            MyClientAuthenticator(
                "testing",
                client1_keypair,
                server_keypair[0],
            ),
        )


@pytest.mark.asyncio
async def test_auth_wrong_user():
    async with server(
        MyServerAuthenticator("testing", server_keypair),
    ) as endpoint:
        with pytest.raises(AuthenticationError):
            await client(
                endpoint,
                MyClientAuthenticator(
                    "testing",
                    client2_keypair,
                    server_keypair[0],
                ),
            )


@pytest.mark.asyncio
async def test_auth_no_auth():
    async with server(
        MyServerAuthenticator("testing", server_keypair),
    ) as endpoint:
        with pytest.raises(AuthenticationError):
            await client(endpoint, None)
