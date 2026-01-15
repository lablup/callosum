"""
Authentication helpers for benchmark testing.
"""

from typing import List, Optional, Set, Tuple

from callosum.auth import (
    AbstractClientAuthenticator,
    AbstractServerAuthenticator,
    AuthResult,
    Credential,
    Identity,
    create_keypair,
)


class DummyServerAuthenticator(AbstractServerAuthenticator):
    """
    Test server authenticator for benchmarks.

    Accepts any client with a valid keypair from the allowed list.
    """

    allowed_public_keys: Optional[Set[bytes]]
    _server_keypair: Tuple[bytes, bytes]

    def __init__(self, allowed_public_keys: Optional[List[bytes]] = None):
        """
        Initialize server authenticator.

        Args:
            allowed_public_keys: List of allowed client public keys.
                                If None, accepts all clients.
        """
        self.allowed_public_keys = (
            set(allowed_public_keys) if allowed_public_keys else None
        )
        self._server_keypair = create_keypair()

    def get_public_key(self) -> bytes:
        """Return server's public key."""
        return self._server_keypair[0]

    def get_secret_key(self) -> bytes:
        """Return server's secret key."""
        return self._server_keypair[1]

    async def server_identity(self) -> Identity:
        """Return the identity of the server."""
        return Identity(domain="benchmark", private_key=self._server_keypair[1])

    async def check_client(self, creds: Credential) -> AuthResult:
        """Check if the given client credential is valid."""
        if self.allowed_public_keys is None:
            # Accept all clients
            return AuthResult(success=True)

        if creds.public_key in self.allowed_public_keys:
            return AuthResult(success=True)

        return AuthResult(success=False)

    async def server_public_key(self) -> bytes:
        """Return the public key of the server."""
        return self._server_keypair[0]


class DummyClientAuthenticator(AbstractClientAuthenticator):
    """
    Test client authenticator for benchmarks.
    """

    _server_public_key: Optional[bytes]
    _client_keypair: Tuple[bytes, bytes]

    def __init__(self, server_public_key_value: Optional[bytes] = None):
        """
        Initialize client authenticator.

        Args:
            server_public_key_value: Server's public key. If None, trust any server.
        """
        self._server_public_key = server_public_key_value
        self._client_keypair = create_keypair()

    def get_public_key(self) -> bytes:
        """Return client's public key."""
        return self._client_keypair[0]

    def get_secret_key(self) -> bytes:
        """Return client's secret key."""
        return self._client_keypair[1]

    async def server_public_key(self) -> bytes:
        """Return the public key of the server."""
        if self._server_public_key is None:
            # Return empty bytes if no server key set (trust any server)
            return b""
        return self._server_public_key

    async def client_identity(self) -> Identity:
        """Return the identity of the client."""
        return Identity(domain="benchmark", private_key=self._client_keypair[1])

    async def client_public_key(self) -> bytes:
        """Return the public key of the client."""
        return self._client_keypair[0]


def create_test_keypairs(count: int) -> List[Tuple[bytes, bytes]]:
    """
    Generate multiple keypairs for testing.

    Args:
        count: Number of keypairs to generate

    Returns:
        List of (public_key, secret_key) tuples
    """
    return [create_keypair() for _ in range(count)]


def setup_benchmark_auth(
    num_clients: int = 1,
) -> Tuple[DummyServerAuthenticator, List[DummyClientAuthenticator]]:
    """
    Setup authenticators for benchmark testing.

    Args:
        num_clients: Number of client authenticators to create

    Returns:
        Tuple of (server_authenticator, list_of_client_authenticators)
    """
    # Create server authenticator
    server_auth = DummyServerAuthenticator()

    # Create client authenticators
    client_authenticators = []
    for _ in range(num_clients):
        client_auth = DummyClientAuthenticator(
            server_public_key_value=server_auth.get_public_key()
        )
        client_authenticators.append(client_auth)

    # Update server to accept these clients
    allowed_keys = [ca.get_public_key() for ca in client_authenticators]
    server_auth.allowed_public_keys = set(allowed_keys)

    return server_auth, client_authenticators
