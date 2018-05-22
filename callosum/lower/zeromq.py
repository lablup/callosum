import asyncio
import logging

import zmq, zmq.asyncio, zmq.auth
import yarl

from . import (
    AbstractBinder, AbstractConnector,
    AbstractConnection,
    BaseTransport,
)

ZAP_VERSION = b'1.0'


class ZAPServer:
    '''
    A simple authenticator adaptor that implements ZAP protocol.
    '''

    def __init__(self, zctx, authenticator=None):
        self._log = logging.getLogger(__name__ + '.ZAPServer')
        self._authenticator = authenticator
        self._zap_socket = None
        self._zctx = zctx

    async def serve(self):
        self._zap_socket = self._zctx.socket(zmq.REP)
        self._zap_socket.linger = 1
        self._zap_socket.bind('inproc://zeromq.zap.01')
        try:
            while True:
                msg = await self._zap_socket.recv_multipart()
                await self.handle_zap_message(msg)
        except asyncio.CancelledError:
            pass
        finally:
            self._zap_socket.close()
            self._zap_socket = None

    def close(self):
        if self._zap_socket is not None:
            self._zap_socket.close()
        self._zap_socket = None

    async def handle_zap_message(self, msg):
        if len(msg) < 6:
            self._log.error('Invalid ZAP message, not enough frames: %r', msg)
            if len(msg) < 2:
                self._log.error('Not enough information to reply')
            else:
                await self._send_zap_reply(msg[1], b'400', b'Not enough frames')
            return

        version, request_id, domain, address, identity, mechanism = msg[:6]
        credentials = msg[6:]
        domain = domain.decode('utf8', 'replace')
        address = address.decode('utf8', 'replace')

        if (version != ZAP_VERSION):
            self._log.error('Invalid ZAP version: %r', msg)
            await self._send_zap_reply(request_id, b'400', b'Invalid version')
            return

        self.log.debug('version: %r, request_id: %r, domain: %r, '
                       'address: %r, identity: %r, mechanism: %r',
                       version, request_id, domain,
                       address, identity, mechanism)
        allowed = False
        user_id = '<anonymous>'
        reason = b'Access denied'

        if self._authenticator is None:
            # When authenticator is not set, allow any NULL auth-requests.
            await self._send_zap_reply(request_id, b'200', b'OK', user_id)
            return

        if mechanism == b'CURVE':
            # For CURVE, even a whitelisted address must authenticate
            if len(credentials) != 1:
                self._log.info('Invalid CURVE credentials: %r', credentials)
                await self._send_zap_reply(request_id,
                                           b'400', b'Invalid credentials')
                return
            key = credentials[0]
            result = await self._authenticator.check_client(domain, key)
            allowed = result.success
            if allowed:
                user_id = result.user_id
        else:
            # In Callosum, we only support public-key based authentication.
            # Other possible values: b'NULL', b'PLAIN', b'GSSAPI'
            reason = b'Unsupported authentication mechanism'

        if allowed:
            await self._send_zap_reply(request_id, b'200', b'OK', user_id)
        else:
            await self._send_zap_reply(request_id, b'400', reason)

    async def _send_zap_reply(self, request_id: bytes,
                              status_code: bytes, status_text: bytes,
                              user_id: str=''):
        user_id = user_id.encode('utf8', 'replace')
        metadata = b''
        self._log.debug('ZAP reply code=%s text=%s', status_code, status_text)
        reply = (ZAP_VERSION, request_id,
                 status_code, status_text,
                 user_id, metadata)
        await self._zap_socket.send_multipart(reply)


class ZeroMQConnection(AbstractConnection):

    __slots__ = ('transport', )

    def __init__(self, transport):
        self.transport = transport

    async def recv_message(self):
        assert not self.transport._closed
        raw_msg = await self.transport._pull_sock.recv_multipart()
        return raw_msg

    async def send_message(self, raw_msg):
        assert not self.transport._closed
        await self.transport._push_sock.send_multipart(raw_msg)


class ZeroMQBinder(AbstractBinder):

    __slots__ = ('transport', 'addr')

    async def __aenter__(self):
        if not self.transport._closed:
            return ZeroMQConnection(self.transport)
        pull_sock = self.transport._zctx.socket(zmq.PULL)
        push_sock = self.transport._zctx.socket(zmq.PUSH)
        if self.transport.authenticator:
            server_private_key = self.transport.authenticator.server_identity()
            pull_sock.setsockopt(zmq.CURVE_SERVER, 1)
            pull_sock.setsockopt(zmq.CURVE_SECRETKEY, server_private_key)
            push_sock.setsockopt(zmq.CURVE_SERVER, 1)
            push_sock.setsockopt(zmq.CURVE_SECRETKEY, server_private_key)
        for key, value in self.transport._zsock_opts.items():
            pull_sock.setsockopt(key, value)
            push_sock.setsockopt(key, value)
        pull_sock.bind(self.addr)
        url = yarl.URL(self.addr)
        push_sock.bind(str(url.with_port(url.port + 1)))
        self.transport._pull_sock = pull_sock
        self.transport._push_sock = push_sock
        return ZeroMQConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class ZeroMQConnector(AbstractConnector):

    __slots__ = ('transport', 'addr')

    async def __aenter__(self):
        if not self.transport._closed:
            return ZeroMQConnection(self.transport)
        pull_sock = self.transport._zctx.socket(zmq.PULL)
        push_sock = self.transport._zctx.socket(zmq.PUSH)
        if self.transport.authenticator:
            client_private_key = self.transport.authenticator.client_identity()
            client_public_key = self.transport.authenticator.client_public_key()
            server_public_key = self.transport.authenticator.server_public_key()
            pull_sock.setsockopt(zmq.CURVE_SERVERKEY, server_public_key)
            pull_sock.setsockopt(zmq.CURVE_PUBLICKEY, client_public_key)
            pull_sock.setsockopt(zmq.CURVE_SECRETKEY, client_private_key)
            push_sock.setsockopt(zmq.CURVE_SERVERKEY, server_public_key)
            push_sock.setsockopt(zmq.CURVE_PUBLICKEY, client_public_key)
            push_sock.setsockopt(zmq.CURVE_SECRETKEY, client_private_key)
        for key, value in self.transport._zsock_opts.items():
            pull_sock.setsockopt(key, value)
            push_sock.setsockopt(key, value)
        push_sock.connect(self.addr)
        url = yarl.URL(self.addr)
        pull_sock.connect(str(url.with_port(url.port + 1)))
        self.transport._pull_sock = pull_sock
        self.transport._push_sock = push_sock
        return ZeroMQConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class ZeroMQTransport(BaseTransport):

    '''
    Implementation for the ZeorMQ-backed transport.

    It keeps a single persistent connection over multiple connect/bind() calls.
    As the underlying PUSH/PULL sockets work asynchronously, this effectively
    achieves connection pooling reducing handshake overheads.
    '''

    __slots__ = BaseTransport.__slots__ + (
        '_zctx', '_zsock_opts',
        '_zap_server', '_zap_task',
        '_pull_sock', '_push_sock',
    )

    binder_cls = ZeroMQBinder
    connector_cls = ZeroMQConnector

    def __init__(self, authenticator, **kwargs):
        loop = asyncio.get_event_loop()
        self._zap_server = None
        self._zap_task = None
        self._zsock_opts = {
            zmq.LINGER: 100,
            **kwargs.pop('zsock_opts', {}),
        }
        super().__init__(authenticator, **kwargs)
        if self.authenticator:
            self._zap_server = ZAPServer(self.authenticator)
            self._zap_task = loop.create_task(self.authenticator.serve())
        self._zctx = zmq.asyncio.Context()
        # Keep sockets during the transport lifetime.
        self._pull_sock = None
        self._push_sock = None

    @property
    def _closed(self):
        return self._pull_sock is None or self._pull_sock._closed

    async def close(self):
        if self._pull_sock is not None:
            self._pull_sock.close()
        if self._push_sock is not None:
            self._push_sock.close()
        if self._zap_task is not None:
            self._zap_task.cancel()
            await self._zap_task
        if self._zctx is not None:
            self._zctx.term()
