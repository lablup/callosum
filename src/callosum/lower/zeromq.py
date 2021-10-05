from __future__ import annotations

import asyncio
import logging
from typing import (
    Any,
    AsyncGenerator,
    ClassVar,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
)
import secrets
import warnings

import attr
import zmq, zmq.asyncio, zmq.utils.monitor

from ..abc import RawHeaderBody
from ..auth import Identity
from . import (
    AbstractAddress,
    AbstractBinder, AbstractConnector,
    AbstractConnection,
    BaseTransport,
)

ZAP_VERSION = b'1.0'

_default_zsock_opts = {
    zmq.LINGER: 100,
}


@attr.s(auto_attribs=True, slots=True)
class ZeroMQAddress(AbstractAddress):
    uri: Union[str, Tuple[str, int]]


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

        version, request_id, domain, address, sock_identity, mechanism = msg[:6]
        credentials = msg[6:]
        domain = domain.decode('utf8', 'replace')
        address = address.decode('utf8', 'replace')

        if (version != ZAP_VERSION):
            self._log.error('Invalid ZAP version: %r', msg)
            await self._send_zap_reply(request_id, b'400', b'Invalid version')
            return

        self.log.debug('version: %r, request_id: %r, domain: %r, '
                       'address: %r, sock_identity: %r, mechanism: %r',
                       version, request_id, domain,
                       address, sock_identity, mechanism)
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
            client_id = Identity(domain, key)
            result = await self._authenticator.check_client(client_id)
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
                              user_id: str = ''):
        metadata = b''
        self._log.debug('ZAP reply code=%s text=%s', status_code, status_text)
        reply = (ZAP_VERSION, request_id,
                 status_code, status_text,
                 user_id.encode('utf8', 'replace'),
                 metadata)
        await self._zap_socket.send_multipart(reply)


class ZeroMQRPCConnection(AbstractConnection):

    __slots__ = ('transport')

    transport: ZeroMQRPCTransport

    def __init__(self, transport):
        self.transport = transport

    async def recv_message(self) -> AsyncGenerator[Optional[RawHeaderBody], None]:
        assert not self.transport._closed
        assert self.transport._sock is not None
        *pre, raw_header, raw_body = await self.transport._sock.recv_multipart()
        if len(pre) > 0:
            # server
            peer_id = pre[0]
            yield RawHeaderBody(raw_header, raw_body, peer_id)
        else:
            # client
            yield RawHeaderBody(raw_header, raw_body, None)

    async def send_message(self, raw_msg: RawHeaderBody) -> None:
        assert not self.transport._closed
        assert self.transport._sock is not None
        peer_id = raw_msg.peer_id
        if peer_id is not None:
            # server
            await self.transport._sock.send_multipart([
                peer_id, raw_msg.header, raw_msg.body,
            ])
        else:
            # client
            await self.transport._sock.send_multipart([
                raw_msg.header, raw_msg.body,
            ])


class ZeroMQMonitorMixin:

    addr: ZeroMQAddress

    _monitor_sock: Optional[zmq.Socket]

    EVENT_MAP = {
        zmq.EVENT_MONITOR_STOPPED: "monitor-stopped",
        zmq.EVENT_ACCEPT_FAILED: "accept-failed",
        zmq.EVENT_ACCEPTED: "accepted",
        zmq.EVENT_BIND_FAILED: "bind-failed",
        zmq.EVENT_LISTENING: "listening",
        zmq.EVENT_CLOSE_FAILED: "close-failed",
        zmq.EVENT_CLOSED: "closed",
        zmq.EVENT_CONNECTED: "connected",
        zmq.EVENT_CONNECT_DELAYED: "connect-delayed",
        zmq.EVENT_CONNECT_RETRIED: "connect-retried",
        zmq.EVENT_DISCONNECTED: "disconnected",
        zmq.EVENT_HANDSHAKE_FAILED_AUTH: "handshake-faield-auth",
        zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL: "handshake-failed-no-detail",
        zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL: "handshake-failed-protocol",
        zmq.EVENT_HANDSHAKE_SUCCEEDED: "handshake-succeeded",
        zmq.EVENT_ALL: "all",
    }

    async def _monitor(self) -> None:
        assert self._monitor_sock is not None
        log = logging.getLogger('callosum.lower.zeromq.monitor')
        try:
            while (await self._monitor_sock.poll()):
                raw_msg = await self._monitor_sock.recv_multipart()
                msg = zmq.utils.monitor.parse_monitor_message(raw_msg)
                msg['description'] = self.EVENT_MAP.get(
                    msg['event'],
                    str(msg['event']),
                )
                log.debug("monitor[%s] event: %r", self.addr, msg)
                if msg['event'] == zmq.EVENT_MONITOR_STOPPED:
                    break
        except Exception:
            log.exception("monitor[%s] unexpected error", self.addr)
        finally:
            self._monitor_sock.close()
            log.debug("monitor[%s] closed", self.addr)


class ZeroMQBaseBinder(ZeroMQMonitorMixin, AbstractBinder):

    __slots__ = (
        'transport',
        'addr',
        '_attach_monitor',
        '_monitor_sock',
        '_monitor_task',
        '_main_sock',
        '_zsock_opts',
    )

    socket_type: ClassVar[int] = 0

    transport: ZeroMQBaseTransport
    addr: ZeroMQAddress

    def __init__(
        self,
        transport: BaseTransport,
        addr: AbstractAddress,
        *,
        attach_monitor: bool = False,
        zsock_opts: Mapping[int, Any] = None,
        **transport_opts,
    ) -> None:
        super().__init__(transport, addr)
        self._attach_monitor = attach_monitor
        self._zsock_opts = {**_default_zsock_opts, **(zsock_opts or {})}
        if attach_monitor:
            warnings.warn(
                "ZeroMQ async monitor socket support is buggy "
                "and not recommended to use.",
                RuntimeWarning,
            )

    async def __aenter__(self):
        if not self.transport._closed:
            return ZeroMQRPCConnection(self.transport)
        server_sock = self.transport._zctx.socket(type(self).socket_type)
        if self.transport.authenticator:
            server_id = await self.transport.authenticator.server_identity()
            server_sock.zap_domain = server_id.domain.encode('utf8')
            server_sock.setsockopt(zmq.CURVE_SERVER, 1)
            server_sock.setsockopt(zmq.CURVE_SECRETKEY, server_id.private_key)
        for key, value in self._zsock_opts.items():
            server_sock.setsockopt(key, value)
        if self._attach_monitor:
            monitor_addr = f"inproc://monitor-{secrets.token_hex(16)}"
            server_sock.get_monitor_socket(addr=monitor_addr)
            self._monitor_sock = self.transport._zctx.socket(zmq.PAIR)
            self._monitor_sock.connect(monitor_addr)
            self._monitor_task = asyncio.create_task(self._monitor())
        else:
            self._monitor_sock = None
            self._monitor_task = None
        server_sock.bind(self.addr.uri)
        self.transport._sock = server_sock
        self._main_sock = server_sock
        return ZeroMQRPCConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        if self._monitor_task is not None:
            self._main_sock.disable_monitor()
            await self._monitor_task


class ZeroMQBaseConnector(ZeroMQMonitorMixin, AbstractConnector):

    __slots__ = (
        'transport',
        'addr',
        '_attach_monitor',
        '_monitor_sock',
        '_monitor_task',
        '_main_sock',
        '_zsock_opts',
    )

    socket_type: ClassVar[int] = 0

    transport: ZeroMQBaseTransport
    addr: ZeroMQAddress

    def __init__(
        self,
        transport: BaseTransport,
        addr: AbstractAddress,
        *,
        attach_monitor: bool = False,
        zsock_opts: Mapping[int, Any] = None,
        **transport_opts,
    ) -> None:
        super().__init__(transport, addr)
        self._attach_monitor = attach_monitor
        self._zsock_opts = {**_default_zsock_opts, **(zsock_opts or {})}
        if attach_monitor:
            warnings.warn(
                "ZeroMQ async monitor socket support is buggy "
                "and not recommended to use.",
                RuntimeWarning,
            )

    async def __aenter__(self):
        if not self.transport._closed:
            return ZeroMQRPCConnection(self.transport)
        client_sock = self.transport._zctx.socket(type(self).socket_type)
        if self.transport.authenticator:
            auth = self.transport.authenticator
            client_id = await auth.client_identity()
            client_public_key = await auth.client_public_key()
            server_public_key = await auth.server_public_key()
            client_sock.zap_domain = client_id.domain.encode('utf8')
            client_sock.setsockopt(zmq.CURVE_SERVERKEY, server_public_key)
            client_sock.setsockopt(zmq.CURVE_PUBLICKEY, client_public_key)
            client_sock.setsockopt(zmq.CURVE_SECRETKEY, client_id.private_key)
        for key, value in self._zsock_opts.items():
            client_sock.setsockopt(key, value)
        if self._attach_monitor:
            monitor_addr = f"inproc://monitor-{secrets.token_hex(16)}"
            client_sock.get_monitor_socket(addr=monitor_addr)
            self._monitor_sock = self.transport._zctx.socket(zmq.PAIR)
            self._monitor_sock.connect(monitor_addr)
            self._monitor_task = asyncio.create_task(self._monitor())
        else:
            self._monitor_sock = None
            self._monitor_task = None
        client_sock.connect(self.addr.uri)
        self._main_sock = client_sock
        self.transport._sock = client_sock
        return ZeroMQRPCConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        if self._monitor_task is not None:
            self._main_sock.disable_monitor()
            await self._monitor_task


class ZeroMQRouterBinder(ZeroMQBaseBinder):

    socket_type: ClassVar[int] = zmq.ROUTER
    transport: ZeroMQRPCTransport


class ZeroMQDealerConnector(ZeroMQBaseConnector):

    socket_type: ClassVar[int] = zmq.DEALER
    transport: ZeroMQRPCTransport


class ZeroMQPushBinder(ZeroMQBaseBinder):

    socket_type: ClassVar[int] = zmq.PUSH
    transport: ZeroMQDistributorTransport


class ZeroMQPullConnector(ZeroMQBaseConnector):

    socket_type: ClassVar[int] = zmq.PULL
    transport: ZeroMQDistributorTransport


class ZeroMQPubBinder(ZeroMQBaseBinder):

    socket_type: ClassVar[int] = zmq.PUB
    transport: ZeroMQBroadcastTransport


class ZeroMQSubConnector(ZeroMQBaseConnector):

    socket_type: ClassVar[int] = zmq.SUB
    transport: ZeroMQBroadcastTransport


class ZeroMQBaseTransport(BaseTransport):

    '''
    Implementation for the ZeorMQ-backed transport.

    It keeps a single persistent connection over multiple connect/bind() calls.
    As the underlying PUSH/PULL sockets work asynchronously, this effectively
    achieves connection pooling reducing handshake overheads.
    '''

    __slots__ = BaseTransport.__slots__ + (
        '_zctx', '_zsock_opts',
        '_zap_server', '_zap_task',
        '_sock',
    )

    binder_cls: ClassVar[Type[ZeroMQBaseBinder]]
    connector_cls: ClassVar[Type[ZeroMQBaseConnector]]

    def __init__(self, authenticator, **kwargs) -> None:
        super().__init__(authenticator, **kwargs)
        loop = asyncio.get_running_loop()
        self._zap_server = None
        self._zap_task = None
        if self.authenticator:
            self._zap_server = ZAPServer(self.authenticator)
            self._zap_task = loop.create_task(self._zap_server.serve())
        self._zctx = zmq.asyncio.Context()
        # Keep sockets during the transport lifetime.
        self._sock = None

    @property
    def _closed(self) -> bool:
        if self._sock is not None:
            return self._sock.closed
        return True

    async def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
        if self._zap_task is not None:
            self._zap_task.cancel()
            await self._zap_task
        if self._zctx is not None:
            self._zctx.destroy()


class ZeroMQRPCTransport(ZeroMQBaseTransport):

    binder_cls = ZeroMQRouterBinder
    connector_cls = ZeroMQDealerConnector


class ZeroMQDistributorTransport(ZeroMQBaseTransport):

    binder_cls = ZeroMQPushBinder
    connector_cls = ZeroMQPullConnector


class ZeroMQBroadcastTransport(ZeroMQBaseTransport):

    binder_cls = ZeroMQPubBinder
    connector_cls = ZeroMQSubConnector
