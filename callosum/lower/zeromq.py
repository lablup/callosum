import zmq, zmq.asyncio, zmq.auth
import yarl

from . import (
    AbstractBinder, AbstractConnector,
    AbstractConnection,
    BaseTransport,
)


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
        # server_key = zmq.auth.load_certificate('test-server.key_secret')
        # sock.setsockopt(zmq.CURVE_SERVER, 1)
        # sock.setsockopt(zmq.CURVE_SECRETKEY, server_key[1])
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
        # client_key = zmq.auth.load_certificate('test.key_secret')
        # server_key = zmq.auth.load_certificate('test-server.key')
        # sock.setsockopt(zmq.CURVE_SERVERKEY, server_key[0])
        # sock.setsockopt(zmq.CURVE_PUBLICKEY, client_key[0])
        # sock.setsockopt(zmq.CURVE_SECRETKEY, client_key[1])
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

    It keeps a single persistent connection over multiple connections.
    As the underlying PUSH/PULL sockets work asynchronously, this
    effectively achieves connection pooling reducing handshake overheads.
    '''

    binder_cls = ZeroMQBinder
    connector_cls = ZeroMQConnector

    def __init__(self, **kwargs):
        self._zsock_opts = {
            zmq.LINGER: 100,
            **kwargs.pop('zsock_opts', {}),
        }
        super().__init__(**kwargs)
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
        if self._zctx is not None:
            self._zctx.term()
