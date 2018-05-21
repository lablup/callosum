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
        pull_sock.setsockopt(zmq.LINGER, 100)
        push_sock.setsockopt(zmq.LINGER, 100)
        pull_sock.bind(self.addr)
        url = yarl.URL(self.addr)
        push_sock.bind(str(url.with_port(url.port + 1)))
        self.transport._pull_sock = pull_sock
        self.transport._push_sock = push_sock
        return ZeroMQConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        self.transport._pull_sock.close()
        self.transport._push_sock.close()
        self.transport._pull_sock = None
        self.transport._push_sock = None


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
        pull_sock.setsockopt(zmq.LINGER, 100)
        push_sock.setsockopt(zmq.LINGER, 100)
        push_sock.connect(self.addr)
        url = yarl.URL(self.addr)
        pull_sock.connect(str(url.with_port(url.port + 1)))
        self.transport._pull_sock = pull_sock
        self.transport._push_sock = push_sock
        return ZeroMQConnection(self.transport)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class ZeroMQTransport(BaseTransport):

    binder_cls = ZeroMQBinder
    connector_cls = ZeroMQConnector

    def __init__(self):
        self._zctx = zmq.asyncio.Context()
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
