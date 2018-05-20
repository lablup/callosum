import zmq, zmq.asyncio, zmq.auth

from . import (
    BaseBinder, BaseConnector, BaseTransport,
    AbstractMessagingMixin, AbstractStreamingMixin,
)


class ZeroMQConnection(AbstractMessagingMixin, AbstractStreamingMixin):

    async def recv_message(self):
        assert not self.transport._closed
        raw_msg = await self.transport._sock.recv_multipart()
        return raw_msg

    async def send_message(self, raw_msg):
        assert not self.transport._closed
        await self.transport._sock.send_multipart(raw_msg)


class ZeroMQBinder(ZeroMQConnection, BaseBinder):

    async def __aenter__(self):
        if not self.transport._closed:
            return self
        sock = self.transport._zctx.socket(zmq.PAIR)
        # server_key = zmq.auth.load_certificate('test-server.key_secret')
        # sock.setsockopt(zmq.CURVE_SERVER, 1)
        # sock.setsockopt(zmq.CURVE_SECRETKEY, server_key[1])
        # sock.setsockopt(zmq.LINGER, 100)
        sock.bind(self.addr)
        self.transport._sock = sock
        return self

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        self.transport._sock.close()
        self.transport._sock = None


class ZeroMQConnector(ZeroMQConnection, BaseConnector):

    async def __aenter__(self):
        if not self.transport._closed:
            return self
        sock = self.transport._zctx.socket(zmq.PAIR)
        # client_key = zmq.auth.load_certificate('test.key_secret')
        # server_key = zmq.auth.load_certificate('test-server.key')
        # sock.setsockopt(zmq.CURVE_SERVERKEY, server_key[0])
        # sock.setsockopt(zmq.CURVE_PUBLICKEY, client_key[0])
        # sock.setsockopt(zmq.CURVE_SECRETKEY, client_key[1])
        sock.setsockopt(zmq.LINGER, 100)
        sock.connect(self.addr)
        self.transport._sock = sock
        return self


class ZeroMQTransport(BaseTransport):

    binder_cls = ZeroMQBinder
    connector_cls = ZeroMQConnector

    def __init__(self):
        self._zctx = zmq.asyncio.Context()
        self._sock = None

    @property
    def _closed(self):
        return self._sock is None or self._sock._closed

    async def close(self):
        if self._sock is not None:
            self._sock.close()
        if self._zctx is not None:
            self._zctx.term()
