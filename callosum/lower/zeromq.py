import zmq, zmq.asyncio

from . import BaseTransport


class ZeroMQTransport(BaseTransport):

    def __init__(self):
        self._zctx = zmq.asyncio.Context()
        self._sock = None

    async def connect(self, connect_addr):
        if self._sock is not None:
            return
        self._sock = self._zctx.socket(zmq.PAIR)
        self._sock.setsockopt(zmq.LINGER, 100)
        self._sock.connect(connect_addr)

    async def bind(self, bind_addr):
        if self._sock is not None:
            return
        self._sock = self._zctx.socket(zmq.PAIR)
        self._sock.setsockopt(zmq.LINGER, 100)
        self._sock.bind(bind_addr)

    @property
    def closed(self):
        return self._sock is None or self._sock.closed

    async def close(self):
        if self._sock is not None:
            self._sock.close()
        if self._zctx is not None:
            self._zctx.term()

    async def recv_message(self):
        assert not self.closed
        raw_msg = await self._sock.recv_multipart()
        return raw_msg

    async def send_message(self, raw_msg):
        assert not self.closed
        await self._sock.send_multipart(raw_msg)
