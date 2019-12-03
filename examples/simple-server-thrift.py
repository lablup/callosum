import asyncio
import pathlib
import signal

from callosum.rpc import Peer
from callosum.serialize import noop_serializer, noop_deserializer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport
from callosum.upper.thrift import ThriftServerAdaptor
import thriftpy2 as thriftpy


simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / 'simple.thrift'),
    module_name='simple_thrift')


class SimpleDispatcher:
    async def echo(self, msg):
        return msg

    async def add(self, a, b):
        return a + b

    async def oops(self):
        raise ZeroDivisionError('oops')


async def serve() -> None:
    peer = Peer(
        bind=ZeroMQAddress('tcp://127.0.0.1:5030'),
        serializer=noop_serializer,
        deserializer=noop_deserializer,
        transport=ZeroMQTransport)
    adaptor = ThriftServerAdaptor(
        peer,
        simple_thrift.SimpleService,
        SimpleDispatcher())
    peer.handle_function('simple', adaptor.handle_function)

    loop = asyncio.get_running_loop()
    forever = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, forever.cancel)
    loop.add_signal_handler(signal.SIGTERM, forever.cancel)
    async with peer:
        try:
            print('server started')
            await forever
        except asyncio.CancelledError:
            pass
    print('server terminated')


if __name__ == '__main__':
    asyncio.run(serve())
