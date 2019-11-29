import asyncio
import pathlib
import signal

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport
from callosum.upper.thrift import ThriftServerAdaptor
import thriftpy


simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / 'simple.thrift'),
    module_name='simple_thrift')


class SimpleDispatcher:
    async def echo(self, msg):
        return msg

    async def add(self, a, b):
        return a + b


async def serve():
    peer = Peer(bind=ZeroMQAddress('tcp://127.0.0.1:5030'),
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
