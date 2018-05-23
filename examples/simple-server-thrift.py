import asyncio
import pathlib

from callosum import Peer
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
    peer = Peer(bind='tcp://127.0.0.1:5000')
    adaptor = ThriftServerAdaptor(
        peer,
        simple_thrift.SimpleService,
        SimpleDispatcher())
    peer.handle_function('simple', adaptor.handle_function)
    try:
        await peer.open()
        await peer.listen()
    except asyncio.CancelledError:
        await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        task = loop.create_task(serve())
        print('listening...')
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        print('closing...')
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()
        print('closed.')
