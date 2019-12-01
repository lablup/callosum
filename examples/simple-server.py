import asyncio
import json
import signal
import sys

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def handle_echo(request):
    print("handle_echo()")
    return {
        'received': request.body['sent'],
    }


async def handle_add(request):
    print("handle_add()")
    return {
        'result': request.body['a'] + request.body['b'],
    }


async def handle_long_delay(request):
    print("handle_long_delay()")
    try:
        await asyncio.sleep(5)
        return {
            'received': request.body['sent'],
        }
    except asyncio.CancelledError:
        print(" -> cancelled as expected")
        # NOTE: due to strange behaviour of asyncio, I have to reraise
        # otherwise, the task.cancelled() returns False
        raise
    else:
        print(" -> not cancelled!")
        sys.exit(1)


async def handle_error(request):
    print("handle_error()")
    await asyncio.sleep(0.1)
    raise ZeroDivisionError('ooops')


async def serve():
    peer = Peer(
        bind=ZeroMQAddress('tcp://127.0.0.1:5020'),
        transport=ZeroMQTransport,
        serializer=json.dumps,
        deserializer=json.loads)
    peer.handle_function('echo', handle_echo)
    peer.handle_function('add', handle_add)
    peer.handle_function('long_delay', handle_long_delay)
    peer.handle_function('error', handle_error)

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
