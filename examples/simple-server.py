import asyncio
import json
import signal

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


# add the following handler when you want to test how
# cancellation on invocation timeout works
'''
async def handle_echo(request):
    try:
        print("Before async sleep")
        await asyncio.sleep(1000)
        print("After async sleep")
        return {
            'received': request.body['sent'],
        }
    except asyncio.CancelledError:
        print("Task was cancelled successfully")
        # NOTE: due to strange behaviour of asyncio, I have to reraise
        # otherwise, the task.cancelled() returns False
        raise
'''


async def handle_echo(request):
    return {
        'received': request.body['sent'],
    }


async def handle_add(request):
    return {
        'result': request.body['a'] + request.body['b'],
    }


async def serve():
    peer = Peer(
        bind=ZeroMQAddress('tcp://127.0.0.1:5020'),
        transport=ZeroMQTransport,
        serializer=json.dumps,
        deserializer=json.loads)
    peer.handle_function('echo', handle_echo)
    peer.handle_function('add', handle_add)

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
