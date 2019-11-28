import asyncio
import json

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
    peer = Peer(bind=ZeroMQAddress('tcp://127.0.0.1:5020'),
                transport=ZeroMQTransport,
                serializer=json.dumps,
                deserializer=json.loads)
    peer.handle_function('echo', handle_echo)
    peer.handle_function('add', handle_add)
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
