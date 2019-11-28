import asyncio
import json
import time

from callosum.rpc import Peer
from callosum.lower.rpc_redis import (
    RedisStreamAddress,
    RPCRedisTransport,
)


async def handle_echo(request):
    # NOTE: Adding this part with "await asyncio.sleep(1)" breaks the code.
    time.sleep(1.0)
    print("After sleeping")
    return {
        'received': request.body['sent'],
    }


async def handle_add(request):
    return {
        'result': request.body['a'] + request.body['b'],
    }


async def serve():
    peer = Peer(bind=RedisStreamAddress(
                    'redis://localhost:6379',
                    'myservice', 'server-group', 'client1'),
                transport=RPCRedisTransport,
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
    loop.set_debug(True)
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
