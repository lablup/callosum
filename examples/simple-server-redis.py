import asyncio
import json

from callosum import Peer
from callosum.lower.redis import (
    RedisStreamAddress, RedisStreamTransport
)


async def handle_echo(request):
    return {
        'received': request.body['sent'],
    }


async def handle_add(request):
    return {
        'result': request.body['a'] + request.body['b'],
    }


async def serve():
    peer = Peer(bind=RedisStreamAddress(
                    'redis://localhost:16379',
                    'myservice', 'server-group', 'client1'),
                transport=RedisStreamTransport,
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
