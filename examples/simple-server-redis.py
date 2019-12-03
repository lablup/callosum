import asyncio
import json
import signal
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


async def serve() -> None:
    peer = Peer(
        bind=RedisStreamAddress(
            'redis://localhost:6379',
            'myservice', 'server-group', 'client1'),
        transport=RPCRedisTransport,
        serializer=lambda o: json.dumps(o).encode('utf8'),
        deserializer=lambda b: json.loads(b))
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
