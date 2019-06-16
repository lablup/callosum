import asyncio
import json
import random
import secrets

from callosum import Peer
from callosum.lower.redis import (
    RedisStreamAddress, RedisStreamTransport
)


async def call():
    peer = Peer(connect=RedisStreamAddress(
                    'redis://localhost:16379',
                    'myservice', 'client-group', 'server1'),
                transport=RedisStreamTransport,
                serializer=json.dumps,
                deserializer=json.loads,
                invoke_timeout=30.0)
    await peer.open()
    response = await peer.invoke('echo', {
        'sent': secrets.token_hex(16),
    })
    print(f"echoed {response['received']}")
    response = await peer.invoke('echo', {
        'sent': secrets.token_hex(16),
    })
    print(f"echoed {response['received']}")
    a = random.randint(1, 10)
    b = random.randint(10, 20)
    response = await peer.invoke('add', {
        'a': a,
        'b': b,
    })
    print(f"{a} + {b} = {response['result']}")
    await peer.close()


if __name__ == '__main__':
    asyncio.run(call())
