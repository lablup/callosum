import asyncio
import json
import random
import secrets
import sys

from async_timeout import timeout

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def call():
    peer = Peer(connect=ZeroMQAddress('tcp://localhost:5020'),
                transport=ZeroMQTransport,
                serializer=json.dumps,
                deserializer=json.loads,
                invoke_timeout=2.0)
    async with peer:
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
        try:
            with timeout(0.5):
                await peer.invoke('long_delay', {
                    'sent': 'some-text',
                })
        except asyncio.TimeoutError:
            print('long_delay(): timeout occurred as expected')
        else:
            print('long_delay(): timeout did not occur!')
            sys.exit(1)


if __name__ == '__main__':
    asyncio.run(call())
