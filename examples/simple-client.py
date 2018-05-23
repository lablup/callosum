import asyncio
import json
import random
import secrets

from callosum import Peer


async def call():
    peer = Peer(connect='tcp://localhost:5000',
                serializer=json.dumps,
                deserializer=json.loads,
                invoke_timeout=2.0)
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
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(call())
    finally:
        loop.close()
