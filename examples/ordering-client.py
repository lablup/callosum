import asyncio
import json

from callosum import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def call():
    peer = Peer(connect=ZeroMQAddress('tcp://localhost:5010'),
                transport=ZeroMQTransport,
                serializer=json.dumps,
                deserializer=json.loads,
                invoke_timeout=5.0)
    await peer.open()

    print('Check the server log to see in which order echo/add are executed.\n')

    print('== Calling with the same ordering key ==')
    tasks = [
        peer.invoke('echo', {'sent': 'bbbb'}, order_key='mykey'),
        peer.invoke('add', {'a': 1, 'b': 2}, order_key='mykey'),
    ]
    responses = await asyncio.gather(*tasks)
    print(responses)

    await peer.invoke('print_delim', {})

    print('== Calling without any ordering key ==')
    tasks = [
        peer.invoke('echo', {'sent': 'bbbb'}),
        peer.invoke('add', {'a': 1, 'b': 2}),
    ]
    responses = await asyncio.gather(*tasks)
    print(responses)

    await peer.invoke('print_delim', {})

    await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(call())
    finally:
        loop.close()
