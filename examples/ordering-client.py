import asyncio
import json

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def call():
    peer = Peer(
        connect=ZeroMQAddress('tcp://localhost:5010'),
        transport=ZeroMQTransport,
        serializer=json.dumps,
        deserializer=json.loads,
        invoke_timeout=5.0)
    async with peer:
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


if __name__ == '__main__':
    asyncio.run(call())
