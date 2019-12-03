import asyncio
import json
import random
import secrets
import sys
import textwrap
import traceback

from async_timeout import timeout

from callosum.rpc import Peer, RPCUserError
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def call() -> None:
    peer = Peer(
        connect=ZeroMQAddress('tcp://localhost:5020'),
        transport=ZeroMQTransport,
        serializer=lambda o: json.dumps(o).encode('utf8'),
        deserializer=lambda b: json.loads(b),
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
            print('long_delay(): timeout occurred as expected '
                  '(with per-call timeout)')
        else:
            print('long_delay(): timeout did not occur!')
            sys.exit(1)

        try:
            await peer.invoke('long_delay', {
                'sent': 'some-text',
            })
        except asyncio.TimeoutError:
            print('long_delay(): timeout occurred as expected '
                  '(with default timeout)')
        else:
            print('long_delay(): timeout did not occur!')
            sys.exit(1)

        try:
            await peer.invoke('error', {})
        except RPCUserError as e:
            print('error(): catched remote error as expected')
            print(textwrap.indent(e.traceback, prefix='| '))
        except Exception:
            print('error(): did not raise RPCUserError but strange one')
            traceback.print_exc()
            sys.exit(1)
        else:
            print('error(): did not return exception!')
            sys.exit(1)


if __name__ == '__main__':
    asyncio.run(call())
