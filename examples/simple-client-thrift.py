import asyncio
import pathlib
import random
import secrets

from callosum import Peer
from callosum.upper.thrift import ThriftClientAdaptor
import thriftpy


simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / 'simple.thrift'),
    module_name='simple_thrift')


async def call():
    peer = Peer(connect='tcp://localhost:5000',
                invoke_timeout=2.0)
    adaptor = ThriftClientAdaptor(simple_thrift.SimpleService)
    response = await peer.invoke(
        'simple',
        adaptor.echo(secrets.token_hex(16)))
    print(f"echoed {response}")
    response = await peer.invoke(
        'simple',
        adaptor.echo(secrets.token_hex(16)))
    print(f"echoed {response}")
    a = random.randint(1, 10)
    b = random.randint(10, 20)
    response = await peer.invoke(
        'simple',
        adaptor.add(a, b))
    print(f"{a} + {b} = {response}")
    await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(call())
    finally:
        loop.close()
