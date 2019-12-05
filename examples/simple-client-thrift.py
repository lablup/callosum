import asyncio
import pathlib
import random
import secrets
import textwrap

from callosum.rpc import Peer, RPCUserError
from callosum.serialize import noop_serializer, noop_deserializer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.upper.thrift import ThriftClientAdaptor
import thriftpy2 as thriftpy


simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / 'simple.thrift'),
    module_name='simple_thrift')


async def call() -> None:
    peer = Peer(
        connect=ZeroMQAddress('tcp://localhost:5030'),
        transport=ZeroMQRPCTransport,
        serializer=noop_serializer,
        deserializer=noop_deserializer,
        invoke_timeout=2.0)
    adaptor = ThriftClientAdaptor(simple_thrift.SimpleService)
    async with peer:
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

        try:
            response = await peer.invoke(
                'simple',
                adaptor.oops())
        except RPCUserError as e:
            print('catched remote error as expected:')
            print(textwrap.indent(e.traceback, prefix='| '))


if __name__ == '__main__':
    asyncio.run(call())
