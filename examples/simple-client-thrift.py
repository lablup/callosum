import asyncio
import logging
import pathlib
import random
import secrets
import sys
import textwrap

import thriftpy2 as thriftpy

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.rpc import Peer, RPCUserError
from callosum.serialize import noop_deserializer, noop_serializer
from callosum.upper.thrift import ThriftClientAdaptor

simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / "simple.thrift"), module_name="simple_thrift"
)


async def call() -> None:
    peer = Peer(
        connect=ZeroMQAddress("tcp://localhost:5030"),
        transport=ZeroMQRPCTransport,
        serializer=noop_serializer,
        deserializer=noop_deserializer,
        invoke_timeout=2.0,
    )
    adaptor = ThriftClientAdaptor(simple_thrift.SimpleService)
    async with peer:
        response = await peer.invoke("simple", adaptor.echo(secrets.token_hex(16)))
        print(f"echoed {response}")

        response = await peer.invoke("simple", adaptor.echo(secrets.token_hex(16)))
        print(f"echoed {response}")

        a = random.randint(1, 10)
        b = random.randint(10, 20)
        response = await peer.invoke("simple", adaptor.add(a, b))
        print(f"{a} + {b} = {response}")

        try:
            response = await peer.invoke("simple", adaptor.oops())
        except RPCUserError as e:
            print("catched remote error as expected:")
            print(textwrap.indent(e.traceback, prefix="| "))

        try:
            async with asyncio.timeout(0.5):
                await peer.invoke("simple", adaptor.long_delay())
        except asyncio.TimeoutError:
            print(
                "long_delay(): timeout occurred as expected "
                "(with per-call timeout)"
            )
        else:
            print("long_delay(): timeout did not occur!")
            sys.exit(1)

        try:
            await peer.invoke("simple", adaptor.long_delay())
        except asyncio.TimeoutError:
            print(
                "long_delay(): timeout occurred as expected "
                "(with default timeout)"
            )
        else:
            print("long_delay(): timeout did not occur!")
            sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger()
    asyncio.run(call())
