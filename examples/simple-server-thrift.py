import asyncio
import logging
import pathlib
import signal

import thriftpy2 as thriftpy

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.rpc import Peer
from callosum.serialize import noop_deserializer, noop_serializer
from callosum.upper.thrift import ThriftServerAdaptor

simple_thrift = thriftpy.load(
    str(pathlib.Path(__file__).parent / "simple.thrift"), module_name="simple_thrift"
)


class SimpleDispatcher:
    async def echo(self, msg: str) -> str:
        return msg

    async def add(self, a: int, b: int) -> int:
        return a + b

    async def oops(self) -> bool:
        raise ZeroDivisionError("oops")

    async def long_delay(self) -> bool:
        await asyncio.sleep(5.0)
        return True


async def serve() -> None:
    peer = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5030"),
        serializer=noop_serializer,
        deserializer=noop_deserializer,
        transport=ZeroMQRPCTransport,
    )
    adaptor = ThriftServerAdaptor(
        peer, simple_thrift.SimpleService, SimpleDispatcher()
    )
    peer.handle_function("simple", adaptor.handle_function)

    loop = asyncio.get_running_loop()
    forever = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, forever.cancel)
    loop.add_signal_handler(signal.SIGTERM, forever.cancel)
    async with peer:
        try:
            print("server started")
            await forever
        except asyncio.CancelledError:
            pass
    print("server terminated")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger()
    asyncio.run(serve())
