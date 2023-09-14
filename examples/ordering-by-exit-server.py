import asyncio
import json
import logging
import signal

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.ordering import ExitOrderedAsyncScheduler
from callosum.rpc import Peer


async def handle_echo(request):
    print("echo start")
    await asyncio.sleep(1)
    print("echo done")
    return {
        "received": request.body["sent"],
    }


async def handle_add(request):
    print("add start")
    await asyncio.sleep(0.5)
    print("add done")
    return {
        "result": request.body["a"] + request.body["b"],
    }


async def handle_delimeter(request):
    print("------")


async def serve():
    peer = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5010"),
        transport=ZeroMQRPCTransport,
        scheduler=ExitOrderedAsyncScheduler(),
        serializer=json.dumps,
        deserializer=json.loads,
    )
    peer.handle_function("echo", handle_echo)
    peer.handle_function("add", handle_add)
    peer.handle_function("print_delim", handle_delimeter)

    print("echo() will take 1 second and add() will take 0.5 second.")
    print(
        "You can confirm the effect of scheduler "
        "and the ordering key by the console logs.\n"
    )

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
