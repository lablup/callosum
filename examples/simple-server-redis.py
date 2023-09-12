import asyncio
import json
import logging
import os
import signal
import time

from callosum.lower.rpc_redis import RedisStreamAddress, RPCRedisTransport
from callosum.rpc import Peer


async def handle_echo(request):
    # NOTE: Adding this part with "await asyncio.sleep(1)" breaks the code.
    time.sleep(1.0)
    print("After sleeping")
    return {
        "received": request.body["sent"],
    }


async def handle_add(request):
    return {
        "result": request.body["a"] + request.body["b"],
    }


async def serve() -> None:
    redis_host = os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    peer = Peer(
        bind=RedisStreamAddress(
            f"redis://{redis_host}:{redis_port}",
            "myservice",
            "server-group",
            "client1",
        ),
        transport=RPCRedisTransport,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    peer.handle_function("echo", handle_echo)
    peer.handle_function("add", handle_add)

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
