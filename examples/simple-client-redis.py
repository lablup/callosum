import asyncio
import json
import logging
import os
import random
import secrets

from callosum.lower.rpc_redis import RedisStreamAddress, RPCRedisTransport
from callosum.rpc import Peer


async def call():
    redis_host = os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    peer = Peer(
        connect=RedisStreamAddress(
            f"redis://{redis_host}:{redis_port}",
            "myservice",
            "client-group",
            "server1",
        ),
        transport=RPCRedisTransport,
        serializer=json.dumps,
        deserializer=json.loads,
        invoke_timeout=3.0,
    )
    async with peer:
        response = await peer.invoke(
            "echo",
            {
                "sent": secrets.token_hex(16),
            },
        )
        print(f"echoed {response['received']}")
        response = await peer.invoke(
            "echo",
            {
                "sent": secrets.token_hex(16),
            },
        )
        print(f"echoed {response['received']}")
        a = random.randint(1, 10)
        b = random.randint(10, 20)
        response = await peer.invoke(
            "add",
            {
                "a": a,
                "b": b,
            },
        )
        print(f"{a} + {b} = {response['result']}")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger()
    asyncio.run(call())
