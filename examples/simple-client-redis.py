import asyncio
import json
import random
import secrets

from callosum.rpc import Peer
from callosum.lower.rpc_redis import (
    RedisStreamAddress,
    RPCRedisTransport,
)


async def call():
    peer = Peer(
        connect=RedisStreamAddress(
            "redis://127.0.0.1:6379", "myservice", "client-group", "server1"
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
    asyncio.run(call())
