"""
During the testing, you can launch multiple publishers
simultaneously, so as to check whether messages from
multiple publishers are distributed among the consumers.
"""
import asyncio
import json
import logging
import os
import random
import secrets

from callosum.lower.dispatch_redis import DispatchRedisTransport, RedisStreamAddress
from callosum.pubsub import Publisher


async def publish() -> None:
    redis_host = os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    pub = Publisher(
        bind=RedisStreamAddress(f"redis://{redis_host}:{redis_port}", "events"),
        serializer=lambda d: json.dumps(d).encode("utf8"),
        transport=DispatchRedisTransport,
    )
    agent_id = secrets.token_hex(2)  # publisher id

    async def heartbeats():
        for _ in range(3):
            await asyncio.sleep(1)
            msg_body = {
                "type": "instance_heartbeat",
                "agent_id": agent_id,
            }
            pub.push(msg_body)
            print("pushed heartbeat")

    async def addition_event(addend1: int, addend2: int):
        await asyncio.sleep(1.5)
        msg_body = {
            "type": "number_addition",
            "addends": (addend1, addend2),
        }
        pub.push(msg_body)
        print("pushed addition event")

    async with pub:
        task1 = asyncio.create_task(heartbeats())
        addend1 = random.randint(1, 10)
        addend2 = random.randint(10, 20)
        task2 = asyncio.create_task(addition_event(addend1, addend2))
        await asyncio.gather(task1, task2)
    print("publisher done")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger()
    asyncio.run(publish())
