'''
During the testing, you can launch multiple publishers
simultaneously, so as to check whether messages from
multiple publishers are distributed among the consumers.
'''
import asyncio
import json
import random
import secrets
from datetime import datetime
from dateutil.tz import tzutc

from callosum.pubsub import (
    Publisher,
)
from callosum.lower.dispatch_redis import (
    RedisStreamAddress,
    DispatchRedisTransport,
)


async def publish() -> None:
    pub = Publisher(
        bind=RedisStreamAddress(
            'redis://localhost:6379',
            'events'),
        serializer=lambda d: json.dumps(d).encode('utf8'),
        transport=DispatchRedisTransport)
    agent_id = secrets.token_hex(2)  # publisher id

    async def heartbeats():
        for _ in range(3):
            await asyncio.sleep(1)
            msg_body = {
                'type': "instance_heartbeat",
                'agent_id': agent_id,
            }
            pub.push(msg_body,
                     datetime.now(tzutc()))
            print("pushed heartbeat")

    async def addition_event(addend1: int, addend2: int):
        await asyncio.sleep(1.5)
        msg_body = {
            'type': "number_addition",
            'addends': (addend1, addend2),
        }
        pub.push(msg_body,
                 datetime.now(tzutc()))
        print("pushed addition event")

    async with pub:
        task1 = asyncio.create_task(heartbeats())
        addend1 = random.randint(1, 10)
        addend2 = random.randint(10, 20)
        task2 = asyncio.create_task(addition_event(addend1, addend2))
        await asyncio.gather(task1, task2)
    print('publisher done')


if __name__ == '__main__':
    asyncio.run(publish())
