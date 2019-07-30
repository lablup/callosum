'''
During the testing, you are supposed to launch
multiple subscribers. In this way, you can check
whether messages are distributed among them and
whether each subscriber gets only the messages
which have not been obtained by others so far.
'''
import asyncio
from aiohttp import web
import json

from callosum import (
    Subscriber,
    EventTypes,
)
from callosum.lower.redis import (
    RedisStreamAddress,
    RedisStreamTransport,
)


def handle_heartbeat(app: web.Application,
                agent_id: str):
    print(f"Heartbeat from agent {agent_id} received.")


async def handle_add(app: web.Application,
                     agent_id: str,
                     addend1: int,
                     addend2: int):
    await asyncio.sleep(2)
    sum = addend1 + addend2
    print(f"{addend1} + {addend2} = {sum}")


async def serve():
    sub = Subscriber(connect=RedisStreamAddress(
                      'redis://localhost:6379',
                      'events', 'subscriber-group', 'consumer1'),
                      deserializer=json.loads,
                      transport=RedisStreamTransport)
    app = web.Application
    sub.add_handler(EventTypes.INSTANCE_HEARTBEAT, app, handle_heartbeat)
    # handle_add is just random handler check and
    # has no logical connection with INSTANCE_STARTED event.
    sub.add_handler(EventTypes.INSTANCE_STARTED, app, handle_add)
    try:
        await sub.open()
        print("listening task has started...")
        await sub.listen()
    except asyncio.CancelledError:
        await sub.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    try:
        task = loop.create_task(serve())
        print('listening...')
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        print('closing...')
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()
        print('closed.')