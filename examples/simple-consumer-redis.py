'''
During the testing, you are supposed to launch
multiple consumers. In this way, you can check
whether messages are distributed among them and
whether each consumer gets only the messages
which have not been obtained by others so far.
'''
import asyncio
import json
import signal

from callosum.pubsub import (
    Consumer,
)
from callosum.lower.dispatch_redis import (
    RedisStreamAddress,
    DispatchRedisTransport,
)


def handle_heartbeat(msg_body):
    print(f"Heartbeat from agent {msg_body['agent_id']} received.")


async def handle_add(msg_body):
    await asyncio.sleep(2)
    addend1, addend2 = msg_body['addends']
    sum = addend1 + addend2
    print(f"{addend1} + {addend2} = {sum}")


async def main_handler(msg):
    if msg.body['type'] == "instance_heartbeat":
        handle_heartbeat(msg.body)
    elif msg.body['type'] == "number_addition":
        await handle_add(msg.body)
    else:
        print("InvalidMessageType: message of type EventTypes was expected.")


async def consume():
    cons = Consumer(
        connect=RedisStreamAddress(
            'redis://localhost:6379',
            'events', 'consumer-group', 'consumer1'),
        deserializer=json.loads,
        transport=DispatchRedisTransport)
    cons.add_handler(main_handler)

    loop = asyncio.get_running_loop()
    forever = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, forever.cancel)
    loop.add_signal_handler(signal.SIGTERM, forever.cancel)
    async with cons:
        try:
            print('consumer started')
            await forever
        except asyncio.CancelledError:
            pass
    print('consumer terminated')


if __name__ == '__main__':
    asyncio.run(consume())
