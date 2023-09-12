"""
During the testing, you are supposed to launch
multiple consumers. In this way, you can check
whether messages are distributed among them and
whether each consumer gets only the messages
which have not been obtained by others so far.
"""
import asyncio
import json
import logging
import os
import signal

from callosum.lower.dispatch_redis import DispatchRedisTransport, RedisStreamAddress
from callosum.pubsub import Consumer


def handle_heartbeat(msg_body):
    print(f"Heartbeat from agent {msg_body['agent_id']} received.")


async def handle_add(msg_body):
    await asyncio.sleep(2)
    addend1, addend2 = msg_body["addends"]
    sum = addend1 + addend2
    print(f"{addend1} + {addend2} = {sum}")


async def main_handler(msg):
    if msg.body["type"] == "instance_heartbeat":
        handle_heartbeat(msg.body)
    elif msg.body["type"] == "number_addition":
        await handle_add(msg.body)
    else:
        print("InvalidMessageType: message of type EventTypes was expected.")


async def consume() -> None:
    redis_host = os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    cons = Consumer(
        connect=RedisStreamAddress(
            f"redis://{redis_host}:{redis_port}",
            "events",
            "consumer-group",
            "consumer1",
        ),
        deserializer=lambda b: json.loads(b),
        transport=DispatchRedisTransport,
    )
    cons.add_handler(main_handler)

    loop = asyncio.get_running_loop()
    forever = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, forever.cancel)
    loop.add_signal_handler(signal.SIGTERM, forever.cancel)
    async with cons:
        try:
            print("consumer started")
            await forever
        except asyncio.CancelledError:
            pass
    print("consumer terminated")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger()
    asyncio.run(consume())
