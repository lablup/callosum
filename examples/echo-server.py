import asyncio
import json
import signal

from callosum import Peer


async def handle_echo(func_id, msg):
    return {
        'received': msg['sent'],
    }


async def serve():
    peer = Peer(bind='tcp://127.0.0.1:5000',
                serializer=json.dumps,
                deserializer=json.loads)
    peer.handle_function('echo', handle_echo)
    try:
        await peer.listen()
    except asyncio.CancelledError:
        await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    try:
        task = loop.create_task(serve())
        loop.run_forever()
        # continued here if interrupted
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()
