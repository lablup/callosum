import asyncio
import json

from callosum.rpc import Peer
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQTransport


async def handle_echo(request):
    print('echo start')
    await asyncio.sleep(1)
    print('echo done')
    return {
        'received': request.body['sent'],
    }


async def handle_add(request):
    print('add start')
    await asyncio.sleep(0.5)
    print('add done')
    return {
        'result': request.body['a'] + request.body['b'],
    }


async def handle_delimeter(request):
    print('------')


async def serve():
    # Peer will use "KeySerializedAsyncScheduler" by default.
    peer = Peer(bind=ZeroMQAddress('tcp://127.0.0.1:5010'),
                transport=ZeroMQTransport,
                serializer=json.dumps,
                deserializer=json.loads)
    peer.handle_function('echo', handle_echo)
    peer.handle_function('add', handle_add)
    peer.handle_function('print_delim', handle_delimeter)

    print('echo() will take 1 second and add() will take 0.5 second.')
    print('You can confirm the effect of scheduler and the ordering key by the '
          'console logs.\n')

    try:
        await peer.open()
        print('listening...')
        await peer.listen()
    except asyncio.CancelledError:
        await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        task = loop.create_task(serve())
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        print('closing...')
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()
        print('closed.')
