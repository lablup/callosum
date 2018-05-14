import asyncio
import json
import secrets

from callosum import Peer


async def call():
    peer = Peer(connect='tcp://localhost:5000',
                serializer=json.dumps,
                deserializer=json.loads,
                invoke_timeout=2.0)
    response = await peer.invoke('echo', {
        'sent': secrets.token_hex(16),
    })
    print(response['received'])
    response = await peer.invoke('echo', {
        'sent': secrets.token_hex(16),
    })
    print(response['received'])
    await peer.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(call())
    finally:
        loop.close()
