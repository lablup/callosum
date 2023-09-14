import asyncio
import json
import logging
import os
import random
import secrets
import sys
import textwrap
import traceback

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.rpc import Peer, RPCUserError

log = logging.getLogger()


async def test_simple(peer, initial_delay: float = 0):
    await asyncio.sleep(initial_delay)

    sent_token = secrets.token_hex(16)
    response = await peer.invoke(
        "echo",
        {
            "sent": sent_token,
        },
    )
    print(f"echoed {response['received']}")
    assert response["received"] == sent_token

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
    assert response["result"] == a + b


async def test_timeout(peer):
    try:
        async with asyncio.timeout(0.5):
            await peer.invoke(
                "long_delay",
                {
                    "sent": "some-text",
                },
            )
    except asyncio.TimeoutError:
        print(
            "long_delay(): timeout occurred as expected " "(with per-call timeout)"
        )
    except Exception as e:
        print("long_delay(): unexpected error", e)
        sys.exit(1)
    else:
        print("long_delay(): timeout did not occur!")
        sys.exit(1)

    try:
        await peer.invoke(
            "long_delay",
            {
                "sent": "some-text",
            },
        )
    except asyncio.TimeoutError:
        print("long_delay(): timeout occurred as expected " "(with default timeout)")
    except Exception as e:
        print("long_delay(): unexpected error", e)
        sys.exit(1)
    else:
        print("long_delay(): timeout did not occur!")
        sys.exit(1)


async def test_exception(peer):
    try:
        await peer.invoke("error", {})
    except RPCUserError as e:
        print("error(): catched remote error as expected")
        print(textwrap.indent(e.traceback, prefix="| "))
    except Exception:
        print("error(): did not raise RPCUserError but strange one")
        traceback.print_exc()
        sys.exit(1)
    else:
        print("error(): did not return exception!")
        sys.exit(1)


async def single_client() -> None:
    peer = Peer(
        connect=ZeroMQAddress("tcp://localhost:5020"),
        transport=ZeroMQRPCTransport,
        transport_opts={"attach_monitor": True},
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
        invoke_timeout=2.0,
    )
    async with peer:
        await test_simple(peer)
        await test_exception(peer)
        await test_timeout(peer)
        await test_simple(peer)


async def overlapped_requests() -> None:
    peer = Peer(
        connect=ZeroMQAddress("tcp://localhost:5020"),
        transport=ZeroMQRPCTransport,
        transport_opts={"attach_monitor": True},
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
        invoke_timeout=2.0,
    )
    async with peer:
        t1 = asyncio.create_task(test_simple(peer))
        t2 = asyncio.create_task(test_simple(peer))
        t3 = asyncio.create_task(test_timeout(peer))
        t4 = asyncio.create_task(test_exception(peer))
        t5 = asyncio.create_task(test_timeout(peer))
        t6 = asyncio.create_task(test_simple(peer, initial_delay=0.5))
        t7 = asyncio.create_task(test_simple(peer, initial_delay=1.0))
        results = await asyncio.gather(
            t1, t2, t3, t4, t5, t6, t7, return_exceptions=True
        )
        print(results)
        if any(map(lambda r: isinstance(r, Exception), results)):
            print("There were failed clients.")
            sys.exit(1)


async def multi_clients() -> None:
    peer = Peer(
        connect=ZeroMQAddress("tcp://localhost:5020"),
        transport=ZeroMQRPCTransport,
        transport_opts={"attach_monitor": True},
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
        invoke_timeout=2.0,
    )
    async with peer:
        await peer.invoke("memstat", {})
        await peer.invoke("set_output", {"enabled": False})

        with open(os.devnull, "w", encoding="utf8") as out:
            orig_stdout, sys.stdout = sys.stdout, out
            try:
                for _ in range(10):
                    c1 = asyncio.create_task(single_client())
                    c2 = asyncio.create_task(single_client())
                    c3 = asyncio.create_task(overlapped_requests())
                    await asyncio.gather(c1, c2, c3, return_exceptions=True)
            finally:
                sys.stdout = orig_stdout

        await peer.invoke("set_output", {"enabled": True})
        await peer.invoke("memstat", {})


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )

    print("==== Testing with a single client ====")
    asyncio.run(single_client())

    print("==== Testing with overlapped requests ====")
    asyncio.run(overlapped_requests())

    print("==== Testing with multiple clients ====")
    asyncio.run(multi_clients())
