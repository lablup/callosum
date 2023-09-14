import asyncio
import json
import logging
import signal
import tracemalloc
from typing import Mapping, Type

import click

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.ordering import (
    AbstractAsyncScheduler,
    ExitOrderedAsyncScheduler,
    KeySerializedAsyncScheduler,
)
from callosum.rpc import Peer

scheduler_types: Mapping[str, Type[AbstractAsyncScheduler]] = {
    "key-serialized": KeySerializedAsyncScheduler,
    "exit-ordered": ExitOrderedAsyncScheduler,
}

last_snapshot = None
show_output = True
scheduler = None

log = logging.getLogger()


async def handle_echo(request):
    if show_output:
        print("handle_echo()")
    return {
        "received": request.body["sent"],
    }


async def handle_add(request):
    if show_output:
        print("handle_add()")
    return {
        "result": request.body["a"] + request.body["b"],
    }


async def handle_output(request):
    global show_output
    show_output = request.body["enabled"]


async def handle_show_memory_stat(request):
    global last_snapshot, scheduler
    last_snapshot = last_snapshot.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, tracemalloc.__file__),
        )
    )
    new_snapshot = tracemalloc.take_snapshot().filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, tracemalloc.__file__),
        )
    )
    top_stats = new_snapshot.compare_to(last_snapshot, "lineno")
    last_snapshot = new_snapshot
    print("[ Top 10 differences ]")
    for stat in top_stats[:10]:
        print(stat)
    print("[ Scheduler Queue Status ]")
    if isinstance(scheduler, KeySerializedAsyncScheduler):
        print("_tasks", len(scheduler._tasks))
        print("_futures", len(scheduler._futures))
        if hasattr(scheduler, "_pending"):
            print("_pending", len(scheduler._pending))
            print(scheduler._pending)
    elif isinstance(scheduler, ExitOrderedAsyncScheduler):
        print("_tasks", len(scheduler._tasks))


async def handle_long_delay(request):
    if show_output:
        print("handle_long_delay()")
    try:
        await asyncio.sleep(5)
        return {
            "received": request.body["sent"],
        }
    except asyncio.CancelledError:
        if show_output:
            print(" -> cancelled as expected")
        # NOTE: due to strange behaviour of asyncio, I have to reraise
        # otherwise, the task.cancelled() returns False
        raise


async def handle_error(request):
    if show_output:
        print("handle_error()")
    await asyncio.sleep(0.1)
    raise ZeroDivisionError("ooops")


async def serve(scheduler_type: str) -> None:
    global last_snapshot, scheduler

    sched_cls = scheduler_types[scheduler_type]
    scheduler = sched_cls()
    peer = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5020"),
        transport=ZeroMQRPCTransport,
        transport_opts={"attach_monitor": True},
        scheduler=scheduler,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    peer.handle_function("echo", handle_echo)
    peer.handle_function("add", handle_add)
    peer.handle_function("long_delay", handle_long_delay)
    peer.handle_function("error", handle_error)
    peer.handle_function("set_output", handle_output)
    peer.handle_function("memstat", handle_show_memory_stat)

    loop = asyncio.get_running_loop()
    forever = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, forever.cancel)
    loop.add_signal_handler(signal.SIGTERM, forever.cancel)

    try:
        tracemalloc.start(10)
        async with peer:
            last_snapshot = tracemalloc.take_snapshot()
            try:
                log.info("server started")
                await forever
            except asyncio.CancelledError:
                pass
        log.info("server terminated")
    finally:
        tracemalloc.stop()


@click.command()
@click.argument(
    "scheduler_type",
    default="exit-ordered",
    type=click.Choice([*scheduler_types.keys()]),
)
def main(scheduler_type):
    asyncio.run(serve(scheduler_type))


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    main()
