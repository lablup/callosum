import asyncio
import json
import random
from typing import Any, Awaitable, Callable, List, Mapping, cast

import pytest

from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.ordering import (
    AbstractAsyncScheduler,
    ExitOrderedAsyncScheduler,
    KeySerializedAsyncScheduler,
)
from callosum.rpc.channel import Peer
from callosum.rpc.exceptions import RPCUserError
from callosum.rpc.message import (
    ErrorMetadata,
    NullMetadata,
    RPCMessage,
    RPCMessageTypes,
)


def test_metadata_serialization():
    orig = NullMetadata()
    data = orig.encode()
    out = NullMetadata.decode(data)
    assert out == orig

    orig = ErrorMetadata("MyError", "MyError()", "this is a long traceback")
    data = orig.encode()
    out = ErrorMetadata.decode(data)
    assert out == orig


def test_rpcmessage_exception_serialization():
    request = RPCMessage(
        peer_id=None,
        msgtype=RPCMessageTypes.FUNCTION,
        method="dummy_function",
        order_key="x",
        client_seq_id=1000,
        metadata=NullMetadata(),
        body=b"{}",
    )
    try:
        raise ZeroDivisionError("oops")
    except Exception:
        failure_msg = RPCMessage.failure(request)
        assert failure_msg.msgtype == RPCMessageTypes.FAILURE
        data = failure_msg.encode(json.dumps)
        decoded_failure_msg = RPCMessage.decode(data, json.loads)
        assert decoded_failure_msg == failure_msg


async def dummy_server(
    scheduler: AbstractAsyncScheduler,
    func: Callable[[RPCMessage], Any],
    done_event: asyncio.Event,
) -> None:
    server = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5020"),
        transport=ZeroMQRPCTransport,
        scheduler=scheduler,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    server.handle_function("func", func)
    async with server:
        await done_event.wait()


async def dummy_client(
    requester: Callable[[Peer], Awaitable[None]],
    done_event: asyncio.Event,
) -> None:
    client = Peer(
        connect=ZeroMQAddress("tcp://localhost:5020"),
        transport=ZeroMQRPCTransport,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    async with client:
        await requester(client)
    done_event.set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scheduler_cls",
    [ExitOrderedAsyncScheduler, KeySerializedAsyncScheduler],
)
async def test_messaging(scheduler_cls) -> None:
    done = asyncio.Event()
    total = 50
    call_results: List[int | BaseException] = []
    return_results: List[int] = []
    # a list of events to make fucntions to return in the reversed order
    order_events = [asyncio.Event() for _ in range(total)]

    async def func(request: RPCMessage) -> int:
        # waits a short delay so that it return in the reversed order
        body = cast(Mapping[str, int], request.body)
        total = body["total"]
        idx = body["idx"]
        if idx == 0:
            await asyncio.sleep(0.01)
            for j in range(total, 0, -1):
                order_events[j - 1].set()
                await asyncio.sleep(0.01)  # let the next waiter proceed
        await order_events[idx].wait()
        return idx

    async def requester(client) -> None:
        async def _do_request(idx: int) -> int:
            ret = await client.invoke(
                "func",
                {
                    "total": total,
                    "idx": idx,
                },
                order_key="mykey",
            )
            return_results.append(ret)
            return ret

        tasks = []
        for idx in range(total):
            tasks.append(asyncio.create_task(_do_request(idx)))
        call_results.extend(await asyncio.gather(*tasks, return_exceptions=True))

    scheduler = scheduler_cls()
    server_task = asyncio.create_task(dummy_server(scheduler, func, done))
    client_task = asyncio.create_task(dummy_client(requester, done))
    await asyncio.wait([server_task, client_task])
    if isinstance(scheduler, KeySerializedAsyncScheduler):
        # check memory leak
        assert len(scheduler._pending) == 0
        assert len(scheduler._tasks) == 0
        assert len(scheduler._futures) == 0
    elif isinstance(scheduler, ExitOrderedAsyncScheduler):
        assert len(scheduler._tasks) == 0

    for idx in range(total):
        assert call_results[idx] == idx
        if isinstance(scheduler, KeySerializedAsyncScheduler):
            assert return_results[idx] == idx
        else:
            assert return_results[idx] == total - idx - 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scheduler_cls",
    [ExitOrderedAsyncScheduler, KeySerializedAsyncScheduler],
)
async def test_messaging_server_cancellation(scheduler_cls) -> None:
    done = asyncio.Event()
    total = 200
    cancel_idxs = [False] * 130 + [True] * 70
    random.shuffle(cancel_idxs)
    call_results: List[int | BaseException] = []
    return_results: List[int] = []
    # a list of events to make fucntions to return in the reversed order
    order_events = [asyncio.Event() for _ in range(total)]

    async def func(request: RPCMessage) -> int:
        # waits a short delay so that it return in the reversed order
        body = cast(Mapping[str, int], request.body)
        total = body["total"]
        idx = body["idx"]
        if idx == 0:
            await asyncio.sleep(0.01)
            for j in range(total, 0, -1):
                order_events[j - 1].set()
                await asyncio.sleep(0.01)  # let the next waiter proceed
        await order_events[idx].wait()
        if cancel_idxs[idx]:
            raise asyncio.CancelledError
        return idx

    async def requester(client) -> None:
        async def _do_request(idx: int) -> int:
            try:
                ret = await client.invoke(
                    "func",
                    {
                        "total": total,
                        "idx": idx,
                    },
                    order_key="mykey",
                )
            except asyncio.CancelledError:
                return_results.append(-1)
                raise
            else:
                return_results.append(ret)
            return ret

        tasks = []
        for idx in range(total):
            tasks.append(asyncio.create_task(_do_request(idx)))
        call_results.extend(await asyncio.gather(*tasks, return_exceptions=True))

    scheduler = scheduler_cls()
    server_task = asyncio.create_task(dummy_server(scheduler, func, done))
    client_task = asyncio.create_task(dummy_client(requester, done))
    await asyncio.wait([server_task, client_task])

    if isinstance(scheduler, KeySerializedAsyncScheduler):
        # check memory leak
        assert len(scheduler._pending) == 0
        assert len(scheduler._tasks) == 0
        assert len(scheduler._futures) == 0
    elif isinstance(scheduler, ExitOrderedAsyncScheduler):
        assert len(scheduler._tasks) == 0

    for idx in range(total):
        if cancel_idxs[idx]:
            assert isinstance(call_results[idx], asyncio.CancelledError)
        else:
            assert call_results[idx] == idx


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scheduler_cls",
    [ExitOrderedAsyncScheduler, KeySerializedAsyncScheduler],
)
async def test_messaging_server_error(scheduler_cls) -> None:
    done = asyncio.Event()
    total = 200
    error_idxs = [False] * 130 + [True] * 70
    random.shuffle(error_idxs)
    call_results: List[int | BaseException] = []
    return_results: List[int] = []
    # a list of events to make fucntions to return in the reversed order
    order_events = [asyncio.Event() for _ in range(total)]

    async def func(request: RPCMessage) -> int:
        # waits a short delay so that it return in the reversed order
        body = cast(Mapping[str, int], request.body)
        total = body["total"]
        idx = body["idx"]
        if idx == 0:
            await asyncio.sleep(0.01)
            for j in range(total, 0, -1):
                order_events[j - 1].set()
                await asyncio.sleep(0.01)  # let the next waiter proceed
        await order_events[idx].wait()
        if error_idxs[idx]:
            raise ZeroDivisionError("oops")
        return idx

    async def requester(client) -> None:
        async def _do_request(idx: int) -> int:
            try:
                ret = await client.invoke(
                    "func",
                    {
                        "total": total,
                        "idx": idx,
                    },
                    order_key="mykey",
                )
            except asyncio.CancelledError:
                return_results.append(-1)
                raise
            else:
                return_results.append(ret)
            return ret

        tasks = []
        for idx in range(total):
            tasks.append(asyncio.create_task(_do_request(idx)))
        call_results.extend(await asyncio.gather(*tasks, return_exceptions=True))

    scheduler = scheduler_cls()
    server_task = asyncio.create_task(dummy_server(scheduler, func, done))
    client_task = asyncio.create_task(dummy_client(requester, done))
    await asyncio.wait([server_task, client_task])

    if isinstance(scheduler, KeySerializedAsyncScheduler):
        # check memory leak
        assert len(scheduler._pending) == 0
        assert len(scheduler._tasks) == 0
        assert len(scheduler._futures) == 0
    elif isinstance(scheduler, ExitOrderedAsyncScheduler):
        assert len(scheduler._tasks) == 0

    for idx in range(total):
        if error_idxs[idx]:
            assert isinstance(call_results[idx], RPCUserError)
            e = cast(RPCUserError, call_results[idx])
            assert e.args[0] == "ZeroDivisionError"
        else:
            assert call_results[idx] == idx


@pytest.mark.asyncio
async def test_external_context_injection() -> None:
    """Test that an externally injected zmq context is used and not destroyed on close."""
    import zmq.asyncio

    # Create an external context
    external_zctx = zmq.asyncio.Context()

    async def func(request: RPCMessage) -> str:
        return "ok"

    # Create server with external context
    server = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5021"),
        transport=ZeroMQRPCTransport,
        transport_opts={"zctx": external_zctx},
        scheduler=ExitOrderedAsyncScheduler(),
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    server.handle_function("func", func)

    # Create client with external context
    client = Peer(
        connect=ZeroMQAddress("tcp://localhost:5021"),
        transport=ZeroMQRPCTransport,
        transport_opts={"zctx": external_zctx},
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )

    # Verify the external context is used
    server_transport = cast(ZeroMQRPCTransport, server._transport)
    client_transport = cast(ZeroMQRPCTransport, client._transport)
    assert server_transport._zctx is external_zctx
    assert server_transport._external_zctx is True
    assert client_transport._zctx is external_zctx
    assert client_transport._external_zctx is True

    async with server:
        async with client:
            result = await client.invoke("func", {})
            assert result == "ok"

    # Verify context is NOT destroyed after transport close
    assert not external_zctx.closed

    # Clean up
    external_zctx.destroy(linger=0)


@pytest.mark.asyncio
async def test_internal_context_destroyed_on_close() -> None:
    """Test that internally created zmq context is destroyed on close."""

    async def func(request: RPCMessage) -> str:
        return "ok"

    # Create server without external context (uses internal)
    server = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5022"),
        transport=ZeroMQRPCTransport,
        scheduler=ExitOrderedAsyncScheduler(),
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    server.handle_function("func", func)

    # Create client without external context (uses internal)
    client = Peer(
        connect=ZeroMQAddress("tcp://localhost:5022"),
        transport=ZeroMQRPCTransport,
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )

    # Verify internal context is created
    server_transport = cast(ZeroMQRPCTransport, server._transport)
    client_transport = cast(ZeroMQRPCTransport, client._transport)
    assert server_transport._external_zctx is False
    assert client_transport._external_zctx is False
    server_zctx = server_transport._zctx
    client_zctx = client_transport._zctx

    async with server:
        async with client:
            result = await client.invoke("func", {})
            assert result == "ok"

    # Verify context IS destroyed after transport close
    assert server_zctx.closed
    assert client_zctx.closed


@pytest.mark.asyncio
async def test_req_idmap_emptied_after_overlapping_calls() -> None:
    """
    Regression test for the done-callback closure bug: the callback captured
    the receive-loop variables by reference, so a function task that finished
    after a newer request had arrived popped the newer request's entry from
    ``_req_idmap`` instead of its own, stranding its own entry forever (and
    silently disabling CANCEL for the newer request).
    """
    done = asyncio.Event()
    first_may_return = asyncio.Event()

    async def func(request: RPCMessage) -> int:
        body = cast(Mapping[str, int], request.body)
        if body["idx"] == 0:
            # Keep the first call in flight until the second call completes,
            # so the first task's done-callback runs only after the receive
            # loop has moved on to the newer request.
            await first_may_return.wait()
        else:
            first_may_return.set()
        return body["idx"]

    server = Peer(
        bind=ZeroMQAddress("tcp://127.0.0.1:5021"),
        transport=ZeroMQRPCTransport,
        scheduler=ExitOrderedAsyncScheduler(),
        serializer=lambda o: json.dumps(o).encode("utf8"),
        deserializer=lambda b: json.loads(b),
    )
    server.handle_function("func", func)

    async def serve() -> None:
        async with server:
            await done.wait()

    async def request() -> None:
        client = Peer(
            connect=ZeroMQAddress("tcp://localhost:5021"),
            transport=ZeroMQRPCTransport,
            serializer=lambda o: json.dumps(o).encode("utf8"),
            deserializer=lambda b: json.loads(b),
        )
        async with client:
            first = asyncio.create_task(
                client.invoke("func", {"idx": 0}, order_key="k0")
            )
            # Make sure the first request is received and its task is waiting
            # before the second request rebinds the receive-loop variables.
            await asyncio.sleep(0.2)
            second = asyncio.create_task(
                client.invoke("func", {"idx": 1}, order_key="k1")
            )
            results = await asyncio.gather(first, second)
            assert sorted(cast(List[int], results)) == [0, 1]
        done.set()

    server_task = asyncio.create_task(serve())
    client_task = asyncio.create_task(request())
    await asyncio.gather(server_task, client_task)
    assert server._req_idmap == {}
