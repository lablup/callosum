"""
Benchmark server implementation.
"""

import asyncio
import json
from enum import Enum
from typing import Any, Dict, Optional

from callosum.auth import AbstractServerAuthenticator
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.ordering import (
    AbstractAsyncScheduler,
    ExitOrderedAsyncScheduler,
    KeySerializedAsyncScheduler,
)
from callosum.rpc.channel import Peer
from callosum.rpc.message import RPCMessage


class SchedulerType(Enum):
    """Scheduler type enumeration."""

    EXIT_ORDERED = "exit-ordered"
    KEY_SERIALIZED = "key-serialized"


class BenchmarkServer:
    """
    Reusable benchmark server with configurable handlers.

    Follows the pattern from tests/test_rpc.py for consistency.
    """

    def __init__(
        self,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
        bind_address: Optional[ZeroMQAddress] = None,
        compress: bool = False,
        authenticator: Optional[AbstractServerAuthenticator] = None,
    ):
        """
        Initialize benchmark server.

        Args:
            scheduler_type: Type of scheduler to use
            bind_address: ZeroMQ bind address. If None, uses random port on localhost
            compress: Enable Snappy compression
            authenticator: Server authenticator for CURVE encryption
        """
        self.scheduler_type = scheduler_type
        self.bind_address = bind_address or ZeroMQAddress("tcp://127.0.0.1:*")
        self.compress = compress
        self.authenticator = authenticator
        self.done_event = asyncio.Event()
        self.peer: Optional[Peer] = None
        self.actual_address: Optional[str] = None
        self._server_task: Optional[asyncio.Task] = None

    def _create_scheduler(self) -> AbstractAsyncScheduler:
        """Create scheduler instance based on type."""
        if self.scheduler_type == SchedulerType.KEY_SERIALIZED:
            return KeySerializedAsyncScheduler()
        else:
            return ExitOrderedAsyncScheduler()

    async def _echo_handler(self, request: RPCMessage) -> Any:
        """Simple echo handler for latency tests."""
        return request.body

    async def _compute_handler(self, request: RPCMessage) -> Any:
        """CPU-intensive handler for stress tests."""
        # Extract computation parameter from request
        body = request.body
        if isinstance(body, dict):
            iterations = body.get("iterations", 1000)
        else:
            iterations = 1000

        # Perform CPU-intensive work
        result = 0
        for i in range(iterations):
            result += i**2

        return {"result": result, "iterations": iterations}

    async def _memory_handler(self, request: RPCMessage) -> Any:
        """Memory allocation/deallocation handler."""
        body = request.body
        if isinstance(body, dict):
            size_kb = body.get("size_kb", 100)
        else:
            size_kb = 100

        # Allocate memory
        data = bytearray(size_kb * 1024)
        # Fill with pattern
        for i in range(len(data)):
            data[i] = i % 256

        # Return size to confirm allocation
        return {"allocated_kb": size_kb}

    async def _variable_delay_handler(self, request: RPCMessage) -> Any:
        """Handler with variable processing delay."""
        body = request.body
        if isinstance(body, dict):
            delay_ms = body.get("delay_ms", 0)
        else:
            delay_ms = 0

        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        return {"delayed_ms": delay_ms}

    async def _run_server(self) -> None:
        """Run the server (internal coroutine)."""
        scheduler = self._create_scheduler()

        self.peer = Peer(
            bind=self.bind_address,
            transport=ZeroMQRPCTransport,
            scheduler=scheduler,
            serializer=lambda o: json.dumps(o).encode("utf8"),
            deserializer=lambda b: json.loads(b),
            authenticator=self.authenticator,
        )

        # Register all handlers
        self.peer.handle_function("echo", self._echo_handler)
        self.peer.handle_function("compute", self._compute_handler)
        self.peer.handle_function("memory", self._memory_handler)
        self.peer.handle_function("variable_delay", self._variable_delay_handler)

        async with self.peer:
            # Get actual bound address (useful when using random port)
            import zmq

            underlying_sock = self.peer._transport._sock  # type: ignore[attr-defined]
            self.actual_address = underlying_sock.getsockopt(
                zmq.LAST_ENDPOINT
            ).decode("utf-8")

            await self.done_event.wait()

    async def __aenter__(self) -> "BenchmarkServer":
        """Start the server as an async context manager."""
        # Start server in background task
        self._server_task = asyncio.create_task(self._run_server())

        # Wait a bit for server to be ready
        await asyncio.sleep(0.1)

        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the server and verify cleanup."""
        # Signal server to stop
        self.done_event.set()

        # Wait for server task to complete
        if self._server_task:
            await self._server_task

        # Verify no memory leaks (following test pattern)
        if self.peer and self.peer._scheduler:
            scheduler = self.peer._scheduler
            if isinstance(scheduler, KeySerializedAsyncScheduler):
                # Check memory leak
                assert len(scheduler._pending) == 0, (
                    "KeySerializedAsyncScheduler has pending tasks"
                )
                assert len(scheduler._tasks) == 0, (
                    "KeySerializedAsyncScheduler has remaining tasks"
                )
                assert len(scheduler._futures) == 0, (
                    "KeySerializedAsyncScheduler has unfulfilled futures"
                )
            elif isinstance(scheduler, ExitOrderedAsyncScheduler):
                assert len(scheduler._tasks) == 0, (
                    "ExitOrderedAsyncScheduler has remaining tasks"
                )

    def get_connect_address(self) -> ZeroMQAddress:
        """
        Get the address clients should use to connect.

        Returns:
            ZeroMQAddress suitable for client connection
        """
        if self.actual_address:
            # Convert tcp://0.0.0.0:12345 to tcp://localhost:12345
            addr = self.actual_address.replace("0.0.0.0", "localhost").replace(
                "127.0.0.1", "localhost"
            )
            return ZeroMQAddress(addr)
        else:
            return self.bind_address

    def verify_no_memory_leaks(self) -> bool:
        """
        Verify that the scheduler has no pending tasks (no memory leaks).

        Returns:
            True if no memory leaks detected
        """
        if not self.peer or not self.peer._scheduler:
            return True

        scheduler = self.peer._scheduler
        if isinstance(scheduler, KeySerializedAsyncScheduler):
            return (
                len(scheduler._pending) == 0
                and len(scheduler._tasks) == 0
                and len(scheduler._futures) == 0
            )
        elif isinstance(scheduler, ExitOrderedAsyncScheduler):
            return len(scheduler._tasks) == 0

        return True

    def get_scheduler_queue_sizes(self) -> Dict[str, int]:
        """
        Get current scheduler queue sizes for memory tracking.

        Returns:
            Dictionary with queue names and sizes
        """
        if not self.peer or not self.peer._scheduler:
            return {}

        scheduler = self.peer._scheduler
        if isinstance(scheduler, KeySerializedAsyncScheduler):
            return {
                "pending": len(scheduler._pending),
                "tasks": len(scheduler._tasks),
                "futures": len(scheduler._futures),
            }
        elif isinstance(scheduler, ExitOrderedAsyncScheduler):
            return {"tasks": len(scheduler._tasks)}

        return {}
