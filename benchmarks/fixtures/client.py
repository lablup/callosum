"""
Benchmark client implementation.
"""

import asyncio
import json
import time
from typing import Any, Callable, List, Optional, Tuple

from benchmarks.core.metrics import RequestMetric
from callosum.auth import AbstractClientAuthenticator
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from callosum.rpc.channel import Peer


class BenchmarkClient:
    """
    Single client for benchmark workloads.

    Handles request execution with timing and error tracking.
    """

    def __init__(
        self,
        connect_address: ZeroMQAddress,
        compress: bool = False,
        authenticator: Optional[AbstractClientAuthenticator] = None,
    ):
        """
        Initialize benchmark client.

        Args:
            connect_address: ZeroMQ address to connect to
            compress: Enable Snappy compression
            authenticator: Client authenticator for CURVE encryption
        """
        self.connect_address = connect_address
        self.compress = compress
        self.authenticator = authenticator
        self.peer: Optional[Peer] = None

    async def __aenter__(self) -> "BenchmarkClient":
        """Connect the client as an async context manager."""
        self.peer = Peer(
            connect=self.connect_address,
            transport=ZeroMQRPCTransport,
            serializer=lambda o: json.dumps(o).encode("utf8"),
            deserializer=lambda b: json.loads(b),
            authenticator=self.authenticator,
        )
        await self.peer.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Disconnect the client."""
        if self.peer:
            await self.peer.__aexit__(exc_type, exc_val, exc_tb)

    async def invoke(
        self,
        method: str,
        body: Any,
        timeout: Optional[float] = None,
    ) -> Tuple[Any, float]:
        """
        Invoke a single RPC method and measure latency.

        Args:
            method: Method name to invoke
            body: Request body
            timeout: Optional timeout in seconds

        Returns:
            Tuple of (response, latency_ms)

        Raises:
            Exception: If RPC call fails
        """
        if not self.peer:
            raise RuntimeError("Client not connected")

        start_time = time.perf_counter()
        try:
            result = await self.peer.invoke(method, body, invoke_timeout=timeout)
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000.0
            return result, latency_ms
        except Exception:
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000.0
            raise

    async def run_requests(
        self,
        count: int,
        method: str,
        payload_generator: Callable[[], Any],
        rate_limit: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> List[RequestMetric]:
        """
        Run multiple requests with timing.

        Args:
            count: Number of requests to make
            method: Method name to invoke
            payload_generator: Callable that generates request payload
            rate_limit: Optional rate limit in requests/second
            timeout: Optional timeout per request in seconds

        Returns:
            List of RequestMetric for each request
        """
        metrics = []
        delay_between_requests = (1.0 / rate_limit) if rate_limit else 0.0

        for i in range(count):
            payload = payload_generator()
            payload_size = len(
                json.dumps(payload).encode("utf8")
            )  # Approximate size

            timestamp = time.time()
            try:
                _, latency_ms = await self.invoke(method, payload, timeout=timeout)
                metrics.append(
                    RequestMetric(
                        timestamp=timestamp,
                        latency_ms=latency_ms,
                        payload_size_bytes=payload_size,
                        success=True,
                    )
                )
            except Exception as e:
                # Record failed request
                metrics.append(
                    RequestMetric(
                        timestamp=timestamp,
                        latency_ms=0.0,
                        payload_size_bytes=payload_size,
                        success=False,
                        error=str(e),
                    )
                )

            # Rate limiting
            if delay_between_requests > 0 and i < count - 1:
                await asyncio.sleep(delay_between_requests)

        return metrics


class MultiClientRunner:
    """
    Coordinates multiple clients for concurrent load testing.
    """

    def __init__(
        self,
        server_address: ZeroMQAddress,
        num_clients: int,
        compress: bool = False,
        authenticators: Optional[List[Optional[AbstractClientAuthenticator]]] = None,
    ):
        """
        Initialize multi-client runner.

        Args:
            server_address: Server address to connect to
            num_clients: Number of concurrent clients
            compress: Enable compression
            authenticators: Optional list of authenticators (one per client)
        """
        self.server_address = server_address
        self.num_clients = num_clients
        self.compress = compress
        self.authenticators: List[Optional[AbstractClientAuthenticator]] = (
            authenticators if authenticators is not None else [None] * num_clients
        )

        if len(self.authenticators) != num_clients:
            raise ValueError("Number of authenticators must match number of clients")

    async def run_concurrent_clients(
        self,
        requests_per_client: int,
        method: str,
        payload_generator: Callable[[], Any],
        rate_limit: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> List[RequestMetric]:
        """
        Run requests concurrently across multiple clients.

        Args:
            requests_per_client: Number of requests each client should make
            method: Method name to invoke
            payload_generator: Callable that generates payloads
            rate_limit: Optional rate limit per client in requests/second
            timeout: Optional timeout per request

        Returns:
            Aggregated list of RequestMetric from all clients
        """

        async def client_worker(client_id: int) -> List[RequestMetric]:
            """Worker function for a single client."""
            client = BenchmarkClient(
                connect_address=self.server_address,
                compress=self.compress,
                authenticator=self.authenticators[client_id],
            )
            async with client:
                return await client.run_requests(
                    count=requests_per_client,
                    method=method,
                    payload_generator=payload_generator,
                    rate_limit=rate_limit,
                    timeout=timeout,
                )

        # Run all clients concurrently
        tasks = [
            asyncio.create_task(client_worker(i)) for i in range(self.num_clients)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Aggregate metrics from all clients
        all_metrics = []
        for metrics in results:
            all_metrics.extend(metrics)

        return all_metrics


async def warmup_connection(
    server_address: ZeroMQAddress,
    num_requests: int = 10,
    compress: bool = False,
    authenticator: Optional[AbstractClientAuthenticator] = None,
) -> None:
    """
    Perform warmup requests to establish connection and warm up caches.

    Args:
        server_address: Server address to connect to
        num_requests: Number of warmup requests
        compress: Enable compression
        authenticator: Optional authenticator
    """
    client = BenchmarkClient(
        connect_address=server_address,
        compress=compress,
        authenticator=authenticator,
    )

    async with client:
        for _ in range(num_requests):
            try:
                await client.invoke("echo", {"warmup": True}, timeout=5.0)
            except Exception:
                # Ignore warmup errors
                pass

    # Small delay to let connection fully establish
    await asyncio.sleep(0.1)
