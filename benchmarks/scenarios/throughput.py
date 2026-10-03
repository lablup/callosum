"""
Throughput benchmark scenarios.
"""

from typing import List, Optional

from benchmarks.core.config import ThroughputConfig
from benchmarks.core.metrics import BenchmarkResult, RequestMetric
from benchmarks.core.profiler import BenchmarkProfiler
from benchmarks.fixtures.client import MultiClientRunner, warmup_connection
from benchmarks.fixtures.server import BenchmarkServer, SchedulerType
from benchmarks.scenarios.base import BaseBenchmarkScenario
from benchmarks.utils.payload import PayloadGenerator


class ThroughputByPayloadSize(BaseBenchmarkScenario):
    """
    Benchmark throughput with variable payload sizes.
    """

    def __init__(
        self,
        config: ThroughputConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("throughput-by-payload-size", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        payload_size: int,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run throughput benchmark for a specific payload size.

        Args:
            payload_size: Payload size in bytes
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult with throughput metrics
        """
        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address, num_requests=self.config.warmup_requests
            )

            # Create payload generator
            payload_gen = PayloadGenerator(size_bytes=payload_size, kind="random")

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=1,
                    compress=False,
                )
                return await client.run_concurrent_clients(
                    requests_per_client=self.config.requests_per_test,
                    method="echo",
                    payload_generator=payload_gen,
                )

            (
                metrics,
                duration,
                profile_metric,
                memory_metric,
            ) = await self.run_with_profiling(server, run_benchmark)

        # Create result
        result_config = {
            "payload_size": payload_size,
            "num_clients": 1,
            "requests": self.config.requests_per_test,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )


class ThroughputByClientCount(BaseBenchmarkScenario):
    """
    Benchmark throughput with variable client counts.
    """

    def __init__(
        self,
        config: ThroughputConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("throughput-by-client-count", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        num_clients: int,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run throughput benchmark with specific number of clients.

        Args:
            num_clients: Number of concurrent clients
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult with throughput metrics
        """
        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address, num_requests=self.config.warmup_requests
            )

            # Create payload generator (1KB fixed size)
            payload_gen = PayloadGenerator(size_bytes=1024, kind="random")

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=False,
                )
                return await client.run_concurrent_clients(
                    requests_per_client=self.config.requests_per_test
                    // num_clients,  # Distribute requests across clients
                    method="echo",
                    payload_generator=payload_gen,
                )

            (
                metrics,
                duration,
                profile_metric,
                memory_metric,
            ) = await self.run_with_profiling(server, run_benchmark)

        # Create result
        result_config = {
            "payload_size": 1024,
            "num_clients": num_clients,
            "requests": self.config.requests_per_test,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )


class ThroughputSchedulerComparison(BaseBenchmarkScenario):
    """
    Compare throughput between different schedulers.
    """

    def __init__(
        self,
        config: ThroughputConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("throughput-scheduler-comparison", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self, scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED
    ) -> BenchmarkResult:
        """
        Run throughput benchmark with specific scheduler.

        Args:
            scheduler_type: Scheduler type to test

        Returns:
            BenchmarkResult with throughput metrics
        """
        # Fixed parameters for fair comparison
        num_clients = 10
        payload_size = 1024

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address, num_requests=self.config.warmup_requests
            )

            # Create payload generator
            payload_gen = PayloadGenerator(size_bytes=payload_size, kind="random")

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=False,
                )
                return await client.run_concurrent_clients(
                    requests_per_client=self.config.requests_per_test // num_clients,
                    method="echo",
                    payload_generator=payload_gen,
                )

            (
                metrics,
                duration,
                profile_metric,
                memory_metric,
            ) = await self.run_with_profiling(server, run_benchmark)

        # Create result
        result_config = {
            "payload_size": payload_size,
            "num_clients": num_clients,
            "requests": self.config.requests_per_test,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )
