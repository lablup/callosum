"""
Latency benchmark scenarios.
"""

from typing import List, Optional

from benchmarks.core.config import LatencyConfig
from benchmarks.core.metrics import BenchmarkResult, RequestMetric
from benchmarks.core.profiler import BenchmarkProfiler
from benchmarks.fixtures.client import MultiClientRunner, warmup_connection
from benchmarks.fixtures.server import BenchmarkServer, SchedulerType
from benchmarks.scenarios.base import BaseBenchmarkScenario
from benchmarks.utils.payload import PayloadGenerator


class LatencyUnderLoad(BaseBenchmarkScenario):
    """
    Measure latency percentiles under different load levels.
    """

    def __init__(
        self,
        config: LatencyConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("latency-under-load", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        target_load: int,  # requests per second
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run latency benchmark under specific load.

        Args:
            target_load: Target load in requests/second
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult with latency percentiles
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
            await warmup_connection(server_address, num_requests=100)

            # Calculate clients needed to achieve target load
            # Assume each client can do ~100 req/s sustainably
            num_clients = max(1, target_load // 100)
            total_requests = target_load * self.config.duration_seconds
            requests_per_client = total_requests // num_clients

            # Rate limit per client
            rate_limit_per_client = target_load / num_clients

            # Create payload generator
            payload_gen = PayloadGenerator(
                size_bytes=self.config.payload_size, kind="random"
            )

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=False,
                )
                return await client.run_concurrent_clients(
                    requests_per_client=requests_per_client,
                    method="echo",
                    payload_generator=payload_gen,
                    rate_limit=rate_limit_per_client,
                    timeout=10.0,
                )

            (
                metrics,
                duration,
                profile_metric,
                memory_metric,
            ) = await self.run_with_profiling(server, run_benchmark)

        # Create result
        result_config = {
            "target_load_rps": target_load,
            "duration_seconds": self.config.duration_seconds,
            "payload_size": self.config.payload_size,
            "num_clients": num_clients,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )


class LatencyByPayloadSize(BaseBenchmarkScenario):
    """
    Measure latency percentiles for different payload sizes.
    """

    def __init__(
        self,
        config: LatencyConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("latency-by-payload-size", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        payload_size: int,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run latency benchmark for specific payload size.

        Args:
            payload_size: Payload size in bytes
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult with latency metrics
        """
        # Fixed concurrency
        num_clients = 10
        requests_per_client = 500

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(server_address, num_requests=100)

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
                    requests_per_client=requests_per_client,
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
            "requests": num_clients * requests_per_client,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )


class TailLatencyAnalysis(BaseBenchmarkScenario):
    """
    Long-running test to identify tail latency and outliers.
    """

    def __init__(
        self,
        config: LatencyConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("tail-latency-analysis", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run long tail latency analysis.

        Args:
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult with detailed latency distribution
        """
        # Long-running test parameters
        num_clients = 10
        total_requests = 10000
        requests_per_client = total_requests // num_clients

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(server_address, num_requests=100)

            # Create payload generator
            payload_gen = PayloadGenerator(size_bytes=1024, kind="random")

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=False,
                )
                return await client.run_concurrent_clients(
                    requests_per_client=requests_per_client,
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
            "total_requests": total_requests,
            "num_clients": num_clients,
            "payload_size": 1024,
            "scheduler": scheduler_type.value,
        }

        return self.create_result(
            config=result_config,
            metrics=metrics,
            duration=duration,
            profile_metric=profile_metric,
            memory_metric=memory_metric,
        )
