"""
Feature overhead benchmark scenarios.
"""

from typing import List, Optional

from benchmarks.core.config import FeatureConfig
from benchmarks.core.metrics import BenchmarkResult, RequestMetric
from benchmarks.core.profiler import BenchmarkProfiler
from benchmarks.fixtures.client import MultiClientRunner, warmup_connection
from benchmarks.fixtures.server import BenchmarkServer, SchedulerType
from benchmarks.scenarios.base import BaseBenchmarkScenario
from benchmarks.utils.auth_helpers import (
    DummyClientAuthenticator,
    DummyServerAuthenticator,
    setup_benchmark_auth,
)
from benchmarks.utils.payload import PayloadGenerator


class CompressionOverhead(BaseBenchmarkScenario):
    """
    Measure performance impact of Snappy compression.
    """

    def __init__(
        self,
        config: FeatureConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("compression-overhead", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        payload_size: int,
        compress: bool,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run benchmark with/without compression.

        Args:
            payload_size: Payload size in bytes
            compress: Enable compression
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult showing compression impact
        """
        num_clients = 10
        requests_per_client = self.config.requests_per_test // num_clients

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=compress,
            authenticator=None,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address, num_requests=100, compress=compress
            )

            # Use compressible payload for fair comparison
            payload_gen = PayloadGenerator(
                size_bytes=payload_size, kind="compressible"
            )

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=compress,
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
            "compress": compress,
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


class AuthenticationOverhead(BaseBenchmarkScenario):
    """
    Measure performance impact of CURVE encryption.
    """

    def __init__(
        self,
        config: FeatureConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("authentication-overhead", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        use_auth: bool,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run benchmark with/without CURVE authentication.

        Args:
            use_auth: Enable CURVE authentication
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult showing auth impact
        """
        num_clients = 10
        requests_per_client = self.config.requests_per_test // num_clients
        payload_size = 1024

        # Setup authentication if needed
        server_auth: Optional[DummyServerAuthenticator] = None
        client_auths: Optional[List[DummyClientAuthenticator]] = None

        if use_auth:
            server_auth, client_auths = setup_benchmark_auth(num_clients=num_clients)

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=False,
            authenticator=server_auth,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address,
                num_requests=100,
                authenticator=client_auths[0] if client_auths else None,
            )

            # Create payload generator
            payload_gen = PayloadGenerator(size_bytes=payload_size, kind="random")

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=False,
                    authenticators=client_auths,  # type: ignore[arg-type]
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
            "use_auth": use_auth,
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


class CombinedFeaturesMatrix(BaseBenchmarkScenario):
    """
    Test combinations of compression and authentication.
    """

    def __init__(
        self,
        config: FeatureConfig,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        super().__init__("combined-features-matrix", profiler)
        self.config = config

    async def run(  # type: ignore[override]
        self,
        compress: bool,
        use_auth: bool,
        scheduler_type: SchedulerType = SchedulerType.EXIT_ORDERED,
    ) -> BenchmarkResult:
        """
        Run benchmark with specific feature combination.

        Args:
            compress: Enable compression
            use_auth: Enable authentication
            scheduler_type: Scheduler type to use

        Returns:
            BenchmarkResult for this feature combination
        """
        num_clients = 10
        requests_per_client = self.config.requests_per_test // num_clients
        payload_size = 10240  # 10KB

        # Setup authentication if needed
        server_auth: Optional[DummyServerAuthenticator] = None
        client_auths: Optional[List[DummyClientAuthenticator]] = None

        if use_auth:
            server_auth, client_auths = setup_benchmark_auth(num_clients=num_clients)

        # Start server
        server = BenchmarkServer(
            scheduler_type=scheduler_type,
            compress=compress,
            authenticator=server_auth,
        )

        async with server:
            server_address = server.get_connect_address()

            # Warmup
            await warmup_connection(
                server_address,
                num_requests=100,
                compress=compress,
                authenticator=client_auths[0] if client_auths else None,
            )

            # Create payload generator
            payload_gen = PayloadGenerator(
                size_bytes=payload_size,
                kind="compressible" if compress else "random",
            )

            # Run benchmark with profiling
            async def run_benchmark() -> List[RequestMetric]:
                client = MultiClientRunner(
                    server_address=server_address,
                    num_clients=num_clients,
                    compress=compress,
                    authenticators=client_auths,  # type: ignore[arg-type]
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
            "compress": compress,
            "use_auth": use_auth,
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
