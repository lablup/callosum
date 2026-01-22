"""
Base benchmark scenario class.
"""

import time
from abc import ABC
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from benchmarks.core.metrics import (
    BenchmarkResult,
    MemoryMetric,
    ProfileMetric,
    RequestMetric,
)
from benchmarks.core.profiler import BenchmarkProfiler
from benchmarks.fixtures.server import BenchmarkServer
from benchmarks.utils.statistics import (
    calculate_latency_metrics,
    calculate_throughput,
)


class BaseBenchmarkScenario(ABC):
    """
    Abstract base class for benchmark scenarios.

    Provides common setup/teardown and result aggregation logic.
    """

    def __init__(
        self,
        scenario_name: str,
        profiler: Optional[BenchmarkProfiler] = None,
    ):
        """
        Initialize benchmark scenario.

        Args:
            scenario_name: Name of the scenario for reporting
            profiler: Optional profiler for CPU/memory tracking
        """
        self.scenario_name = scenario_name
        self.profiler = profiler

    async def run(self, **kwargs: Any) -> BenchmarkResult:
        """
        Run the benchmark scenario.

        Subclasses should override this method with scenario-specific parameters.

        Args:
            **kwargs: Scenario-specific parameters

        Returns:
            BenchmarkResult with metrics
        """
        raise NotImplementedError("Subclasses must implement run()")

    def create_result(
        self,
        config: Dict[str, Any],
        metrics: List[RequestMetric],
        duration: float,
        profile_metric: Optional[ProfileMetric] = None,
        memory_metric: Optional[MemoryMetric] = None,
    ) -> BenchmarkResult:
        """
        Create BenchmarkResult from collected metrics.

        Args:
            config: Benchmark configuration dictionary
            metrics: List of request metrics
            duration: Total duration in seconds
            profile_metric: Optional CPU profiling metric
            memory_metric: Optional memory profiling metric

        Returns:
            Complete BenchmarkResult
        """
        # Calculate latency and throughput
        latency = calculate_latency_metrics(metrics)
        throughput = calculate_throughput(metrics, duration)

        return BenchmarkResult(
            scenario_name=self.scenario_name,
            config=config,
            throughput=throughput,
            latency=latency,
            memory=memory_metric,
            profile=profile_metric,
            raw_metrics=metrics,
        )

    async def run_with_profiling(
        self,
        server: BenchmarkServer,
        benchmark_func: Callable[..., Awaitable[List[RequestMetric]]],
        **kwargs: Any,
    ) -> Tuple[
        List[RequestMetric], float, Optional[ProfileMetric], Optional[MemoryMetric]
    ]:
        """
        Run benchmark with profiling enabled.

        Args:
            server: BenchmarkServer instance
            benchmark_func: Async function that runs the benchmark
            **kwargs: Arguments to pass to benchmark_func

        Returns:
            BenchmarkResult with profiling data
        """
        if self.profiler:
            async with self.profiler:
                start_time = time.perf_counter()
                metrics = await benchmark_func(**kwargs)
                duration = time.perf_counter() - start_time

            # Get profiling results
            scheduler_queue_sizes = server.get_scheduler_queue_sizes()
            profile_metric, memory_metric = self.profiler.get_results(
                scheduler_queue_sizes
            )
        else:
            start_time = time.perf_counter()
            metrics = await benchmark_func(**kwargs)
            duration = time.perf_counter() - start_time
            profile_metric = None
            memory_metric = None

        return metrics, duration, profile_metric, memory_metric


class MultiIterationRunner:
    """
    Runs a scenario multiple times and aggregates results.
    """

    def __init__(self, iterations: int = 3, warmup_iterations: int = 1):
        """
        Initialize multi-iteration runner.

        Args:
            iterations: Number of measurement iterations
            warmup_iterations: Number of warmup iterations (not measured)
        """
        self.iterations = iterations
        self.warmup_iterations = warmup_iterations

    async def run_scenario(
        self, scenario: BaseBenchmarkScenario, **kwargs
    ) -> List[BenchmarkResult]:
        """
        Run a scenario multiple times.

        Args:
            scenario: Scenario to run
            **kwargs: Arguments to pass to scenario.run()

        Returns:
            List of BenchmarkResult from each iteration
        """
        results = []

        # Warmup iterations (not measured)
        for i in range(self.warmup_iterations):
            await scenario.run(**kwargs)

        # Measurement iterations
        for i in range(self.iterations):
            result = await scenario.run(**kwargs)
            results.append(result)

        return results
