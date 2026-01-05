"""
Main benchmark runner orchestration.
"""

import time
from pathlib import Path
from typing import List, Optional

from benchmarks.core.config import BenchmarkConfig
from benchmarks.core.metrics import BenchmarkResult
from benchmarks.core.profiler import BenchmarkProfiler
from benchmarks.fixtures.server import SchedulerType
from benchmarks.reporters.console import ConsoleReporter
from benchmarks.reporters.html import HTMLReporter
from benchmarks.scenarios.features import (
    AuthenticationOverhead,
    CombinedFeaturesMatrix,
    CompressionOverhead,
)
from benchmarks.scenarios.latency import (
    LatencyByPayloadSize,
    LatencyUnderLoad,
    TailLatencyAnalysis,
)
from benchmarks.scenarios.throughput import (
    ThroughputByClientCount,
    ThroughputByPayloadSize,
    ThroughputSchedulerComparison,
)


class BenchmarkRunner:
    """
    Main orchestrator for running benchmark suites.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        console_reporter: Optional[ConsoleReporter] = None,
        enable_profiling: bool = True,
    ):
        """
        Initialize benchmark runner.

        Args:
            config: Benchmark configuration
            console_reporter: Optional console reporter (creates default if None)
            enable_profiling: Enable CPU and memory profiling
        """
        self.config = config
        self.console = console_reporter or ConsoleReporter()
        self.enable_profiling = enable_profiling and config.profiling.enabled
        self.all_results: List[BenchmarkResult] = []

    def _create_profiler(self) -> Optional[BenchmarkProfiler]:
        """Create profiler if enabled."""
        if not self.enable_profiling:
            return None

        return BenchmarkProfiler(
            profile_cpu=self.config.profiling.profile_cpu,
            profile_memory=self.config.profiling.profile_memory,
        )

    async def run_throughput_benchmarks(self) -> List[BenchmarkResult]:
        """Run all throughput benchmarks."""
        results = []

        self.console.print_info("Running throughput benchmarks...")

        # Throughput by payload size
        payload_scenario = ThroughputByPayloadSize(
            config=self.config.throughput,
            profiler=self._create_profiler(),
        )

        for payload_size in self.config.throughput.payload_sizes:
            for scheduler_name in self.config.server.scheduler_types:
                scheduler_type = SchedulerType(scheduler_name)
                self.console.print_info(
                    f"  Testing payload={payload_size}B, scheduler={scheduler_name}"
                )
                result = await payload_scenario.run(
                    payload_size=payload_size,
                    scheduler_type=scheduler_type,
                )
                results.append(result)

        # Throughput by client count
        client_scenario = ThroughputByClientCount(
            config=self.config.throughput,
            profiler=self._create_profiler(),
        )

        for num_clients in self.config.throughput.client_counts:
            for scheduler_name in self.config.server.scheduler_types:
                scheduler_type = SchedulerType(scheduler_name)
                self.console.print_info(
                    f"  Testing clients={num_clients}, scheduler={scheduler_name}"
                )
                result = await client_scenario.run(
                    num_clients=num_clients,
                    scheduler_type=scheduler_type,
                )
                results.append(result)

        # Scheduler comparison
        scheduler_scenario = ThroughputSchedulerComparison(
            config=self.config.throughput,
            profiler=self._create_profiler(),
        )

        for scheduler_name in self.config.server.scheduler_types:
            scheduler_type = SchedulerType(scheduler_name)
            self.console.print_info(f"  Comparing scheduler={scheduler_name}")
            result = await scheduler_scenario.run(scheduler_type=scheduler_type)
            results.append(result)

        return results

    async def run_latency_benchmarks(self) -> List[BenchmarkResult]:
        """Run all latency benchmarks."""
        results = []

        self.console.print_info("Running latency benchmarks...")

        # Latency under load
        load_scenario = LatencyUnderLoad(
            config=self.config.latency,
            profiler=self._create_profiler(),
        )

        for target_load in self.config.latency.target_loads:
            self.console.print_info(f"  Testing load={target_load} req/s")
            result = await load_scenario.run(target_load=target_load)
            results.append(result)

        # Latency by payload size
        payload_scenario = LatencyByPayloadSize(
            config=self.config.latency,
            profiler=self._create_profiler(),
        )

        for payload_size in self.config.latency.payload_sizes_test:
            self.console.print_info(f"  Testing payload={payload_size}B")
            result = await payload_scenario.run(payload_size=payload_size)
            results.append(result)

        # Tail latency analysis
        tail_scenario = TailLatencyAnalysis(
            config=self.config.latency,
            profiler=self._create_profiler(),
        )

        self.console.print_info("  Running tail latency analysis...")
        result = await tail_scenario.run()
        results.append(result)

        return results

    async def run_feature_benchmarks(self) -> List[BenchmarkResult]:
        """Run all feature overhead benchmarks."""
        results = []

        self.console.print_info("Running feature overhead benchmarks...")

        # Compression overhead
        compression_scenario = CompressionOverhead(
            config=self.config.features,
            profiler=self._create_profiler(),
        )

        for payload_size in self.config.features.compression_payloads:
            for compress in [False, True]:
                self.console.print_info(
                    f"  Testing compression={compress}, payload={payload_size}B"
                )
                result = await compression_scenario.run(
                    payload_size=payload_size,
                    compress=compress,
                )
                results.append(result)

        # Authentication overhead
        auth_scenario = AuthenticationOverhead(
            config=self.config.features,
            profiler=self._create_profiler(),
        )

        for use_auth in [False, True]:
            self.console.print_info(f"  Testing authentication={use_auth}")
            result = await auth_scenario.run(use_auth=use_auth)
            results.append(result)

        # Combined features matrix
        combined_scenario = CombinedFeaturesMatrix(
            config=self.config.features,
            profiler=self._create_profiler(),
        )

        for compress in [False, True]:
            for use_auth in [False, True]:
                self.console.print_info(
                    f"  Testing compression={compress}, auth={use_auth}"
                )
                result = await combined_scenario.run(
                    compress=compress,
                    use_auth=use_auth,
                )
                results.append(result)

        return results

    async def run_all(
        self, scenario_filter: Optional[str] = None
    ) -> List[BenchmarkResult]:
        """
        Run all benchmarks or filtered scenarios.

        Args:
            scenario_filter: Optional filter - 'throughput', 'latency', 'features', or None for all

        Returns:
            List of all benchmark results
        """
        self.console.show_header("Callosum RPC Benchmark Suite")

        start_time = time.time()
        all_results = []

        # Run selected scenarios
        if scenario_filter is None or scenario_filter == "throughput":
            results = await self.run_throughput_benchmarks()
            all_results.extend(results)

        if scenario_filter is None or scenario_filter == "latency":
            results = await self.run_latency_benchmarks()
            all_results.extend(results)

        if scenario_filter is None or scenario_filter == "features":
            results = await self.run_feature_benchmarks()
            all_results.extend(results)

        total_duration = time.time() - start_time

        # Display results
        self.console.show_results_table(all_results, title="All Benchmark Results")
        self.console.show_summary(
            total_scenarios=len(all_results),
            total_duration=total_duration,
        )

        self.all_results = all_results
        return all_results

    def save_results(
        self,
        output_dir: Path,
        format: str = "both",  # 'html', 'json', 'both'
    ) -> None:
        """
        Save benchmark results to files.

        Args:
            output_dir: Output directory
            format: Output format - 'html', 'json', or 'both'
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        # Save JSON
        if format in ["json", "both"]:
            import json

            json_path = output_dir / f"benchmark-results-{timestamp}.json"
            results_dict = [r.to_dict() for r in self.all_results]
            json_path.write_text(json.dumps(results_dict, indent=2))
            self.console.print_info(f"Saved JSON results to {json_path}")

        # Save HTML
        if format in ["html", "both"]:
            html_path = output_dir / f"benchmark-report-{timestamp}.html"
            html_reporter = HTMLReporter()
            html_reporter.generate_report(
                results=self.all_results,
                output_path=html_path,
            )
            self.console.print_info(f"Saved HTML report to {html_path}")
