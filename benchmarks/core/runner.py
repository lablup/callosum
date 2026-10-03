"""
Main benchmark runner orchestration.
"""

import time
import traceback
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


class BenchmarkInterrupted(Exception):
    """Raised when benchmark is interrupted by user or signal."""

    pass


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
        self.failed_scenarios: List[dict] = []
        self._interrupted = False
        self._start_time: Optional[float] = None

    def _create_profiler(self) -> Optional[BenchmarkProfiler]:
        """Create profiler if enabled."""
        if not self.enable_profiling:
            return None

        return BenchmarkProfiler(
            profile_cpu=self.config.profiling.profile_cpu,
            profile_memory=self.config.profiling.profile_memory,
        )

    def _check_interrupted(self) -> None:
        """Check if the benchmark has been interrupted."""
        if self._interrupted:
            raise BenchmarkInterrupted("Benchmark interrupted by user")

    async def _run_single_scenario(
        self,
        scenario_name: str,
        run_func,
        **kwargs,
    ) -> Optional[BenchmarkResult]:
        """
        Run a single scenario with error handling.

        Args:
            scenario_name: Name for logging
            run_func: Async function to run
            **kwargs: Arguments to pass to run_func

        Returns:
            BenchmarkResult if successful, None if failed
        """
        self._check_interrupted()
        try:
            result = await run_func(**kwargs)
            self.all_results.append(result)
            return result
        except BenchmarkInterrupted:
            raise
        except Exception as e:
            error_info = {
                "scenario": scenario_name,
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
            self.failed_scenarios.append(error_info)
            self.console.print_error(f"Scenario '{scenario_name}' failed: {e}")
            return None

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
                result = await self._run_single_scenario(
                    f"throughput-payload-{payload_size}B-{scheduler_name}",
                    payload_scenario.run,
                    payload_size=payload_size,
                    scheduler_type=scheduler_type,
                )
                if result:
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
                result = await self._run_single_scenario(
                    f"throughput-clients-{num_clients}-{scheduler_name}",
                    client_scenario.run,
                    num_clients=num_clients,
                    scheduler_type=scheduler_type,
                )
                if result:
                    results.append(result)

        # Scheduler comparison
        scheduler_scenario = ThroughputSchedulerComparison(
            config=self.config.throughput,
            profiler=self._create_profiler(),
        )

        for scheduler_name in self.config.server.scheduler_types:
            scheduler_type = SchedulerType(scheduler_name)
            self.console.print_info(f"  Comparing scheduler={scheduler_name}")
            result = await self._run_single_scenario(
                f"throughput-scheduler-{scheduler_name}",
                scheduler_scenario.run,
                scheduler_type=scheduler_type,
            )
            if result:
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
            result = await self._run_single_scenario(
                f"latency-load-{target_load}",
                load_scenario.run,
                target_load=target_load,
            )
            if result:
                results.append(result)

        # Latency by payload size
        payload_scenario = LatencyByPayloadSize(
            config=self.config.latency,
            profiler=self._create_profiler(),
        )

        for payload_size in self.config.latency.payload_sizes_test:
            self.console.print_info(f"  Testing payload={payload_size}B")
            result = await self._run_single_scenario(
                f"latency-payload-{payload_size}B",
                payload_scenario.run,
                payload_size=payload_size,
            )
            if result:
                results.append(result)

        # Tail latency analysis
        tail_scenario = TailLatencyAnalysis(
            config=self.config.latency,
            profiler=self._create_profiler(),
        )

        self.console.print_info("  Running tail latency analysis...")
        result = await self._run_single_scenario(
            "latency-tail-analysis",
            tail_scenario.run,
        )
        if result:
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
                result = await self._run_single_scenario(
                    f"compression-{payload_size}B-{'on' if compress else 'off'}",
                    compression_scenario.run,
                    payload_size=payload_size,
                    compress=compress,
                )
                if result:
                    results.append(result)

        # Authentication overhead
        auth_scenario = AuthenticationOverhead(
            config=self.config.features,
            profiler=self._create_profiler(),
        )

        for use_auth in [False, True]:
            self.console.print_info(f"  Testing authentication={use_auth}")
            result = await self._run_single_scenario(
                f"authentication-{'on' if use_auth else 'off'}",
                auth_scenario.run,
                use_auth=use_auth,
            )
            if result:
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
                result = await self._run_single_scenario(
                    f"combined-compress={'on' if compress else 'off'}-auth={'on' if use_auth else 'off'}",
                    combined_scenario.run,
                    compress=compress,
                    use_auth=use_auth,
                )
                if result:
                    results.append(result)

        return results

    def interrupt(self) -> None:
        """Signal the runner to stop after the current scenario."""
        self._interrupted = True

    def get_partial_results(self) -> List[BenchmarkResult]:
        """Get results collected so far (useful after interruption)."""
        return self.all_results

    def get_failed_scenarios(self) -> List[dict]:
        """Get list of failed scenarios with error information."""
        return self.failed_scenarios

    def show_partial_report(self, reason: str = "interrupted") -> None:
        """
        Display a report for partial results.

        Args:
            reason: Reason for partial report ('interrupted' or 'error')
        """
        total_duration = time.time() - (self._start_time or time.time())

        if self.all_results:
            title = f"Partial Benchmark Results ({reason})"
            self.console.show_results_table(self.all_results, title=title)

        self.console.show_summary(
            total_scenarios=len(self.all_results),
            total_duration=total_duration,
            failed_scenarios=len(self.failed_scenarios),
        )

        # Show failed scenarios if any
        if self.failed_scenarios:
            self.console.console.print()
            self.console.console.print("[bold red]Failed Scenarios:[/bold red]")
            for failed in self.failed_scenarios:
                self.console.console.print(
                    f"  - {failed['scenario']}: {failed['error']}"
                )

    async def run_all(
        self, scenario_filter: Optional[str] = None
    ) -> List[BenchmarkResult]:
        """
        Run all benchmarks or filtered scenarios.

        Args:
            scenario_filter: Optional filter - 'throughput', 'latency', 'features', or None for all

        Returns:
            List of all benchmark results

        Raises:
            BenchmarkInterrupted: If interrupted by user signal
        """
        self.console.show_header("Callosum RPC Benchmark Suite")

        self._start_time = time.time()
        self._interrupted = False
        self.all_results = []
        self.failed_scenarios = []

        try:
            # Run selected scenarios
            if scenario_filter is None or scenario_filter == "throughput":
                await self.run_throughput_benchmarks()

            if scenario_filter is None or scenario_filter == "latency":
                await self.run_latency_benchmarks()

            if scenario_filter is None or scenario_filter == "features":
                await self.run_feature_benchmarks()

        except BenchmarkInterrupted:
            # Re-raise to be handled by CLI
            raise

        total_duration = time.time() - self._start_time

        # Display results
        self.console.show_results_table(
            self.all_results, title="All Benchmark Results"
        )
        self.console.show_summary(
            total_scenarios=len(self.all_results),
            total_duration=total_duration,
            failed_scenarios=len(self.failed_scenarios),
        )

        return self.all_results

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
