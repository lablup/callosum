"""
Console reporter using Rich for formatted output.
"""

from typing import List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from benchmarks.core.metrics import BenchmarkResult


class ConsoleReporter:
    """
    Rich console output for benchmark results.
    """

    def __init__(self):
        """Initialize console reporter."""
        self.console = Console()

    def show_header(self, title: str) -> None:
        """
        Display benchmark header.

        Args:
            title: Benchmark suite title
        """
        self.console.print()
        self.console.rule(f"[bold blue]{title}", style="blue")
        self.console.print()

    def create_progress(
        self, description: str = "Running benchmarks..."
    ) -> Progress:
        """
        Create a progress bar for benchmark execution.

        Args:
            description: Progress description

        Returns:
            Progress instance
        """
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
        )

    def show_results_table(
        self,
        results: List[BenchmarkResult],
        title: str = "Benchmark Results",
    ) -> None:
        """
        Display results in a formatted table.

        Args:
            results: List of benchmark results
            title: Table title
        """
        if not results:
            self.console.print("[yellow]No results to display[/yellow]")
            return

        table = Table(title=title, show_header=True, header_style="bold magenta")

        # Add columns
        table.add_column("Scenario", style="cyan", no_wrap=True)
        table.add_column("Config", style="dim")
        table.add_column("Throughput\n(req/s)", justify="right", style="green")
        table.add_column("P50\n(ms)", justify="right")
        table.add_column("P95\n(ms)", justify="right")
        table.add_column("P99\n(ms)", justify="right")
        table.add_column("Success\n(%)", justify="right")
        table.add_column("Status", justify="center")

        # Add rows
        for result in results:
            # Format config string
            config_parts = []
            if "payload_size" in result.config:
                size_kb = result.config["payload_size"] / 1024
                config_parts.append(f"{size_kb:.0f}KB")
            if "num_clients" in result.config:
                config_parts.append(f"{result.config['num_clients']}c")
            if "scheduler" in result.config:
                sched = result.config["scheduler"]
                if sched == "exit-ordered":
                    config_parts.append("exit")
                elif sched == "key-serialized":
                    config_parts.append("key-ser")
            if "compress" in result.config:
                if result.config["compress"]:
                    config_parts.append("comp")
            if "use_auth" in result.config:
                if result.config["use_auth"]:
                    config_parts.append("auth")

            config_str = ", ".join(config_parts) if config_parts else "-"

            # Status indicator
            success_rate = result.throughput.success_rate
            if success_rate == 100.0:
                status = "[green]✓[/green]"
            elif success_rate >= 95.0:
                status = "[yellow]⚠[/yellow]"
            else:
                status = "[red]✗[/red]"

            # Add memory leak warning
            if result.memory and result.memory.has_memory_leak:
                status = "[red]⚠ LEAK[/red]"

            table.add_row(
                result.scenario_name,
                config_str,
                f"{result.throughput.requests_per_second:,.0f}",
                f"{result.latency.median_ms:.2f}",
                f"{result.latency.p95_ms:.2f}",
                f"{result.latency.p99_ms:.2f}",
                f"{success_rate:.1f}",
                status,
            )

        self.console.print(table)
        self.console.print()

    def show_summary(
        self,
        total_scenarios: int,
        total_duration: float,
        failed_scenarios: int = 0,
    ) -> None:
        """
        Display benchmark summary.

        Args:
            total_scenarios: Total number of scenarios run
            total_duration: Total duration in seconds
            failed_scenarios: Number of failed scenarios
        """
        summary_text = Text()
        summary_text.append("Completed: ", style="bold")
        summary_text.append(f"{total_scenarios} scenarios ", style="cyan")
        summary_text.append(f"in {total_duration:.1f}s\n", style="dim")

        if failed_scenarios > 0:
            summary_text.append(
                f"Failed: {failed_scenarios} scenarios\n", style="red bold"
            )

        panel = Panel(summary_text, title="Summary", border_style="green")
        self.console.print(panel)

    def show_comparison(
        self,
        current: BenchmarkResult,
        baseline: Optional[BenchmarkResult],
    ) -> None:
        """
        Show side-by-side comparison with baseline.

        Args:
            current: Current benchmark result
            baseline: Baseline benchmark result
        """
        if baseline is None:
            self.print_warning("No baseline provided for comparison")
            return

        table = Table(title=f"Comparison: {current.scenario_name}", show_header=True)

        table.add_column("Metric", style="cyan")
        table.add_column("Baseline", justify="right")
        table.add_column("Current", justify="right")
        table.add_column("Change", justify="right")

        # Helper to calculate and format change
        def format_change(
            current_val: float, baseline_val: float, lower_is_better: bool = False
        ) -> str:
            if baseline_val == 0:
                return "-"

            change_pct = ((current_val - baseline_val) / baseline_val) * 100.0

            if lower_is_better:
                # For latency, lower is better
                if change_pct <= -10:
                    color = "green"
                    symbol = "↓"
                elif change_pct >= 10:
                    color = "red"
                    symbol = "↑"
                else:
                    color = "yellow"
                    symbol = "~"
            else:
                # For throughput, higher is better
                if change_pct >= 10:
                    color = "green"
                    symbol = "↑"
                elif change_pct <= -10:
                    color = "red"
                    symbol = "↓"
                else:
                    color = "yellow"
                    symbol = "~"

            return f"[{color}]{symbol} {abs(change_pct):.1f}%[/{color}]"

        # Throughput
        table.add_row(
            "Throughput (req/s)",
            f"{baseline.throughput.requests_per_second:,.0f}",
            f"{current.throughput.requests_per_second:,.0f}",
            format_change(
                current.throughput.requests_per_second,
                baseline.throughput.requests_per_second,
                lower_is_better=False,
            ),
        )

        # Latency metrics
        table.add_row(
            "P50 Latency (ms)",
            f"{baseline.latency.median_ms:.2f}",
            f"{current.latency.median_ms:.2f}",
            format_change(
                current.latency.median_ms,
                baseline.latency.median_ms,
                lower_is_better=True,
            ),
        )

        table.add_row(
            "P95 Latency (ms)",
            f"{baseline.latency.p95_ms:.2f}",
            f"{current.latency.p95_ms:.2f}",
            format_change(
                current.latency.p95_ms,
                baseline.latency.p95_ms,
                lower_is_better=True,
            ),
        )

        table.add_row(
            "P99 Latency (ms)",
            f"{baseline.latency.p99_ms:.2f}",
            f"{current.latency.p99_ms:.2f}",
            format_change(
                current.latency.p99_ms,
                baseline.latency.p99_ms,
                lower_is_better=True,
            ),
        )

        self.console.print(table)
        self.console.print()

    def show_profiling_results(self, result: BenchmarkResult) -> None:
        """
        Display profiling results if available.

        Args:
            result: Benchmark result with profiling data
        """
        if not result.profile and not result.memory:
            return

        self.console.print(f"[bold]Profiling Results: {result.scenario_name}[/bold]")
        self.console.print()

        # CPU Profiling
        if result.profile:
            table = Table(title="Top CPU Hotspots", show_header=True)
            table.add_column("Function", style="cyan")
            table.add_column("Time (s)", justify="right", style="green")
            table.add_column("%", justify="right")

            for func_name, cumtime, pct in result.profile.get_top_n(10):
                table.add_row(
                    func_name[:60],  # Truncate long names
                    f"{cumtime:.3f}",
                    f"{pct:.1f}%",
                )

            self.console.print(table)
            self.console.print()

        # Memory Profiling
        if result.memory:
            table = Table(title="Memory Usage", show_header=True)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", justify="right")

            table.add_row("Peak Memory", f"{result.memory.peak_memory_mb:.2f} MB")
            table.add_row(
                "Memory Increase", f"{result.memory.memory_increase_mb:.2f} MB"
            )
            table.add_row("Allocations", f"{result.memory.allocations_count:,}")

            if result.memory.scheduler_queue_sizes:
                for queue_name, size in result.memory.scheduler_queue_sizes.items():
                    style = "red" if size > 0 else "green"
                    table.add_row(
                        f"Scheduler {queue_name}",
                        f"[{style}]{size}[/{style}]",
                    )

            self.console.print(table)
            self.console.print()

            if result.memory.has_memory_leak:
                self.console.print(
                    "[red bold]⚠ Warning: Potential memory leak detected![/red bold]"
                )
                self.console.print()

    def print_error(self, message: str) -> None:
        """
        Print error message.

        Args:
            message: Error message
        """
        self.console.print(f"[red bold]Error:[/red bold] {message}")

    def print_warning(self, message: str) -> None:
        """
        Print warning message.

        Args:
            message: Warning message
        """
        self.console.print(f"[yellow bold]Warning:[/yellow bold] {message}")

    def print_info(self, message: str) -> None:
        """
        Print info message.

        Args:
            message: Info message
        """
        self.console.print(f"[blue]ℹ[/blue] {message}")
