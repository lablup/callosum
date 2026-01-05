"""
CLI interface for Callosum benchmark suite.
"""

import asyncio
from pathlib import Path

import click

from benchmarks.core.config import BenchmarkConfig
from benchmarks.core.runner import BenchmarkRunner
from benchmarks.reporters.console import ConsoleReporter


@click.command()
@click.option(
    "--scenario",
    type=click.Choice(
        ["all", "throughput", "latency", "features"], case_sensitive=False
    ),
    default="all",
    help="Which scenarios to run",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("benchmark-results"),
    help="Output directory for results",
)
@click.option(
    "--format",
    type=click.Choice(["html", "json", "both"], case_sensitive=False),
    default="both",
    help="Output format",
)
@click.option(
    "--quick",
    is_flag=True,
    help="Run with reduced iterations for quick testing",
)
@click.option(
    "--no-profiling",
    is_flag=True,
    help="Disable CPU and memory profiling",
)
@click.option(
    "--list",
    "list_scenarios",
    is_flag=True,
    help="List available scenarios and exit",
)
def main(
    scenario: str,
    output_dir: Path,
    format: str,
    quick: bool,
    no_profiling: bool,
    list_scenarios: bool,
):
    """
    Callosum RPC Performance Benchmark Suite

    Run comprehensive performance benchmarks for the Callosum RPC library
    with detailed profiling and reporting.
    """
    console = ConsoleReporter()

    # List scenarios
    if list_scenarios:
        console.print_info("Available benchmark scenarios:")
        console.console.print()
        console.console.print("  [cyan]throughput[/cyan] - Throughput benchmarks")
        console.console.print("    • Variable payload sizes")
        console.console.print("    • Variable client counts")
        console.console.print("    • Scheduler comparison")
        console.console.print()
        console.console.print(
            "  [cyan]latency[/cyan] - Latency percentile benchmarks"
        )
        console.console.print("    • Latency under load")
        console.console.print("    • Latency by payload size")
        console.console.print("    • Tail latency analysis")
        console.console.print()
        console.console.print(
            "  [cyan]features[/cyan] - Feature overhead benchmarks"
        )
        console.console.print("    • Compression overhead")
        console.console.print("    • Authentication overhead")
        console.console.print("    • Combined features matrix")
        console.console.print()
        return

    # Create configuration
    if quick:
        config = BenchmarkConfig.quick()
        console.print_info("Using quick test configuration (reduced iterations)")
    else:
        config = BenchmarkConfig()

    # Disable profiling if requested
    if no_profiling:
        config.profiling.enabled = False
        console.print_info("Profiling disabled")

    # Create and run benchmark runner
    runner = BenchmarkRunner(
        config=config,
        console_reporter=console,
        enable_profiling=config.profiling.enabled,
    )

    # Run benchmarks
    scenario_filter = None if scenario == "all" else scenario

    try:
        asyncio.run(runner.run_all(scenario_filter=scenario_filter))

        # Save results
        runner.save_results(output_dir=output_dir, format=format)

        console.console.print()
        console.console.print("[green]✓[/green] Benchmarks completed successfully!")
        console.console.print(f"Results saved to: {output_dir}")

    except KeyboardInterrupt:
        console.print_warning("Benchmark interrupted by user")
    except Exception as e:
        console.print_error(f"Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()
