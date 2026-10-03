"""
CLI interface for Callosum benchmark suite.
"""

import asyncio
import signal
from pathlib import Path

import click

from benchmarks.core.config import BenchmarkConfig
from benchmarks.core.runner import BenchmarkInterrupted, BenchmarkRunner
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

    # Setup signal handler for graceful shutdown
    def signal_handler(signum, frame):
        console.print_warning(
            "Received interrupt signal, stopping after current scenario..."
        )
        runner.interrupt()

    original_sigint = signal.signal(signal.SIGINT, signal_handler)

    try:
        asyncio.run(runner.run_all(scenario_filter=scenario_filter))

        # Save results
        runner.save_results(output_dir=output_dir, format=format)

        console.console.print()
        if runner.get_failed_scenarios():
            console.console.print(
                f"[yellow]⚠[/yellow] Benchmarks completed with "
                f"{len(runner.get_failed_scenarios())} failed scenario(s)."
            )
        else:
            console.console.print(
                "[green]✓[/green] Benchmarks completed successfully!"
            )
        console.console.print(f"Results saved to: {output_dir}")

    except (KeyboardInterrupt, BenchmarkInterrupted):
        console.console.print()
        console.print_warning("Benchmark interrupted by user")

        # Show and save partial results
        if runner.get_partial_results():
            console.console.print()
            runner.show_partial_report(reason="interrupted")

            # Save partial results
            try:
                runner.save_results(output_dir=output_dir, format=format)
                console.console.print()
                console.print_info(f"Partial results saved to: {output_dir}")
            except Exception as save_error:
                console.print_error(f"Failed to save partial results: {save_error}")
        else:
            console.print_info("No results collected before interruption.")

    except Exception as e:
        console.console.print()
        console.print_error(f"Benchmark failed: {e}")

        # Show and save partial results
        if runner.get_partial_results():
            console.console.print()
            runner.show_partial_report(reason="error")

            # Save partial results
            try:
                runner.save_results(output_dir=output_dir, format=format)
                console.console.print()
                console.print_info(f"Partial results saved to: {output_dir}")
            except Exception as save_error:
                console.print_error(f"Failed to save partial results: {save_error}")

        raise

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, original_sigint)


if __name__ == "__main__":
    main()
