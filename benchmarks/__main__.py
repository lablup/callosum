"""
Main entry point for running benchmarks as a module.

Usage:
    python -m benchmarks
    python -m benchmarks --scenario throughput
    python -m benchmarks --quick
"""

from benchmarks.cli import main

if __name__ == "__main__":
    main()
