"""
CPU and memory profiling integration for benchmarks.
"""

import cProfile
import pstats
import tracemalloc
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from benchmarks.core.metrics import MemoryMetric, ProfileMetric


class CProfileProfiler:
    """
    Function-level CPU profiling using cProfile.
    """

    def __init__(self, filter_callosum_only: bool = True):
        """
        Initialize CPU profiler.

        Args:
            filter_callosum_only: If True, focus on callosum.* modules
        """
        self.filter_callosum_only = filter_callosum_only
        self.profiler: Optional[cProfile.Profile] = None
        self.stats: Optional[pstats.Stats] = None

    def start_profiling(self) -> None:
        """Start CPU profiling."""
        self.profiler = cProfile.Profile()
        self.profiler.enable()

    def stop_profiling(self) -> None:
        """Stop CPU profiling and collect stats."""
        if self.profiler:
            self.profiler.disable()
            # Create stats from profiler
            string_io = StringIO()
            self.stats = pstats.Stats(self.profiler, stream=string_io)
            self.stats.strip_dirs()

    def get_top_functions(self, n: int = 20) -> List[Tuple[str, float]]:
        """
        Get top N functions by cumulative time.

        Args:
            n: Number of top functions to return

        Returns:
            List of (function_name, cumulative_time_seconds)
        """
        if not self.stats:
            return []

        # Sort by cumulative time
        self.stats.sort_stats("cumulative")

        # Extract function stats
        result = []
        # Access internal stats dict (not in public API)
        for func, (cc, nc, tt, ct, callers) in list(self.stats.stats.items())[:n]:  # type: ignore[attr-defined]
            filename, line, func_name = func
            full_name = f"{Path(filename).name}:{line}({func_name})"

            # Filter if requested
            if self.filter_callosum_only and "callosum" not in filename:
                continue

            result.append((full_name, ct))

        return result[:n]

    def get_total_time(self) -> float:
        """
        Get total profiled time.

        Returns:
            Total time in seconds
        """
        if not self.stats:
            return 0.0

        return self.stats.total_tt  # type: ignore[attr-defined]

    def save_stats(self, filepath: Path) -> None:
        """
        Save profiling stats to file.

        Args:
            filepath: Path to save stats
        """
        if self.profiler:
            self.profiler.dump_stats(str(filepath))

    def get_profile_metric(self) -> ProfileMetric:
        """
        Get ProfileMetric from collected stats.

        Returns:
            ProfileMetric with top functions and total time
        """
        return ProfileMetric(
            top_functions=self.get_top_functions(20),
            total_time_seconds=self.get_total_time(),
        )


class MemoryProfiler:
    """
    Memory allocation tracking using tracemalloc.

    Follows the pattern from examples/simple-server.py.
    """

    def __init__(self, max_frames: int = 10):
        """
        Initialize memory profiler.

        Args:
            max_frames: Maximum number of stack frames to capture
        """
        self.max_frames = max_frames
        self.start_snapshot: Optional[tracemalloc.Snapshot] = None
        self.end_snapshot: Optional[tracemalloc.Snapshot] = None
        self.peak_memory: float = 0.0

    def start_tracking(self) -> None:
        """Start memory tracking."""
        tracemalloc.start(self.max_frames)
        # Take initial snapshot
        self.start_snapshot = self._take_filtered_snapshot()
        self.peak_memory = 0.0

    def _take_filtered_snapshot(self) -> tracemalloc.Snapshot:
        """
        Take a filtered snapshot excluding system modules.

        Returns:
            Filtered snapshot
        """
        snapshot = tracemalloc.take_snapshot()
        # Filter out system modules (following simple-server.py pattern)
        return snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, tracemalloc.__file__),
        ))

    def take_snapshot(self) -> tracemalloc.Snapshot:
        """
        Take a memory snapshot.

        Returns:
            Filtered snapshot
        """
        snapshot = self._take_filtered_snapshot()

        # Update peak memory
        current, peak = tracemalloc.get_traced_memory()
        self.peak_memory = max(self.peak_memory, peak / (1024**2))  # Convert to MB

        return snapshot

    def stop_tracking(self) -> None:
        """Stop memory tracking and take final snapshot."""
        self.end_snapshot = self._take_filtered_snapshot()
        tracemalloc.stop()

    def get_memory_increase(self) -> float:
        """
        Calculate memory increase from start to end.

        Returns:
            Memory increase in MB
        """
        if not self.start_snapshot or not self.end_snapshot:
            return 0.0

        # Calculate total allocated memory
        start_total = sum(
            stat.size for stat in self.start_snapshot.statistics("lineno")
        )
        end_total = sum(stat.size for stat in self.end_snapshot.statistics("lineno"))

        increase_bytes = end_total - start_total
        return increase_bytes / (1024**2)  # Convert to MB

    def get_top_allocations(self, n: int = 10) -> List[Tuple[str, int, int]]:
        """
        Get top N memory allocations.

        Args:
            n: Number of top allocations to return

        Returns:
            List of (location, size_kb, count)
        """
        if not self.end_snapshot:
            return []

        top_stats = self.end_snapshot.statistics("lineno")[:n]

        result = []
        for stat in top_stats:
            location = f"{stat.traceback}"
            size_kb = stat.size / 1024
            count = stat.count
            result.append((location, int(size_kb), count))

        return result

    def get_memory_diff(self, n: int = 10) -> List[Tuple[str, int]]:
        """
        Get top memory differences between start and end.

        Args:
            n: Number of top differences to return

        Returns:
            List of (location, size_diff_kb)
        """
        if not self.start_snapshot or not self.end_snapshot:
            return []

        top_stats = self.end_snapshot.compare_to(self.start_snapshot, "lineno")[:n]

        result = []
        for stat in top_stats:
            location = str(stat.traceback)
            size_diff_kb = stat.size_diff / 1024
            result.append((location, int(size_diff_kb)))

        return result

    def get_allocation_count(self) -> int:
        """
        Get total number of allocations.

        Returns:
            Number of allocations
        """
        if not self.end_snapshot:
            return 0

        return sum(stat.count for stat in self.end_snapshot.statistics("lineno"))

    def check_scheduler_queues(
        self, scheduler_queue_sizes: Dict[str, int]
    ) -> MemoryMetric:
        """
        Create MemoryMetric with scheduler queue information.

        Args:
            scheduler_queue_sizes: Dictionary of queue names to sizes

        Returns:
            MemoryMetric with all collected information
        """
        return MemoryMetric(
            peak_memory_mb=self.peak_memory,
            memory_increase_mb=self.get_memory_increase(),
            allocations_count=self.get_allocation_count(),
            scheduler_queue_sizes=scheduler_queue_sizes,
        )


class BenchmarkProfiler:
    """
    Combined CPU and memory profiler with unified interface.
    """

    def __init__(
        self,
        profile_cpu: bool = True,
        profile_memory: bool = True,
        filter_callosum_only: bool = True,
    ):
        """
        Initialize benchmark profiler.

        Args:
            profile_cpu: Enable CPU profiling
            profile_memory: Enable memory profiling
            filter_callosum_only: Focus on callosum modules only
        """
        self.profile_cpu = profile_cpu
        self.profile_memory = profile_memory

        self.cpu_profiler = (
            CProfileProfiler(filter_callosum_only=filter_callosum_only)
            if profile_cpu
            else None
        )
        self.memory_profiler = MemoryProfiler() if profile_memory else None

    async def __aenter__(self) -> "BenchmarkProfiler":
        """Start profiling as async context manager."""
        if self.cpu_profiler:
            self.cpu_profiler.start_profiling()
        if self.memory_profiler:
            self.memory_profiler.start_tracking()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop profiling."""
        if self.cpu_profiler:
            self.cpu_profiler.stop_profiling()
        if self.memory_profiler:
            self.memory_profiler.stop_tracking()

    def get_results(
        self, scheduler_queue_sizes: Optional[Dict[str, int]] = None
    ) -> Tuple[Optional[ProfileMetric], Optional[MemoryMetric]]:
        """
        Get profiling results.

        Args:
            scheduler_queue_sizes: Optional scheduler queue sizes for memory metric

        Returns:
            Tuple of (ProfileMetric, MemoryMetric)
        """
        profile_metric = None
        memory_metric = None

        if self.cpu_profiler:
            profile_metric = self.cpu_profiler.get_profile_metric()

        if self.memory_profiler:
            queue_sizes = scheduler_queue_sizes or {}
            memory_metric = self.memory_profiler.check_scheduler_queues(queue_sizes)

        return profile_metric, memory_metric
