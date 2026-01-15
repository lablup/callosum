"""
Core metrics data structures for benchmark results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class RequestMetric:
    """Single request measurement."""

    timestamp: float
    latency_ms: float
    payload_size_bytes: int
    success: bool
    error: Optional[str] = None


@dataclass
class ThroughputMetric:
    """Throughput measurement."""

    requests_per_second: float
    bytes_per_second: float
    duration_seconds: float
    total_requests: int
    failed_requests: int

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 0.0
        return (
            (self.total_requests - self.failed_requests)
            / self.total_requests
            * 100.0
        )


@dataclass
class LatencyMetric:
    """Latency statistics."""

    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    p999_ms: float
    stddev_ms: float


@dataclass
class MemoryMetric:
    """Memory usage statistics."""

    peak_memory_mb: float
    memory_increase_mb: float
    allocations_count: int
    scheduler_queue_sizes: Dict[str, int]

    @property
    def has_memory_leak(self) -> bool:
        """Check if there are potential memory leaks based on scheduler queues."""
        return any(size > 0 for size in self.scheduler_queue_sizes.values())


@dataclass
class ProfileMetric:
    """CPU profiling statistics."""

    top_functions: List[Tuple[str, float]]  # (function_name, cumulative_time_sec)
    total_time_seconds: float

    def get_top_n(self, n: int = 10) -> List[Tuple[str, float, float]]:
        """
        Get top N functions by cumulative time.

        Returns:
            List of (function_name, cumulative_time_sec, percentage)
        """
        result = []
        for func_name, cumtime in self.top_functions[:n]:
            percentage = (
                (cumtime / self.total_time_seconds * 100.0)
                if self.total_time_seconds > 0
                else 0.0
            )
            result.append((func_name, cumtime, percentage))
        return result


@dataclass
class BenchmarkResult:
    """Complete benchmark result."""

    scenario_name: str
    config: Dict[str, Any]
    throughput: ThroughputMetric
    latency: LatencyMetric
    memory: Optional[MemoryMetric] = None
    profile: Optional[ProfileMetric] = None
    raw_metrics: List[RequestMetric] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "scenario_name": self.scenario_name,
            "config": self.config,
            "throughput": {
                "requests_per_second": self.throughput.requests_per_second,
                "bytes_per_second": self.throughput.bytes_per_second,
                "duration_seconds": self.throughput.duration_seconds,
                "total_requests": self.throughput.total_requests,
                "failed_requests": self.throughput.failed_requests,
                "success_rate": self.throughput.success_rate,
            },
            "latency": {
                "min_ms": self.latency.min_ms,
                "max_ms": self.latency.max_ms,
                "mean_ms": self.latency.mean_ms,
                "median_ms": self.latency.median_ms,
                "p95_ms": self.latency.p95_ms,
                "p99_ms": self.latency.p99_ms,
                "p999_ms": self.latency.p999_ms,
                "stddev_ms": self.latency.stddev_ms,
            },
            "memory": (
                {
                    "peak_memory_mb": self.memory.peak_memory_mb,
                    "memory_increase_mb": self.memory.memory_increase_mb,
                    "allocations_count": self.memory.allocations_count,
                    "scheduler_queue_sizes": self.memory.scheduler_queue_sizes,
                    "has_memory_leak": self.memory.has_memory_leak,
                }
                if self.memory
                else None
            ),
            "profile": (
                {
                    "top_functions": self.profile.top_functions,
                    "total_time_seconds": self.profile.total_time_seconds,
                    "top_10": self.profile.get_top_n(10),
                }
                if self.profile
                else None
            ),
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }
