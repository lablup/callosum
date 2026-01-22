"""
Benchmark configuration data structures.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ThroughputConfig:
    """Configuration for throughput benchmarks."""

    payload_sizes: List[int] = field(
        default_factory=lambda: [64, 256, 1024, 4096, 16384, 65536, 262144, 1048576]
    )
    client_counts: List[int] = field(
        default_factory=lambda: [1, 2, 5, 10, 20, 50, 100]
    )
    requests_per_test: int = 10000
    warmup_requests: int = 1000


@dataclass
class LatencyConfig:
    """Configuration for latency benchmarks."""

    target_loads: List[int] = field(
        default_factory=lambda: [100, 500, 1000, 2000, 5000]
    )  # requests/sec
    duration_seconds: int = 30
    payload_size: int = 1024
    payload_sizes_test: List[int] = field(
        default_factory=lambda: [64, 256, 1024, 4096, 16384, 65536]
    )


@dataclass
class FeatureConfig:
    """Configuration for feature overhead benchmarks."""

    compression_payloads: List[int] = field(
        default_factory=lambda: [1024, 10240, 102400, 1048576]
    )
    requests_per_test: int = 5000


@dataclass
class ServerConfig:
    """Configuration for benchmark server."""

    bind_address: str = "tcp://127.0.0.1:*"
    scheduler_types: List[str] = field(
        default_factory=lambda: ["exit-ordered", "key-serialized"]
    )


@dataclass
class ProfilingConfig:
    """Configuration for profiling."""

    enabled: bool = True
    profile_cpu: bool = True
    profile_memory: bool = True
    max_frames: int = 10


@dataclass
class BenchmarkConfig:
    """Complete benchmark configuration."""

    throughput: ThroughputConfig = field(default_factory=ThroughputConfig)
    latency: LatencyConfig = field(default_factory=LatencyConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    profiling: ProfilingConfig = field(default_factory=ProfilingConfig)

    # Global settings
    iterations: int = 3  # Number of times to run each benchmark
    warmup_iterations: int = 1  # Number of warmup iterations

    @classmethod
    def quick(cls) -> "BenchmarkConfig":
        """
        Create a quick test configuration with reduced parameters.

        Returns:
            BenchmarkConfig optimized for quick testing
        """
        config = cls()
        config.iterations = 1
        config.warmup_iterations = 0

        # Reduce test sizes
        config.throughput.payload_sizes = [1024, 4096]
        config.throughput.client_counts = [1, 5, 10]
        config.throughput.requests_per_test = 1000
        config.throughput.warmup_requests = 100

        config.latency.target_loads = [100, 500]
        config.latency.duration_seconds = 10
        config.latency.payload_sizes_test = [1024, 4096]

        config.features.compression_payloads = [1024, 10240]
        config.features.requests_per_test = 1000

        return config
