"""
Statistical analysis utilities for benchmark results.
"""

import math
from typing import Dict, List, Tuple

import numpy as np

from benchmarks.core.metrics import LatencyMetric, RequestMetric, ThroughputMetric


def calculate_percentiles(
    values: List[float], percentiles: List[float] = [50, 95, 99, 99.9]
) -> Dict[float, float]:
    """
    Calculate percentiles using numpy.

    Args:
        values: List of values to analyze
        percentiles: List of percentile values (0-100)

    Returns:
        Dictionary mapping percentile to value
    """
    if not values:
        return {p: 0.0 for p in percentiles}

    arr = np.array(values)
    result = {}
    for p in percentiles:
        result[p] = float(np.percentile(arr, p))
    return result


def calculate_latency_metrics(metrics: List[RequestMetric]) -> LatencyMetric:
    """
    Calculate latency statistics from request metrics.

    Args:
        metrics: List of request metrics

    Returns:
        LatencyMetric with calculated statistics
    """
    if not metrics:
        return LatencyMetric(
            min_ms=0.0,
            max_ms=0.0,
            mean_ms=0.0,
            median_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            p999_ms=0.0,
            stddev_ms=0.0,
        )

    latencies = [m.latency_ms for m in metrics if m.success]

    if not latencies:
        return LatencyMetric(
            min_ms=0.0,
            max_ms=0.0,
            mean_ms=0.0,
            median_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            p999_ms=0.0,
            stddev_ms=0.0,
        )

    arr = np.array(latencies)
    percentiles = calculate_percentiles(latencies, [50, 95, 99, 99.9])

    return LatencyMetric(
        min_ms=float(np.min(arr)),
        max_ms=float(np.max(arr)),
        mean_ms=float(np.mean(arr)),
        median_ms=percentiles[50],
        p95_ms=percentiles[95],
        p99_ms=percentiles[99],
        p999_ms=percentiles[99.9],
        stddev_ms=float(np.std(arr)),
    )


def calculate_throughput(
    metrics: List[RequestMetric], duration: float
) -> ThroughputMetric:
    """
    Calculate throughput from request metrics.

    Args:
        metrics: List of request metrics
        duration: Total duration in seconds

    Returns:
        ThroughputMetric with calculated statistics
    """
    if duration <= 0:
        duration = 0.001  # Avoid division by zero

    total_requests = len(metrics)
    failed_requests = sum(1 for m in metrics if not m.success)
    total_bytes = sum(m.payload_size_bytes for m in metrics if m.success)

    requests_per_second = total_requests / duration
    bytes_per_second = total_bytes / duration

    return ThroughputMetric(
        requests_per_second=requests_per_second,
        bytes_per_second=bytes_per_second,
        duration_seconds=duration,
        total_requests=total_requests,
        failed_requests=failed_requests,
    )


def detect_outliers(
    values: List[float], threshold: float = 3.0
) -> Tuple[List[int], List[float]]:
    """
    Identify outlier indices using standard deviation method.

    Args:
        values: List of values to analyze
        threshold: Number of standard deviations for outlier detection

    Returns:
        Tuple of (outlier_indices, outlier_values)
    """
    if len(values) < 3:
        return [], []

    arr = np.array(values)
    mean = np.mean(arr)
    std = np.std(arr)

    if std == 0:
        return [], []

    z_scores = np.abs((arr - mean) / std)
    outlier_mask = z_scores > threshold

    outlier_indices = np.where(outlier_mask)[0].tolist()
    outlier_values = arr[outlier_mask].tolist()

    return outlier_indices, outlier_values


def calculate_confidence_interval(
    values: List[float], confidence: float = 0.95
) -> Tuple[float, float]:
    """
    Calculate confidence interval for mean.

    Args:
        values: List of values
        confidence: Confidence level (0-1)

    Returns:
        Tuple of (lower_bound, upper_bound)
    """
    if len(values) < 2:
        return (0.0, 0.0)

    try:
        from scipy import stats

        arr = np.array(values)
        mean = np.mean(arr)
        std_err = np.std(arr, ddof=1) / math.sqrt(len(arr))

        # Using t-distribution for small samples
        t_val = stats.t.ppf((1 + confidence) / 2, len(arr) - 1)
        margin = t_val * std_err

        return (float(mean - margin), float(mean + margin))
    except ImportError:
        # Fallback if scipy is not available
        arr = np.array(values)
        mean = np.mean(arr)
        # Use 1.96 for 95% confidence (z-score approximation)
        margin = 1.96 * np.std(arr, ddof=1) / math.sqrt(len(arr))
        return (float(mean - margin), float(mean + margin))


def aggregate_metrics(
    metric_lists: List[List[RequestMetric]],
) -> List[RequestMetric]:
    """
    Aggregate multiple runs of request metrics.

    Args:
        metric_lists: List of metric lists from multiple runs

    Returns:
        Flattened list of all metrics
    """
    aggregated = []
    for metrics in metric_lists:
        aggregated.extend(metrics)
    return aggregated


def calculate_regression(current_value: float, baseline_value: float) -> float:
    """
    Calculate performance regression as percentage.

    Args:
        current_value: Current benchmark value
        baseline_value: Baseline benchmark value

    Returns:
        Regression percentage (negative = improvement, positive = regression)
    """
    if baseline_value == 0:
        return 0.0

    return ((current_value - baseline_value) / baseline_value) * 100.0
