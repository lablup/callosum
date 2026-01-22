# Callosum RPC Benchmark Suite

Comprehensive performance benchmark suite for the Callosum RPC library with detailed profiling capabilities.

## Features

- **Throughput Benchmarks**: Measure requests/second with varying payload sizes and client counts
- **Latency Benchmarks**: Analyze latency percentiles (p50, p95, p99) under different load levels
- **Feature Overhead**: Measure performance impact of compression and CURVE authentication
- **Detailed Profiling**: CPU profiling (cProfile) and memory tracking (tracemalloc)
- **Rich Reports**: Interactive HTML reports with Plotly charts + colorful console output

## Installation

Install the benchmark dependencies:

```bash
# Using uv (recommended)
uv pip install -e ".[benchmark,zeromq]"

# Or using pip
pip install -e ".[benchmark,zeromq]"
```

This installs:
- `rich` - Console output with tables and colors
- `plotly` - Interactive charts for HTML reports
- `pandas` - Data processing
- `numpy` - Statistical calculations
- `scipy` - Advanced statistics
- `jinja2` - HTML templating
- `pyzmq` - ZeroMQ transport (required for RPC)

## Quick Start

Run all benchmarks with default settings:

```bash
python -m benchmarks
```

Run quick test (reduced iterations):

```bash
python -m benchmarks --quick
```

Run specific scenario:

```bash
python -m benchmarks --scenario throughput
python -m benchmarks --scenario latency
python -m benchmarks --scenario features
```

List available scenarios:

```bash
python -m benchmarks --list
```

## Command-Line Options

```
Options:
  --scenario [all|throughput|latency|features]
                                  Which scenarios to run (default: all)
  --output-dir PATH              Output directory for results (default: benchmark-results)
  --format [html|json|both]      Output format (default: both)
  --quick                        Run with reduced iterations for quick testing
  --no-profiling                 Disable CPU and memory profiling
  --list                         List available scenarios and exit
  --help                         Show this message and exit
```

## Benchmark Scenarios

### Throughput Benchmarks

**Variable Payload Sizes**
- Tests: 64B, 256B, 1KB, 4KB, 16KB, 64KB, 256KB, 1MB
- Measures: requests/second, MB/second
- Compares: Both schedulers (ExitOrdered vs KeySerialized)

**Variable Client Counts**
- Tests: 1, 2, 5, 10, 20, 50, 100 concurrent clients
- Fixed: 1KB payload
- Measures: Total throughput, scalability

**Scheduler Comparison**
- Compares: ExitOrderedAsyncScheduler vs KeySerializedAsyncScheduler
- Measures: Performance overhead of ordering guarantees

### Latency Benchmarks

**Latency Under Load**
- Tests: Different load levels (100, 500, 1000, 2000, 5000 req/s)
- Measures: p50, p95, p99, p99.9 latencies
- Duration: 30 seconds per load level

**Latency by Payload Size**
- Tests: Various payload sizes
- Measures: Latency distribution for each size
- Fixed: 10 concurrent clients

**Tail Latency Analysis**
- Long-running: 10,000 requests
- Identifies: Jitter and outliers
- Detailed: Time-series latency tracking

### Feature Overhead Benchmarks

**Compression Overhead**
- Compares: Snappy compression on/off
- Payload sizes: 1KB, 10KB, 100KB, 1MB
- Measures: Throughput impact, CPU overhead

**Authentication Overhead**
- Compares: CURVE encryption on/off
- Measures: Handshake latency, throughput impact
- Fixed: 1KB payload, 10 clients

**Combined Features Matrix**
- Tests: All combinations of compression × authentication
- Measures: Combined performance impact

## Profiling

### CPU Profiling (cProfile)

Identifies function-level CPU hotspots:
- Top 20 functions by cumulative time
- Call counts and timing
- Focuses on callosum.* modules

### Memory Profiling (tracemalloc)

Tracks memory allocations:
- Peak memory usage
- Memory increase during benchmark
- Scheduler queue sizes (leak detection)
- Top allocation locations

Disable profiling for faster benchmarks:

```bash
python -m benchmarks --no-profiling
```

## Output

### Console Output

Real-time colorized output with:
- Progress bars during execution
- Results table with key metrics
- Summary statistics
- Profiling results (if enabled)

### HTML Report

Interactive report with:
- Executive summary with key metrics
- Plotly charts (line charts, box plots, bar charts)
- Sortable tables
- Profiling results
- Downloadable raw data (JSON)

### JSON Output

Machine-readable format with:
- All metrics and configuration
- Raw request data
- Profiling results
- Suitable for CI/CD integration

## Example Usage

### Basic Benchmark Run

```bash
# Run all benchmarks with HTML and JSON output
python -m benchmarks

# Results will be in: benchmark-results/
# - benchmark-report-YYYYMMDD-HHMMSS.html
# - benchmark-results-YYYYMMDD-HHMMSS.json
```

### Quick Performance Check

```bash
# Fast iteration for development
python -m benchmarks --quick --scenario throughput
```

### Deep Profiling

```bash
# Run with full profiling enabled (default)
python -m benchmarks --scenario latency --format html
```

### CI/CD Integration

```bash
# JSON-only output for automated testing
python -m benchmarks --quick --format json --output-dir ci-results
```

## Configuration

The default configuration can be found in `benchmarks/core/config.py`. Key parameters:

```python
# Throughput config
payload_sizes = [64, 256, 1024, 4096, 16384, 65536, 262144, 1048576]
client_counts = [1, 2, 5, 10, 20, 50, 100]
requests_per_test = 10000

# Latency config
target_loads = [100, 500, 1000, 2000, 5000]  # req/s
duration_seconds = 30
payload_sizes_test = [64, 256, 1024, 4096, 16384, 65536]

# Feature config
compression_payloads = [1024, 10240, 102400, 1048576]
requests_per_test = 5000

# Global settings
iterations = 3  # Number of runs per benchmark
warmup_iterations = 1
```

## Architecture

```
benchmarks/
├── core/           # Core infrastructure
│   ├── config.py   # Configuration dataclasses
│   ├── metrics.py  # Metric data structures
│   ├── profiler.py # CPU and memory profiling
│   └── runner.py   # Main orchestration
├── scenarios/      # Benchmark scenarios
│   ├── base.py     # Base scenario class
│   ├── throughput.py
│   ├── latency.py
│   └── features.py
├── fixtures/       # Server and client implementations
│   ├── server.py   # Benchmark server
│   └── client.py   # Benchmark client
├── reporters/      # Output formatting
│   ├── console.py  # Rich console output
│   └── html.py     # HTML report generation
├── utils/          # Utilities
│   ├── payload.py     # Payload generators
│   ├── statistics.py  # Statistical functions
│   └── auth_helpers.py # Auth setup
├── cli.py          # Command-line interface
└── __main__.py     # Module entry point
```

## Interpreting Results

### Throughput

- **Higher is better** for requests/second
- Look for: Scalability with client count
- Compare: Scheduler overhead (ExitOrdered vs KeySerialized)

### Latency

- **Lower is better** for all percentiles
- p50 (median): Typical latency
- p95: Most users' experience
- p99: Tail latency, important for SLAs
- p99.9: Extreme outliers

### Memory Leaks

If you see warnings about memory leaks:
- Check scheduler queue sizes (should be 0 after cleanup)
- Review memory increase (should be minimal)
- Profiling shows which code is allocating memory

### Regression Detection

Compare current results with baseline:
- >10% throughput decrease: Warning
- >25% throughput decrease: Critical
- >10% latency increase: Warning
- >25% latency increase: Critical

## Troubleshooting

### "Connection refused" errors

Ensure no other process is using port 5020 or similar ports.

### High memory usage

This is expected during profiling. Disable with `--no-profiling` if needed.

### Slow benchmark execution

Use `--quick` for faster iterations, or run specific scenarios only.

### Dependencies not found

Make sure you installed with benchmark extras:
```bash
pip install -e ".[benchmark,zeromq]"
```

## Contributing

To add a new benchmark scenario:

1. Create a new scenario class in `scenarios/`
2. Inherit from `BaseBenchmarkScenario`
3. Implement the `run()` method
4. Add to the runner in `core/runner.py`
5. Update CLI help text

## License

MIT License - See main project LICENSE file
