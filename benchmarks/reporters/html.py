"""
HTML report generator with Plotly charts.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import plotly.graph_objects as go

from benchmarks.core.metrics import BenchmarkResult


class HTMLReporter:
    """
    Generate comprehensive HTML reports with interactive charts.
    """

    def __init__(self):
        """Initialize HTML reporter."""
        pass

    def generate_report(
        self,
        results: List[BenchmarkResult],
        output_path: Path,
        baseline: Optional[List[BenchmarkResult]] = None,
        title: str = "Callosum RPC Benchmark Results",
    ) -> None:
        """
        Generate complete HTML report.

        Args:
            results: List of benchmark results
            output_path: Path to save HTML file
            baseline: Optional baseline results for comparison
            title: Report title
        """
        # Create HTML content
        html_parts = []

        # Header
        html_parts.append(self._create_header(title))

        # Executive Summary
        html_parts.append(self._create_summary_section(results))

        # Group results by scenario type
        throughput_results = [r for r in results if "throughput" in r.scenario_name]
        latency_results = [r for r in results if "latency" in r.scenario_name]
        feature_results = [
            r
            for r in results
            if any(
                x in r.scenario_name
                for x in ["compression", "authentication", "combined"]
            )
        ]

        # Throughput Section
        if throughput_results:
            html_parts.append(self._create_section_header("Throughput Benchmarks"))
            html_parts.append(self._create_throughput_charts(throughput_results))

        # Latency Section
        if latency_results:
            html_parts.append(self._create_section_header("Latency Benchmarks"))
            html_parts.append(self._create_latency_charts(latency_results))

        # Feature Overhead Section
        if feature_results:
            html_parts.append(self._create_section_header("Feature Overhead"))
            html_parts.append(self._create_feature_charts(feature_results))

        # Profiling Section
        profiled_results = [r for r in results if r.profile or r.memory]
        if profiled_results:
            html_parts.append(self._create_section_header("Profiling Results"))
            html_parts.append(self._create_profiling_section(profiled_results))

        # Raw Data Section
        html_parts.append(self._create_section_header("Raw Data"))
        html_parts.append(self._create_raw_data_section(results))

        # Footer
        html_parts.append(self._create_footer())

        # Write HTML file
        html_content = "\n".join(html_parts)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html_content)

    def _create_header(self, title: str) -> str:
        """Create HTML header."""
        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 40px;
            border-left: 4px solid #3498db;
            padding-left: 15px;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 20px 0;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .metric-box {{
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            color: #2980b9;
        }}
        .metric-label {{
            font-size: 12px;
            color: #7f8c8d;
            text-transform: uppercase;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 20px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            margin: 20px 0;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
        }}
        th {{
            background: #3498db;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #95a5a6;
            border-top: 1px solid #ecf0f1;
            margin-top: 40px;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p style="color: #7f8c8d;">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
"""

    def _create_summary_section(self, results: List[BenchmarkResult]) -> str:
        """Create executive summary section."""
        if not results:
            return ""

        total_scenarios = len(results)
        avg_throughput = sum(
            r.throughput.requests_per_second for r in results
        ) / len(results)
        avg_p99 = sum(r.latency.p99_ms for r in results) / len(results)
        total_requests = sum(r.throughput.total_requests for r in results)

        return f"""
    <div class="summary-card">
        <h2>Executive Summary</h2>
        <div class="summary-grid">
            <div class="metric-box">
                <div class="metric-value">{total_scenarios}</div>
                <div class="metric-label">Scenarios Tested</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{avg_throughput:,.0f}</div>
                <div class="metric-label">Avg Throughput (req/s)</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{avg_p99:.2f}</div>
                <div class="metric-label">Avg P99 Latency (ms)</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{total_requests:,}</div>
                <div class="metric-label">Total Requests</div>
            </div>
        </div>
    </div>
"""

    def _create_section_header(self, title: str) -> str:
        """Create section header."""
        return f"<h2>{title}</h2>"

    def _create_throughput_charts(self, results: List[BenchmarkResult]) -> str:
        """Create throughput visualization charts."""
        # Group by scenario type
        by_payload = [r for r in results if "payload-size" in r.scenario_name]
        by_client = [r for r in results if "client-count" in r.scenario_name]
        by_scheduler = [
            r for r in results if "scheduler-comparison" in r.scenario_name
        ]

        html_parts = []

        # Throughput by payload size
        if by_payload:
            fig = go.Figure()

            # Group by scheduler
            for scheduler in ["exit-ordered", "key-serialized"]:
                scheduler_results = [
                    r for r in by_payload if r.config.get("scheduler") == scheduler
                ]
                if scheduler_results:
                    x_values = [
                        r.config["payload_size"] / 1024 for r in scheduler_results
                    ]  # KB
                    y_values = [
                        r.throughput.requests_per_second for r in scheduler_results
                    ]

                    fig.add_trace(
                        go.Scatter(
                            x=x_values,
                            y=y_values,
                            mode="lines+markers",
                            name=scheduler,
                            line=dict(width=2),
                            marker=dict(size=8),
                        )
                    )

            fig.update_layout(
                title="Throughput by Payload Size",
                xaxis_title="Payload Size (KB)",
                yaxis_title="Throughput (requests/sec)",
                hovermode="x unified",
                height=400,
            )

            html_parts.append(
                '<div class="chart-container"><div id="throughput-payload"></div></div>'
            )
            html_parts.append(
                f'<script>Plotly.newPlot("throughput-payload", {fig.to_json()});</script>'
            )

        # Throughput by client count
        if by_client:
            fig = go.Figure()

            # Group by scheduler
            for scheduler in ["exit-ordered", "key-serialized"]:
                scheduler_results = [
                    r for r in by_client if r.config.get("scheduler") == scheduler
                ]
                if scheduler_results:
                    x_values = [r.config["num_clients"] for r in scheduler_results]
                    y_values = [
                        r.throughput.requests_per_second for r in scheduler_results
                    ]

                    fig.add_trace(
                        go.Scatter(
                            x=x_values,
                            y=y_values,
                            mode="lines+markers",
                            name=scheduler,
                            line=dict(width=2),
                            marker=dict(size=8),
                        )
                    )

            fig.update_layout(
                title="Throughput by Client Count",
                xaxis_title="Number of Concurrent Clients",
                yaxis_title="Throughput (requests/sec)",
                hovermode="x unified",
                height=400,
            )

            html_parts.append(
                '<div class="chart-container"><div id="throughput-clients"></div></div>'
            )
            html_parts.append(
                f'<script>Plotly.newPlot("throughput-clients", {fig.to_json()});</script>'
            )

        # Scheduler comparison
        if by_scheduler:
            schedulers = [r.config["scheduler"] for r in by_scheduler]
            throughputs = [r.throughput.requests_per_second for r in by_scheduler]

            fig = go.Figure(
                data=[
                    go.Bar(
                        x=schedulers,
                        y=throughputs,
                        marker_color=["#3498db", "#e74c3c"],
                    )
                ]
            )

            fig.update_layout(
                title="Scheduler Comparison",
                xaxis_title="Scheduler Type",
                yaxis_title="Throughput (requests/sec)",
                height=400,
            )

            html_parts.append(
                '<div class="chart-container"><div id="scheduler-comparison"></div></div>'
            )
            html_parts.append(
                f'<script>Plotly.newPlot("scheduler-comparison", {fig.to_json()});</script>'
            )

        return "\n".join(html_parts)

    def _create_latency_charts(self, results: List[BenchmarkResult]) -> str:
        """Create latency visualization charts."""
        html_parts = []

        # Latency percentiles box plot
        if results:
            fig = go.Figure()

            for result in results:
                config_str = f"{result.config.get('payload_size', '?')}B"
                if "target_load_rps" in result.config:
                    config_str = f"{result.config['target_load_rps']} rps"

                fig.add_trace(
                    go.Box(
                        name=config_str,
                        y=[
                            result.latency.min_ms,
                            result.latency.median_ms,
                            result.latency.p95_ms,
                            result.latency.p99_ms,
                            result.latency.max_ms,
                        ],
                        boxmean="sd",
                    )
                )

            fig.update_layout(
                title="Latency Distribution",
                yaxis_title="Latency (ms)",
                height=400,
            )

            html_parts.append(
                '<div class="chart-container"><div id="latency-box"></div></div>'
            )
            html_parts.append(
                f'<script>Plotly.newPlot("latency-box", {fig.to_json()});</script>'
            )

        return "\n".join(html_parts)

    def _create_feature_charts(self, results: List[BenchmarkResult]) -> str:
        """Create feature overhead charts."""
        html_parts = []

        if results:
            # Create comparison bar chart
            labels = []
            throughputs = []

            for result in results:
                label_parts = []
                if "compress" in result.config:
                    label_parts.append(
                        "Comp" if result.config["compress"] else "NoComp"
                    )
                if "use_auth" in result.config:
                    label_parts.append(
                        "Auth" if result.config["use_auth"] else "NoAuth"
                    )

                labels.append(
                    " + ".join(label_parts) if label_parts else result.scenario_name
                )
                throughputs.append(result.throughput.requests_per_second)

            fig = go.Figure(
                data=[go.Bar(x=labels, y=throughputs, marker_color="#9b59b6")]
            )

            fig.update_layout(
                title="Feature Overhead Impact on Throughput",
                xaxis_title="Feature Configuration",
                yaxis_title="Throughput (requests/sec)",
                height=400,
            )

            html_parts.append(
                '<div class="chart-container"><div id="feature-overhead"></div></div>'
            )
            html_parts.append(
                f'<script>Plotly.newPlot("feature-overhead", {fig.to_json()});</script>'
            )

        return "\n".join(html_parts)

    def _create_profiling_section(self, results: List[BenchmarkResult]) -> str:
        """Create profiling results section."""
        html_parts = []

        for result in results:
            if result.profile:
                html_parts.append(f"<h3>{result.scenario_name} - CPU Profile</h3>")
                html_parts.append("<table>")
                html_parts.append(
                    "<tr><th>Function</th><th>Time (s)</th><th>%</th></tr>"
                )

                for func_name, cumtime, pct in result.profile.get_top_n(10):
                    html_parts.append(
                        f"<tr><td>{func_name[:80]}</td><td>{cumtime:.3f}</td><td>{pct:.1f}%</td></tr>"
                    )

                html_parts.append("</table>")

            if result.memory:
                html_parts.append(f"<h3>{result.scenario_name} - Memory Usage</h3>")
                html_parts.append("<table>")
                html_parts.append("<tr><th>Metric</th><th>Value</th></tr>")
                html_parts.append(
                    f"<tr><td>Peak Memory</td><td>{result.memory.peak_memory_mb:.2f} MB</td></tr>"
                )
                html_parts.append(
                    f"<tr><td>Memory Increase</td><td>{result.memory.memory_increase_mb:.2f} MB</td></tr>"
                )
                html_parts.append(
                    f"<tr><td>Allocations</td><td>{result.memory.allocations_count:,}</td></tr>"
                )
                html_parts.append("</table>")

        return "\n".join(html_parts)

    def _create_raw_data_section(self, results: List[BenchmarkResult]) -> str:
        """Create raw data section with downloadable JSON."""
        # Convert results to dict
        results_dict = [r.to_dict() for r in results]
        json_data = json.dumps(results_dict, indent=2)

        return f"""
    <div class="summary-card">
        <h3>Raw Data (JSON)</h3>
        <p>Download raw benchmark data for further analysis:</p>
        <textarea readonly style="width: 100%; height: 200px; font-family: monospace; font-size: 12px;">{json_data[:1000]}...
        </textarea>
        <p><small>Full data available in exported JSON file</small></p>
    </div>
"""

    def _create_footer(self) -> str:
        """Create HTML footer."""
        return """
    <div class="footer">
        <p>Generated by Callosum Benchmark Suite</p>
        <p><a href="https://github.com/lablup/callosum">github.com/lablup/callosum</a></p>
    </div>
</body>
</html>
"""
