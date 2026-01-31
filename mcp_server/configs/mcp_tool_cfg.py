from dataclasses import dataclass


@dataclass
class McpToolCfg:
    prometheus_url: str | None = None
    jaeger_base_url: str | None = None
    benchmark_submit_url: str | None = None
