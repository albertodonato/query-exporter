from enum import StrEnum
from typing import Any, ClassVar

from prometheus_aioexporter import MetricConfig


class BuiltinMetric(StrEnum):
    """Name for builtin metrics."""

    DATABASE_ERRORS = "database_errors"
    QUERIES = "queries"
    QUERY_INTERVAL = "query_interval"
    QUERY_LATENCY = "query_latency"
    QUERY_TIMESTAMP = "query_timestamp"


class BuiltinMetrics:
    """Definitions for builtin metrics."""

    _METRICS: ClassVar[dict[str, MetricConfig]] = {
        config.name: config
        for config in [
            MetricConfig(
                name=BuiltinMetric.DATABASE_ERRORS,
                description="Number of database errors",
                type="counter",
                config={"increment": True},
            ),
            MetricConfig(
                name=BuiltinMetric.QUERIES,
                description="Number of database queries",
                type="counter",
                labels=("query", "status"),
                config={"increment": True},
            ),
            MetricConfig(
                name=BuiltinMetric.QUERY_INTERVAL,
                description="Query execution interval",
                type="gauge",
                labels=("query",),
            ),
            MetricConfig(
                name=BuiltinMetric.QUERY_LATENCY,
                description="Query execution latency",
                type="histogram",
                labels=("query",),
            ),
            MetricConfig(
                name=BuiltinMetric.QUERY_TIMESTAMP,
                description="Query last execution timestamp",
                type="gauge",
                labels=("query",),
            ),
        ]
    }

    # Metrics that should not have extra (database) labels merged in
    _NO_EXTRA_LABELS = frozenset({BuiltinMetric.QUERY_INTERVAL})

    @classmethod
    def names(cls) -> frozenset[str]:
        """Builtin metrics names."""
        return frozenset(str(m) for m in cls._METRICS)

    @classmethod
    def get_configs(
        cls,
        extra_labels: frozenset[str],
        overrides: dict[str, dict[str, Any]],
    ) -> dict[str, MetricConfig]:
        """Return configurations for builtin metrics with config overrides."""
        return {
            name: MetricConfig(
                metric_config.name,
                metric_config.description,
                metric_config.type,
                labels=(
                    set(metric_config.labels)
                    if name in cls._NO_EXTRA_LABELS
                    else set(metric_config.labels) | extra_labels
                ),
                config=(
                    metric_config.config
                    | overrides.get(metric_config.name, {})
                ),
            )
            for name, metric_config in cls._METRICS.items()
        }
