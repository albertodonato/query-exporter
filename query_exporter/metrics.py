from prometheus_aioexporter import MetricConfig

# metric for counting database errors
DB_ERRORS_METRIC_NAME = "database_errors"
_DB_ERRORS_METRIC_CONFIG = MetricConfig(
    name=DB_ERRORS_METRIC_NAME,
    description="Number of database errors",
    type="counter",
    config={"increment": True},
)

# metric for counting performed queries
QUERIES_METRIC_NAME = "queries"
_QUERIES_METRIC_CONFIG = MetricConfig(
    name=QUERIES_METRIC_NAME,
    description="Number of database queries",
    type="counter",
    labels=("query", "status"),
    config={"increment": True},
)
# metric for tracking last query execution timestamp
QUERY_TIMESTAMP_METRIC_NAME = "query_timestamp"
_QUERY_TIMESTAMP_METRIC_CONFIG = MetricConfig(
    name=QUERY_TIMESTAMP_METRIC_NAME,
    description="Query last execution timestamp",
    type="gauge",
    labels=("query",),
)
# metric for counting queries execution latency
QUERY_LATENCY_METRIC_NAME = "query_latency"
_QUERY_LATENCY_METRIC_CONFIG = MetricConfig(
    name=QUERY_LATENCY_METRIC_NAME,
    description="Query execution latency",
    type="histogram",
    labels=("query",),
)
# metrics reporting the query interval
QUERY_INTERVAL_METRIC_NAME = "query_interval"
_QUERY_INTERVAL_METRIC_CONFIG = MetricConfig(
    name=QUERY_INTERVAL_METRIC_NAME,
    description="Query execution interval",
    type="gauge",
    labels=("query",),
)


BUILTIN_METRICS = frozenset(
    (
        DB_ERRORS_METRIC_NAME,
        QUERIES_METRIC_NAME,
        QUERY_INTERVAL_METRIC_NAME,
        QUERY_LATENCY_METRIC_NAME,
        QUERY_TIMESTAMP_METRIC_NAME,
    )
)


def get_builtin_metric_configs(
    extra_labels: frozenset[str],
) -> dict[str, MetricConfig]:
    """Return configuration for builtin metrics."""
    metric_configs = {
        metric_config.name: MetricConfig(
            metric_config.name,
            metric_config.description,
            metric_config.type,
            labels=set(metric_config.labels) | extra_labels,
            config=metric_config.config,
        )
        for metric_config in (
            _DB_ERRORS_METRIC_CONFIG,
            _QUERIES_METRIC_CONFIG,
            _QUERY_LATENCY_METRIC_CONFIG,
            _QUERY_TIMESTAMP_METRIC_CONFIG,
        )
    }
    metric_configs[QUERY_INTERVAL_METRIC_NAME] = _QUERY_INTERVAL_METRIC_CONFIG
    return metric_configs
