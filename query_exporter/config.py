"""Configuration management functions."""

from collections import defaultdict
import os
import re
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Mapping,
    NamedTuple,
)

import jsonschema
from prometheus_aioexporter import MetricConfig
import yaml

from . import PACKAGE
from .db import (
    AUTOMATIC_LABELS,
    DataBase,
    DATABASE_LABEL,
    InvalidQueryParameters,
    Query,
    QueryMetric,
)

# metric for counting database errors
DB_ERRORS_METRIC_NAME = "database_errors"
DB_ERRORS_METRIC = MetricConfig(
    DB_ERRORS_METRIC_NAME,
    "Number of database errors",
    "counter",
    {"labels": [DATABASE_LABEL]},
)
# metric for counting performed queries
QUERIES_METRIC_NAME = "queries"
QUERIES_METRIC = MetricConfig(
    QUERIES_METRIC_NAME,
    "Number of database queries",
    "counter",
    {"labels": [DATABASE_LABEL, "status"]},
)

# regexp for validating environment variables names
_ENV_VAR_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*$")


class ConfigError(Exception):
    """Configuration is invalid."""


class Config(NamedTuple):
    """Top-level configuration."""

    databases: List[DataBase]
    metrics: List[MetricConfig]
    queries: List[Query]


# Type matching os.environ.
Environ = Mapping[str, str]


def load_config(config_fd, env: Environ = os.environ) -> Config:
    """Load YAML config from file."""
    config = defaultdict(dict, yaml.safe_load(config_fd))
    _validate_config(config)
    databases = _get_databases(config["databases"], env)
    metrics = _get_metrics(config["metrics"])
    database_names = frozenset(database.name for database in databases)
    queries = _get_queries(config["queries"], database_names, metrics)
    return Config(databases, metrics, queries)


def _get_databases(configs: Dict[str, Dict[str, Any]], env: Environ) -> List[DataBase]:
    """Return a dict mapping names to DataBases names."""
    databases = []
    for name, config in configs.items():
        try:
            db = DataBase(
                name,
                _resolve_dsn(config["dsn"], env),
                keep_connected=bool(config.get("keep-connected", True)),
            )
            databases.append(db)
        except Exception as e:
            raise ConfigError(str(e))
    return databases


def _get_metrics(metrics: Dict[str, Dict[str, Any]]) -> List[MetricConfig]:
    """Return metrics configuration."""
    # add global metrics
    configs = [DB_ERRORS_METRIC, QUERIES_METRIC]
    for name, config in metrics.items():
        _validate_metric_config(name, config)
        metric_type = config.pop("type")
        config.setdefault("labels", []).extend(AUTOMATIC_LABELS)
        config["labels"].sort()
        description = config.pop("description", "")
        configs.append(MetricConfig(name, description, metric_type, config))
    return configs


def _validate_metric_config(name: str, config: Dict[str, Any]):
    """Validate a metric configuration stanza."""
    labels = set(config.get("labels", ()))
    overlap_labels = labels & AUTOMATIC_LABELS
    if overlap_labels:
        overlap_list = ", ".join(sorted(overlap_labels))
        raise ConfigError(
            f'Reserved labels declared for metric "{name}": {overlap_list}'
        )


def _get_queries(
    configs: Dict[str, Dict[str, Any]],
    database_names: FrozenSet[str],
    metrics: List[MetricConfig],
) -> List[Query]:
    """Return a list of Queries from config."""
    all_metrics = {metric.name: metric for metric in metrics}
    metric_names = frozenset(all_metrics)
    queries: List[Query] = []
    for name, config in configs.items():
        _validate_query_config(name, config, database_names, metric_names)
        _convert_query_interval(name, config)
        query_metrics = _get_query_metrics(config, all_metrics)
        parameters = config.get("parameters")
        try:
            if parameters:
                queries.extend(
                    Query(
                        f"{name}[params{index}]",
                        config["interval"],
                        config["databases"],
                        query_metrics,
                        config["sql"].strip(),
                        parameters=params,
                    )
                    for index, params in enumerate(parameters)
                )
            else:
                queries.append(
                    Query(
                        name,
                        config["interval"],
                        config["databases"],
                        query_metrics,
                        config["sql"].strip(),
                    )
                )
        except InvalidQueryParameters as e:
            raise ConfigError(str(e))
    return queries


def _get_query_metrics(
    config: Dict[str, Any], metrics: Dict[str, MetricConfig]
) -> List[QueryMetric]:
    """Return QueryMetrics for a query."""

    def _metric_labels(labels: List[str]) -> List[str]:
        return sorted(set(labels) - AUTOMATIC_LABELS)

    return [
        QueryMetric(name, _metric_labels(metrics[name].config["labels"]))
        for name in config["metrics"]
    ]


def _validate_query_config(
    name: str,
    config: Dict[str, Any],
    database_names: FrozenSet[str],
    metric_names: FrozenSet[str],
):
    """Validate a query configuration stanza."""
    unknown_databases = set(config["databases"]) - database_names
    if unknown_databases:
        unknown_list = ", ".join(sorted(unknown_databases))
        raise ConfigError(f'Unknown databases for query "{name}": {unknown_list}')
    unknown_metrics = set(config["metrics"]) - metric_names
    if unknown_metrics:
        unknown_list = ", ".join(sorted(unknown_metrics))
        raise ConfigError(f'Unknown metrics for query "{name}": {unknown_list}')
    parameters = config.get("parameters")
    if parameters:
        keys = {frozenset(param.keys()) for param in parameters}
        if len(keys) > 1:
            raise ConfigError(
                f'Invalid parameters definition for query "{name}": '
                "parameters dictionaries must all have the same keys"
            )


def _convert_query_interval(name: str, config: Dict[str, Any]):
    """Convert query intervals to seconds."""
    interval = config.setdefault("interval", None)
    if interval is None:
        # the query should be run at every request
        return

    multiplier = 1
    if isinstance(interval, str):
        # convert to seconds
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 3600 * 24}
        suffix = interval[-1]
        if suffix in multipliers:
            interval = interval[:-1]
            multiplier = multipliers[suffix]

    config["interval"] = int(interval) * multiplier


def _resolve_dsn(dsn: str, env: Environ) -> str:
    if dsn.startswith("env:"):
        _, varname = dsn.split(":", 1)
        if not _ENV_VAR_RE.match(varname):
            raise ValueError(f'Invalid variable name: "{varname}"')
        if varname not in env:
            raise ValueError(f'Undefined variable: "{varname}"')
        dsn = env[varname]

    return dsn


def _validate_config(config: Dict[str, Any]):
    schema_path = PACKAGE.get_resource_filename(  # type: ignore
        None, "query_exporter/schemas/config.yaml"
    )
    with open(schema_path) as fd:
        schema = yaml.safe_load(fd)
    try:
        jsonschema.validate(config, schema)
    except jsonschema.ValidationError as e:
        path = "/".join(str(item) for item in e.absolute_path)
        raise ConfigError(f"Invalid config at {path}: {e.message}")
