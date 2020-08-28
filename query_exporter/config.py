"""Configuration management functions."""

from collections import defaultdict
from copy import deepcopy
from logging import Logger
import os
import re
from typing import (
    Any,
    Dict,
    FrozenSet,
    IO,
    List,
    Mapping,
    NamedTuple,
    Set,
    Tuple,
)

import jsonschema
from prometheus_aioexporter import MetricConfig
import yaml

from . import PACKAGE
from .db import (
    DataBase,
    DATABASE_LABEL,
    InvalidQueryParameters,
    InvalidQuerySchedule,
    Query,
    QueryMetric,
)

# metric for counting database errors
DB_ERRORS_METRIC_NAME = "database_errors"
_DB_ERRORS_METRIC_CONFIG = MetricConfig(
    DB_ERRORS_METRIC_NAME,
    "Number of database errors",
    "counter",
    {"labels": []},
)
# metric for counting performed queries
QUERIES_METRIC_NAME = "queries"
_QUERIES_METRIC_CONFIG = MetricConfig(
    QUERIES_METRIC_NAME,
    "Number of database queries",
    "counter",
    {"labels": ["query", "status"]},
)
# metric for counting queries execution latency
QUERY_LATENCY_METRIC_NAME = "query_latency"
_QUERY_LATENCY_METRIC_CONFIG = MetricConfig(
    QUERY_LATENCY_METRIC_NAME,
    "Query execution latency",
    "histogram",
    {"labels": ["query"]},
)
GLOBAL_METRICS = frozenset(
    [DB_ERRORS_METRIC_NAME, QUERIES_METRIC_NAME, QUERY_LATENCY_METRIC_NAME]
)

# regexp for validating environment variables names
_ENV_VAR_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*$")


class ConfigError(Exception):
    """Configuration is invalid."""


class Config(NamedTuple):
    """Top-level configuration."""

    databases: Dict[str, DataBase]
    metrics: Dict[str, MetricConfig]
    queries: Dict[str, Query]


# Type matching os.environ.
Environ = Mapping[str, str]


def load_config(config_fd: IO, logger: Logger, env: Environ = os.environ) -> Config:
    """Load YAML config from file."""
    data = defaultdict(dict, yaml.safe_load(config_fd))
    _validate_config(data)
    databases, database_labels = _get_databases(data["databases"], env)
    extra_labels = frozenset([DATABASE_LABEL]) | database_labels
    metrics = _get_metrics(data["metrics"], extra_labels)
    queries = _get_queries(data["queries"], frozenset(databases), metrics, extra_labels)
    config = Config(databases, metrics, queries)
    _warn_if_unused(config, logger)
    return config


def _get_databases(
    configs: Dict[str, Dict[str, Any]], env: Environ
) -> Tuple[Dict[str, DataBase], FrozenSet[str]]:
    """Return a dict mapping names to DataBases."""
    databases = {}
    all_db_labels: Set[FrozenSet[str]] = set()  # set of all labels sets
    try:
        for name, config in configs.items():
            labels = config.get("labels")
            all_db_labels.add(frozenset((labels) if labels else frozenset()))
            databases[name] = DataBase(
                name,
                _resolve_dsn(config["dsn"], env),
                connect_sql=config.get("connect-sql"),
                keep_connected=config.get("keep-connected", True),
                autocommit=config.get("autocommit", True),
                labels=labels,
            )
    except Exception as e:
        raise ConfigError(str(e))

    db_labels: FrozenSet[str]
    if not all_db_labels:
        db_labels = frozenset()
    elif len(all_db_labels) > 1:
        raise ConfigError("Not all databases define the same labels")
    else:
        db_labels = all_db_labels.pop()

    return databases, db_labels


def _get_metrics(
    metrics: Dict[str, Dict[str, Any]], extra_labels: FrozenSet[str]
) -> Dict[str, MetricConfig]:
    """Return a dict mapping metric names to their configuration."""
    configs = {}
    # global metrics
    for metric_config in (
        _DB_ERRORS_METRIC_CONFIG,
        _QUERIES_METRIC_CONFIG,
        _QUERY_LATENCY_METRIC_CONFIG,
    ):
        # make a copy since labels are not immutable
        metric_config = deepcopy(metric_config)
        metric_config.config["labels"].extend(extra_labels)
        metric_config.config["labels"].sort()
        configs[metric_config.name] = metric_config
    # other metrics
    for name, config in metrics.items():
        _validate_metric_config(name, config, extra_labels)
        metric_type = config.pop("type")
        config.setdefault("labels", []).extend(extra_labels)
        config["labels"].sort()
        description = config.pop("description", "")
        configs[name] = MetricConfig(name, description, metric_type, config)
    return configs


def _validate_metric_config(
    name: str, config: Dict[str, Any], extra_labels: FrozenSet[str]
):
    """Validate a metric configuration stanza."""
    if name in GLOBAL_METRICS:
        raise ConfigError(f'Label name "{name} is reserved for builtin metric')
    labels = set(config.get("labels", ()))
    overlap_labels = labels & extra_labels
    if overlap_labels:
        overlap_list = ", ".join(sorted(overlap_labels))
        raise ConfigError(
            f'Labels for metric "{name}" overlap with reserved/database ones: {overlap_list}'
        )


def _get_queries(
    configs: Dict[str, Dict[str, Any]],
    database_names: FrozenSet[str],
    metrics: Dict[str, MetricConfig],
    extra_labels: FrozenSet[str],
) -> Dict[str, Query]:
    """Return a list of Queries from config."""
    metric_names = frozenset(metrics)
    queries: Dict[str, Query] = {}
    for name, config in configs.items():
        _validate_query_config(name, config, database_names, metric_names)
        _convert_query_interval(name, config)
        query_metrics = _get_query_metrics(config, metrics, extra_labels)
        parameters = config.get("parameters")
        try:
            if parameters:
                queries.update(
                    (
                        f"{name}[params{index}]",
                        Query(
                            f"{name}[params{index}]",
                            config["databases"],
                            query_metrics,
                            config["sql"].strip(),
                            parameters=params,
                            timeout=config.get("timeout"),
                            interval=config["interval"],
                            schedule=config.get("schedule"),
                            config_name=name,
                        ),
                    )
                    for index, params in enumerate(parameters)
                )
            else:
                queries[name] = Query(
                    name,
                    config["databases"],
                    query_metrics,
                    config["sql"].strip(),
                    timeout=config.get("timeout"),
                    interval=config["interval"],
                    schedule=config.get("schedule"),
                )
        except (InvalidQueryParameters, InvalidQuerySchedule) as e:
            raise ConfigError(str(e))
    return queries


def _get_query_metrics(
    config: Dict[str, Any],
    metrics: Dict[str, MetricConfig],
    extra_labels: FrozenSet[str],
) -> List[QueryMetric]:
    """Return QueryMetrics for a query."""

    def _metric_labels(labels: List[str]) -> List[str]:
        return sorted(set(labels) - extra_labels)

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


def _warn_if_unused(config: Config, logger: Logger):
    """Warn if there are unused databases or metrics defined."""
    used_dbs: Set[str] = set()
    used_metrics: Set[str] = set()
    for query in config.queries.values():
        used_dbs.update(query.databases)
        used_metrics.update(metric.name for metric in query.metrics)

    unused_dbs = sorted(set(config.databases) - used_dbs)
    if unused_dbs:
        logger.warning(
            f"unused entries in \"databases\" section: {', '.join(unused_dbs)}"
        )
    unused_metrics = sorted(set(config.metrics) - GLOBAL_METRICS - used_metrics)
    if unused_metrics:
        logger.warning(
            f"unused entries in \"metrics\" section: {', '.join(unused_metrics)}"
        )
