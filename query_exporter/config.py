"""Configuration management functions."""

from collections import defaultdict
from copy import deepcopy
from dataclasses import (
    dataclass,
    field,
)
from functools import reduce
import itertools
from logging import Logger
import os
from pathlib import Path
import re
from typing import (
    Any,
    Dict,
    FrozenSet,
    IO,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import (
    quote_plus,
    urlencode,
)

import jsonschema
from prometheus_aioexporter import MetricConfig
import yaml

from . import PACKAGE
from .db import (
    create_db_engine,
    DATABASE_LABEL,
    DataBaseError,
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


@dataclass(frozen=True)
class DataBaseConfig:
    """Configuration for a database."""

    name: str
    dsn: str
    connect_sql: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    keep_connected: bool = True
    autocommit: bool = True

    def __post_init__(self):
        try:
            create_db_engine(self.dsn)
        except DataBaseError as e:
            raise ConfigError(str(e))


@dataclass(frozen=True)
class Config:
    """Top-level configuration."""

    databases: Dict[str, DataBaseConfig]
    metrics: Dict[str, MetricConfig]
    queries: Dict[str, Query]


# Type matching os.environ.
Environ = Mapping[str, str]

# Content for the "parameters" config option
ParametersConfig = Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]


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
) -> Tuple[Dict[str, DataBaseConfig], FrozenSet[str]]:
    """Return a dict mapping names to database configs, and a set of database labels."""
    databases = {}
    all_db_labels: Set[FrozenSet[str]] = set()  # set of all labels sets
    try:
        for name, config in configs.items():
            labels = config.get("labels", {})
            all_db_labels.add(frozenset(labels))
            databases[name] = DataBaseConfig(
                name,
                _resolve_dsn(config["dsn"], env),
                connect_sql=config.get("connect-sql", []),
                labels=labels,
                keep_connected=config.get("keep-connected", True),
                autocommit=config.get("autocommit", True),
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
        config["expiration"] = _convert_interval(config.get("expiration"))
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
        query_metrics = _get_query_metrics(config, metrics, extra_labels)
        parameters = config.get("parameters")

        query_args = {
            "databases": config["databases"],
            "metrics": query_metrics,
            "sql": config["sql"].strip(),
            "timeout": config.get("timeout"),
            "interval": _convert_interval(config.get("interval")),
            "schedule": config.get("schedule"),
            "config_name": name,
        }

        try:
            if parameters:
                parameters_sets = _get_parameters_sets(parameters)
                queries.update(
                    (
                        f"{name}[params{index}]",
                        Query(
                            name=f"{name}[params{index}]",
                            parameters=params,
                            **query_args,
                        ),
                    )
                    for index, params in enumerate(parameters_sets)
                )
            else:
                queries[name] = Query(name, **query_args)
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
        if isinstance(parameters, dict):
            for key, params in parameters.items():
                keys = {frozenset(param.keys()) for param in params}
                if len(keys) > 1:
                    raise ConfigError(
                        f'Invalid parameters definition by path "{key}" for query "{name}": '
                        "parameters dictionaries must all have the same keys"
                    )
        else:
            keys = {frozenset(param.keys()) for param in parameters}
            if len(keys) > 1:
                raise ConfigError(
                    f'Invalid parameters definition for query "{name}": '
                    "parameters dictionaries must all have the same keys"
                )


def _convert_interval(interval: Union[int, str, None]) -> Optional[int]:
    """Convert a time interval to seconds.

    Return None if no interval is specified.

    """
    if interval is None:
        return None

    multiplier = 1
    if isinstance(interval, str):
        # convert to seconds
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 3600 * 24}
        suffix = interval[-1]
        if suffix in multipliers:
            interval = interval[:-1]
            multiplier = multipliers[suffix]

    return int(interval) * multiplier


def _resolve_dsn(dsn: Union[str, Dict[str, Any]], env: Environ) -> str:
    """Build and resolve the database DSN string from the right source."""

    def from_env(varname: str) -> str:
        if not _ENV_VAR_RE.match(varname):
            raise ValueError(f'Invalid variable name: "{varname}"')
        if varname not in env:
            raise ValueError(f'Undefined variable: "{varname}"')
        return env[varname]

    def from_file(filename: str) -> str:
        try:
            return Path(filename).read_text().strip()
        except OSError as err:
            raise ValueError(f'Unable to read dsn file : "{filename}": {err.strerror}')

    origins = {
        "env": from_env,
        "file": from_file,
    }

    if isinstance(dsn, dict):
        dsn = _build_dsn(dsn)
    elif ":" in dsn:
        source, value = dsn.split(":", 1)
        handler = origins.get(source)
        if handler is not None:
            return handler(value)

    return dsn


def _build_dsn(details: Dict[str, Any]) -> str:
    """Build a DSN string from details."""
    url = f"{details['dialect']}://"
    user = details.get("user")
    if user:
        url += quote_plus(user)
    password = details.get("password")
    if password:
        url += f":{quote_plus(password)}"
    if user or password:
        url += "@"
    host = details.get("host")
    if host:
        url += host
    port = details.get("port")
    if port:
        url += f":{port}"
    database = details.get("database")
    if database:
        if not database.startswith("/"):
            database = f"/{database}"
        url += database
    query = details.get("options")
    if query:
        url += f"?{urlencode(query, doseq=True)}"
    return url


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


def _get_parameters_sets(parameters: ParametersConfig) -> List[Dict[str, Any]]:
    """Return an sequence of set of paramters with their values."""
    if isinstance(parameters, dict):
        return _get_parameters_matrix(parameters)
    return parameters


def _get_parameters_matrix(
    parameters: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """Return parameters combinations from a matrix."""
    # first, flatten dict like
    #
    # {
    #     'a': [{'arg1': 1, 'arg2': 1}],
    #     'b': [{'arg1': 1, 'arg2': 1}],
    # }
    #
    # into a sequence like
    #
    # (
    #     [{'a__arg1': 1, 'a__arg2': 2}],
    #     [{'b__arg1': 1, 'b__arg2': 2}],
    # )
    flattened_params = (
        [
            {f"{top_key}__{key}": value for key, value in arg_set.items()}
            for arg_set in arg_sets
        ]
        for top_key, arg_sets in parameters.items()
    )
    # return a list of merged dictionaries from each combination of the two
    # sets
    return list(
        reduce(lambda p1, p2: {**p1, **p2}, params)
        for params in itertools.product(*flattened_params)
    )
