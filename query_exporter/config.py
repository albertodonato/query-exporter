"""Configuration management functions."""

from collections import defaultdict
from collections.abc import Mapping
import dataclasses
from functools import reduce
from importlib import resources
import itertools
import os
from pathlib import Path
import re
import typing as t
from urllib.parse import (
    quote_plus,
    urlencode,
)

import jsonschema
from prometheus_aioexporter import MetricConfig
import structlog
import yaml

from .db import (
    DataBaseConfig,
    InvalidQueryParameters,
    InvalidQuerySchedule,
    Query,
    QueryMetric,
)
from .metrics import BUILTIN_METRICS, get_builtin_metric_configs
from .yaml import load_yaml_config

# Label used to tag metrics by database
DATABASE_LABEL = "database"

# regexp for validating environment variables names
_ENV_VAR_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*$")


class ConfigError(Exception):
    """Configuration is invalid."""


@dataclasses.dataclass(frozen=True)
class Config:
    """Top-level configuration."""

    databases: dict[str, DataBaseConfig]
    metrics: dict[str, MetricConfig]
    queries: dict[str, Query]


# Type matching os.environ.
Environ = Mapping[str, str]

# Content for the "parameters" config option
ParametersConfig = list[dict[str, t.Any]] | dict[str, list[dict[str, t.Any]]]


def load_config(
    paths: list[Path],
    logger: structlog.stdlib.BoundLogger | None = None,
    env: Environ = os.environ,
) -> Config:
    """Load YAML config from file."""
    if logger is None:
        logger = structlog.get_logger()

    try:
        data = _load_config(paths)
    except yaml.scanner.ScannerError as e:
        raise ConfigError(str(e))
    _validate_config(data)
    databases, database_labels = _get_databases(data["databases"], env)
    extra_labels = frozenset([DATABASE_LABEL]) | database_labels
    metrics = _get_metrics(data["metrics"], extra_labels)
    queries = _get_queries(
        data["queries"], frozenset(databases), metrics, extra_labels
    )
    config = Config(databases, metrics, queries)
    _warn_if_unused(config, logger)
    return config


def _load_config(paths: list[Path]) -> dict[str, t.Any]:
    """Return the combined configuration from provided files."""
    config: dict[str, t.Any] = defaultdict(dict)
    for path in paths:
        conf = load_yaml_config(path)
        if not isinstance(conf, dict):
            raise ConfigError(f"File content is not a mapping: {path}")
        data = defaultdict(dict, conf)
        for key in (field.name for field in dataclasses.fields(Config)):
            entries = data.pop(key, None)
            if entries is not None:
                if overlap_entries := set(config[key]) & set(entries):
                    overlap_list = ", ".join(sorted(overlap_entries))
                    raise ConfigError(
                        f'Duplicated entries in the "{key}" section: {overlap_list}'
                    )
                config[key].update(entries)
        config.update(data)
    return config


def _validate_config(config: dict[str, t.Any]) -> None:
    schema_file = resources.files("query_exporter") / "schemas" / "config.yaml"
    schema = yaml.safe_load(schema_file.read_bytes())
    try:
        jsonschema.validate(config, schema)
    except jsonschema.ValidationError as e:
        path = "/".join(str(item) for item in e.absolute_path)
        raise ConfigError(f"Invalid config at {path}: {e.message}")


def _get_databases(
    configs: dict[str, dict[str, t.Any]], env: Environ
) -> tuple[dict[str, DataBaseConfig], frozenset[str]]:
    """Return a dict mapping names to database configs, and a set of database labels."""
    databases = {}
    all_db_labels: set[frozenset[str]] = set()  # set of all labels sets
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

    db_labels: frozenset[str]
    if not all_db_labels:
        db_labels = frozenset()
    elif len(all_db_labels) > 1:
        raise ConfigError("Not all databases define the same labels")
    else:
        db_labels = all_db_labels.pop()

    return databases, db_labels


def _get_metrics(
    metrics: dict[str, dict[str, t.Any]], extra_labels: frozenset[str]
) -> dict[str, MetricConfig]:
    """Return a dict mapping metric names to their configuration."""
    configs = get_builtin_metric_configs(extra_labels)
    for name, config in metrics.items():
        _validate_metric_config(name, config, extra_labels)
        metric_type = config.pop("type")
        labels = set(config.pop("labels", ())) | extra_labels
        config["expiration"] = _convert_interval(config.get("expiration"))
        description = config.pop("description", "")
        configs[name] = MetricConfig(
            name, description, metric_type, labels=labels, config=config
        )
    return configs


def _validate_metric_config(
    name: str, config: dict[str, t.Any], extra_labels: frozenset[str]
) -> None:
    """Validate a metric configuration stanza."""
    if name in BUILTIN_METRICS:
        raise ConfigError(f'Label name "{name} is reserved for builtin metric')
    labels = set(config.get("labels", ()))
    overlap_labels = labels & extra_labels
    if overlap_labels:
        overlap_list = ", ".join(sorted(overlap_labels))
        raise ConfigError(
            f'Labels for metric "{name}" overlap with reserved/database ones: {overlap_list}'
        )


def _get_queries(
    configs: dict[str, dict[str, t.Any]],
    database_names: frozenset[str],
    metrics: dict[str, MetricConfig],
    extra_labels: frozenset[str],
) -> dict[str, Query]:
    """Return a list of Queries from config."""
    metric_names = frozenset(metrics)
    queries: dict[str, Query] = {}
    for name, config in configs.items():
        _validate_query_config(name, config, database_names, metric_names)
        query_metrics = _get_query_metrics(config, metrics, extra_labels)
        try:
            queries[name] = Query(
                name=name,
                databases=config["databases"],
                metrics=query_metrics,
                sql=config["sql"].strip(),
                timeout=config.get("timeout"),
                interval=_convert_interval(config.get("interval")),
                schedule=config.get("schedule"),
                parameter_sets=_get_parameter_sets(config.get("parameters")),
            )
        except (InvalidQueryParameters, InvalidQuerySchedule) as e:
            raise ConfigError(str(e))
    return queries


def _get_query_metrics(
    config: dict[str, t.Any],
    metrics: dict[str, MetricConfig],
    extra_labels: frozenset[str],
) -> list[QueryMetric]:
    """Return QueryMetrics for a query."""

    def _metric_labels(labels: t.Iterable[str]) -> list[str]:
        return sorted(set(labels) - extra_labels)

    return [
        QueryMetric(name, _metric_labels(metrics[name].labels))
        for name in config["metrics"]
    ]


def _validate_query_config(
    name: str,
    config: dict[str, t.Any],
    database_names: frozenset[str],
    metric_names: frozenset[str],
) -> None:
    """Validate a query configuration stanza."""
    unknown_databases = set(config["databases"]) - database_names
    if unknown_databases:
        unknown_list = ", ".join(sorted(unknown_databases))
        raise ConfigError(
            f'Unknown databases for query "{name}": {unknown_list}'
        )
    unknown_metrics = set(config["metrics"]) - metric_names
    if unknown_metrics:
        unknown_list = ", ".join(sorted(unknown_metrics))
        raise ConfigError(
            f'Unknown metrics for query "{name}": {unknown_list}'
        )
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


def _convert_interval(interval: int | str | None) -> int | None:
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


def _resolve_dsn(dsn: str | dict[str, t.Any], env: Environ) -> str:
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
            raise ValueError(
                f'Unable to read dsn file : "{filename}": {err.strerror}'
            )

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
            logger = structlog.get_logger()
            logger.warn(
                f"deprecated DSN source '{dsn}', use '!{source} {value}' instead",
                source=source,
                value=value,
            )
            return handler(value)

    return dsn


def _build_dsn(details: dict[str, t.Any]) -> str:
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


def _warn_if_unused(
    config: Config, logger: structlog.stdlib.BoundLogger
) -> None:
    """Warn if there are unused databases or metrics defined."""
    used_dbs: set[str] = set()
    used_metrics: set[str] = set()
    for query in config.queries.values():
        used_dbs.update(query.databases)
        used_metrics.update(metric.name for metric in query.metrics)

    if unused_dbs := sorted(set(config.databases) - used_dbs):
        logger.warning(
            "unused config entries",
            section="databases",
            entries=unused_dbs,
        )
    if unused_metrics := sorted(
        set(config.metrics) - BUILTIN_METRICS - used_metrics
    ):
        logger.warning(
            "unused config entries",
            section="metrics",
            entries=unused_metrics,
        )


def _get_parameter_sets(
    parameters: ParametersConfig | None,
) -> list[dict[str, t.Any]]:
    """Return an sequence of set of paramters with their values."""
    if not parameters:
        return []
    if isinstance(parameters, dict):
        return _get_parameters_matrix(parameters)
    return parameters


def _get_parameters_matrix(
    parameters: dict[str, list[dict[str, t.Any]]],
) -> list[dict[str, t.Any]]:
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
