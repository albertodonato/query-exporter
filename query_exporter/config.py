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

from prometheus_aioexporter import MetricConfig
import yaml

from .db import (
    DataBase,
    Query,
    validate_dsn,
)

# the label used to filter metrics by database
DATABASE_LABEL = 'database'

# metric for counting database errors
DB_ERRORS_METRIC_NAME = 'database_errors'
DB_ERRORS_METRIC = MetricConfig(
    DB_ERRORS_METRIC_NAME, 'Number of database errors', 'counter',
    {'labels': [DATABASE_LABEL]})
# metric for counting performed queries
QUERIES_METRIC_NAME = 'queries'
QUERIES_METRIC = MetricConfig(
    QUERIES_METRIC_NAME, 'Number of database queries', 'counter',
    {'labels': [DATABASE_LABEL, 'status']})


class ConfigError(Exception):
    """Configuration is invalid."""


class Config(NamedTuple):
    """Top-level configuration."""

    databases: List[DataBase]
    metrics: List[MetricConfig]
    queries: List[Query]


# Supported metric types.
SUPPORTED_METRICS = ('counter', 'enum', 'gauge', 'histogram', 'summary')

# Type matching os.environ.
Environ = Mapping[str, str]


def load_config(config_fd, env: Environ = os.environ) -> Config:
    """Load YaML config from file."""
    config = defaultdict(dict, yaml.safe_load(config_fd))
    databases = _get_databases(config['databases'], env)
    metrics = _get_metrics(config['metrics'])
    database_names = frozenset(database.name for database in databases)
    metric_names = frozenset(metric.name for metric in metrics)
    queries = _get_queries(config['queries'], database_names, metric_names)
    return Config(databases, metrics, queries)


def _get_databases(configs: Dict[str, Dict[str, Any]],
                   env: Environ) -> List[DataBase]:
    """Return a dict mapping names to DataBases names."""
    databases = []
    for name, config in configs.items():
        try:
            databases.append(DataBase(name, _resolve_dsn(config['dsn'], env)))
        except KeyError as e:
            _raise_missing_key(e, 'database', name)
        except Exception as e:
            raise ConfigError(str(e))
    return databases


def _get_metrics(metrics: Dict[str, Dict[str, Any]]) -> List[MetricConfig]:
    """Return metrics configuration."""
    # add global metrics
    configs = [DB_ERRORS_METRIC, QUERIES_METRIC]
    for name, config in metrics.items():
        metric_type = config.pop('type', '')
        if metric_type not in SUPPORTED_METRICS:
            raise ConfigError(f"Unsupported metric type: '{metric_type}'")
        description = config.pop('description', '')
        # add a label to allow filtering by database
        config['labels'] = [DATABASE_LABEL]
        configs.append(MetricConfig(name, description, metric_type, config))
    return configs


def _get_queries(
        configs: Dict[str, Dict[str, Any]], database_names: FrozenSet[str],
        metric_names: FrozenSet[str]) -> List[Query]:
    """Return a list of Queries from config."""
    queries = []
    for name, config in configs.items():
        try:
            _validate_query_config(name, config, database_names, metric_names)
            _convert_interval(name, config)
            queries.append(
                Query(
                    name, config['interval'], config['databases'],
                    config['metrics'], config['sql'].strip()))
        except KeyError as e:
            _raise_missing_key(e, 'query', name)
    return queries


def _validate_query_config(
        name: str, config: Dict[str, Any], database_names: FrozenSet[str],
        metric_names: FrozenSet[str]):
    """Validate a query configuration stanza."""
    unknown_databases = set(config['databases']) - database_names
    if unknown_databases:
        unknown_list = ', '.join(sorted(unknown_databases))
        raise ConfigError(
            f"Unknown databases for query '{name}': {unknown_list}")
    unknown_metrics = set(config['metrics']) - metric_names
    if unknown_metrics:
        unknown_list = ', '.join(sorted(unknown_metrics))
        raise ConfigError(
            f"Unknown metrics for query '{name}': {unknown_list}")


def _convert_interval(name: str, config: Dict[str, Any]):
    """Convert query intervals to seconds."""
    interval = config.setdefault('interval', None)
    if interval is None:
        # the query should be run at every request
        return

    multiplier = 1

    config_error = ConfigError(f"Invalid interval for query '{name}'")

    if isinstance(interval, str):
        # convert to seconds
        multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 3600 * 24}
        suffix = interval[-1]
        if suffix in multipliers:
            interval = interval[:-1]
            multiplier = multipliers[suffix]

        if '.' in interval:
            raise config_error

    try:
        interval = int(interval)
    except ValueError:
        raise config_error

    if interval <= 0:
        raise config_error

    config['interval'] = interval * multiplier


def _resolve_dsn(dsn: str, env: Environ) -> str:
    if dsn.startswith('env:'):
        _, varname = dsn.split(':', 1)
        if not re.match(r'[a-zA-Z_][a-zA-Z0-9_]*$', varname):
            raise ValueError(f"Invalid variable name: '{varname}'")
        if varname not in env:
            raise ValueError(f"Undefined variable: '{varname}'")
        dsn = env[varname]

    validate_dsn(dsn)
    return dsn


def _raise_missing_key(key_error: KeyError, entry_type: str, entry_name: str):
    raise ConfigError(
        f"Missing key {str(key_error)} for {entry_type} '{entry_name}'")
