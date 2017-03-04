from collections import namedtuple

import yaml

from prometheus_aioexporter.metric import MetricConfig

from .db import Query, DataBase


class ConfigError(Exception):
    '''Configuration is invalid.'''


# Top-level configuration
Config = namedtuple('Config', ['databases', 'metrics', 'queries'])


def load_config(config_fd):
    '''Load YAML config from file.'''
    config = yaml.load(config_fd)
    databases = _get_databases(config.get('databases', {}))
    metrics = _get_metrics(config.get('metrics', {}))
    database_names = frozenset(database.name for database in databases)
    metric_names = frozenset(metric.name for metric in metrics)
    queries = _get_queries(
        config.get('queries', {}), database_names, metric_names)
    return Config(databases, metrics, queries)


def _get_databases(configs):
    '''Return a dict mapping names to DataBases names.'''
    databases = []
    for name, config in configs.items():
        try:
            databases.append(DataBase(name, config['dsn']))
        except KeyError as e:
            _raise_missing_key(e, 'database', name)
    return databases


def _get_metrics(metrics):
    '''Return metrics configuration.'''
    configs = []
    for name, config in metrics.items():
        metric_type = config.pop('type', '')
        description = config.pop('description', '')
        # add a 'database' label to have different series for sharded databases
        config['labels'] = ['database']
        configs.append(MetricConfig(name, description, metric_type, config))
    return configs


def _get_queries(configs, database_names, metric_names):
    '''Return a list of Queries from config.'''
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


def _validate_query_config(name, config, database_names, metric_names):
    '''Validate a query configuration stanza.'''
    unknown_databases = set(config['databases']) - database_names
    if unknown_databases:
        raise ConfigError(
            "Unknown databases for query '{}': {}".format(
                name, ', '.join(sorted(unknown_databases))))
    unknown_metrics = set(config['metrics']) - metric_names
    if unknown_metrics:
        raise ConfigError(
            "Unknown metrics for query '{}': {}".format(
                name, ', '.join(sorted(unknown_metrics))))


def _convert_interval(name, config):
    '''Convert query intervals to seconds.'''
    interval = config['interval']
    multiplier = 1

    config_error = ConfigError(
        "Invalid interval for query '{}'".format(name))

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


def _raise_missing_key(key_error, entry_type, entry_name):
    raise ConfigError(
        'Missing key {} for {} \'{}\''.format(
            str(key_error), entry_type, entry_name))
