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
    queries = _get_queries(config.get('queries', {}))
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
        configs.append(MetricConfig(name, description, metric_type, config))
    return configs


def _get_queries(configs):
    '''Return a list of Queries from config.'''
    queries = []
    for name, config in configs.items():
        try:
            queries.append(
                Query(
                    name, config['interval'], config['metrics'],
                    config['sql'].strip()))
        except KeyError as e:
            _raise_missing_key(e, 'query', name)
    return queries


def _raise_missing_key(key_error, entry_type, entry_name):
    raise ConfigError(
        'Missing key "{}" for {} "{}"'.format(
            str(key_error), entry_type, entry_name))
