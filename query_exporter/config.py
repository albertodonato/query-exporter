"""Configuration management functions."""

from collections import defaultdict
import dataclasses
from pathlib import Path
import typing as t

from prometheus_aioexporter import MetricConfig
from pydantic import ValidationError
import structlog
import yaml

from . import db, schema
from .metrics import BUILTIN_METRICS, get_builtin_metric_configs
from .yaml import load_yaml

# Label used to tag metrics by database
DATABASE_LABEL = "database"


class ConfigError(Exception):
    """Configuration is invalid.

    Optionally, a list of dicts can be provided with error details.
    """

    def __init__(self, message: str, details: t.Sequence[dict[str, str]] = ()):
        super().__init__(message)
        self.details = details


@dataclasses.dataclass(frozen=True)
class Config:
    """Top-level configuration."""

    databases: dict[str, schema.Database]
    metrics: dict[str, MetricConfig]
    queries: dict[str, db.Query]


def load_config(
    paths: list[Path],
    logger: structlog.stdlib.BoundLogger | None = None,
) -> Config:
    """Load YAML configuration files."""
    if logger is None:
        logger = structlog.get_logger()

    try:
        data = _load_config(paths)
    except yaml.scanner.ScannerError as e:
        raise ConfigError(str(e))
    try:
        configuration = schema.ExporterConfig(**data)
    except ValidationError as ve:
        errors = ve.errors()
        details = [
            {
                "location": (
                    ".".join(str(item) for item in e["loc"])
                    if e["loc"]
                    else ""
                ),
                "error": e["msg"],
            }
            for e in errors
        ]
        raise ConfigError(
            f"{len(errors)} validation error{'s' if len(errors) > 1 else ''}",
            details=details,
        )
    database_labels = _validate_databases(configuration.databases)
    extra_labels = frozenset([DATABASE_LABEL]) | database_labels
    builtin_metrics_config = (
        {
            name: metric.config()
            for name, metric in configuration.builtin_metrics.as_dict().items()
        }
        if configuration.builtin_metrics
        else {}
    )
    metrics = _get_metrics(
        configuration.metrics, builtin_metrics_config, extra_labels
    )
    queries = _get_queries(
        configuration.queries,
        frozenset(configuration.databases),
        metrics,
        extra_labels,
    )
    config = Config(configuration.databases, metrics, queries)
    _warn_if_unused(config, logger)
    return config


def _load_config(paths: list[Path]) -> dict[str, t.Any]:
    """Return the combined configuration from provided files."""
    config: dict[str, t.Any] = defaultdict(dict)
    for path in paths:
        conf = load_yaml(path)
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


def _validate_databases(dbs: dict[str, schema.Database]) -> frozenset[str]:
    """Validate database configuration and return a set of all database labels."""
    all_db_labels: set[frozenset[str]] = set()  # set of all labels sets
    try:
        for name, config in dbs.items():
            # raise an error if the configuration is not valid
            db.create_db_engine(config)
            all_db_labels.add(frozenset(config.labels))
    except Exception as e:
        raise ConfigError(str(e))

    db_labels: frozenset[str]
    if not all_db_labels:
        db_labels = frozenset()
    elif len(all_db_labels) > 1:
        raise ConfigError("Not all databases define the same labels")
    else:
        db_labels = all_db_labels.pop()

    return db_labels


def _get_metrics(
    metrics: dict[str, schema.Metric],
    builtin_metrics_config: dict[str, dict[str, t.Any]],
    extra_labels: frozenset[str],
) -> dict[str, MetricConfig]:
    """Return a dict mapping metric names to their configuration."""
    configs = get_builtin_metric_configs(extra_labels, builtin_metrics_config)
    for name, metric in metrics.items():
        _validate_metric_config(name, metric, extra_labels)
        configs[name] = MetricConfig(
            name,
            metric.description,
            metric.type,
            labels=list(set(metric.labels) | extra_labels),
            config=metric.config,
        )
    return configs


def _validate_metric_config(
    name: str, metric: schema.Metric, extra_labels: frozenset[str]
) -> None:
    """Validate a metric configuration stanza."""
    if name in BUILTIN_METRICS:
        raise ConfigError(f'Label name "{name} is reserved for builtin metric')
    labels = set(metric.labels)
    overlap_labels = labels & extra_labels
    if overlap_labels:
        overlap_list = ", ".join(sorted(overlap_labels))
        raise ConfigError(
            f'Labels for metric "{name}" overlap with reserved/database ones: {overlap_list}'
        )


def _get_queries(
    configs: dict[str, schema.Query],
    database_names: frozenset[str],
    metrics: dict[str, MetricConfig],
    extra_labels: frozenset[str],
) -> dict[str, db.Query]:
    """Return a list of Queries from config."""
    metric_names = frozenset(metrics)
    queries: dict[str, db.Query] = {}
    for name, config in configs.items():
        _validate_query_config(name, config, database_names, metric_names)
        query_metrics = _get_query_metrics(config, metrics, extra_labels)
        parameter_sets = t.cast(list[dict[str, t.Any]], config.parameters)
        try:
            queries[name] = db.Query(
                name=name,
                databases=config.databases,
                metrics=query_metrics,
                sql=config.sql.strip(),
                timeout=config.timeout,
                interval=config.interval,
                schedule=config.schedule,
                parameter_sets=parameter_sets,
            )
        except db.InvalidQueryParameters as e:
            raise ConfigError(str(e))
    return queries


def _get_query_metrics(
    query: schema.Query,
    metrics: dict[str, MetricConfig],
    extra_labels: frozenset[str],
) -> list[db.QueryMetric]:
    """Return QueryMetrics for a query."""

    def _metric_labels(labels: t.Iterable[str]) -> list[str]:
        return sorted(set(labels) - extra_labels)

    return [
        db.QueryMetric(name, _metric_labels(metrics[name].labels))
        for name in query.metrics
    ]


def _validate_query_config(
    name: str,
    query: schema.Query,
    database_names: frozenset[str],
    metric_names: frozenset[str],
) -> None:
    """Validate a query configuration stanza."""
    unknown_databases = set(query.databases) - database_names
    if unknown_databases:
        unknown_list = ", ".join(sorted(unknown_databases))
        raise ConfigError(
            f'Unknown databases for query "{name}": {unknown_list}'
        )
    unknown_metrics = set(query.metrics) - metric_names
    if unknown_metrics:
        unknown_list = ", ".join(sorted(unknown_metrics))
        raise ConfigError(
            f'Unknown metrics for query "{name}": {unknown_list}'
        )
    if query.parameters:
        params = t.cast(list[dict[str, t.Any]], query.parameters)
        keys = {frozenset(param.keys()) for param in params}
        if len(keys) > 1:
            raise ConfigError(
                f'Invalid parameters definition for query "{name}": '
                "parameters dictionaries must all have the same keys"
            )


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
