"""Script entry point."""

from functools import partial
from pathlib import Path
import typing as t

from aiohttp.web import AppKey, Application
import click
from prometheus_aioexporter import (
    EXPORTER_APP_KEY,
    Arguments,
    InvalidMetricType,
    MetricConfig,
    PrometheusExporterScript,
)
from prometheus_client.metrics import Gauge

from . import __version__
from .config import (
    Config,
    ConfigError,
    load_config,
)
from .executor import QueryExecutor
from .executor_parallel import ParallelQueryExecutor
from .metrics import QUERY_INTERVAL_METRIC_NAME

# The application key to track the QueryExecutor
QUERY_EXECUTOR_APP_KEY: AppKey[QueryExecutor] = AppKey("query-executor")


class QueryExporterScript(PrometheusExporterScript):
    """Periodically run database queries and export results to Prometheus."""

    name = "query-exporter"
    version = __version__
    description = __doc__
    default_port = 9560
    envvar_prefix = "QE"

    def command_line_parameters(self) -> list[click.Parameter]:
        return [
            click.Option(
                ["--check-only"],
                type=bool,
                help="only check configuration, don't run the exporter",
                is_flag=True,
                show_default=True,
                show_envvar=True,
            ),
            click.Option(
                ["--config"],
                type=click.Path(
                    exists=True,
                    dir_okay=False,
                    path_type=Path,
                ),
                help="configuration file",
                multiple=True,
                default=[Path("config.yaml")],
                show_default=True,
                show_envvar=True,
            ),
        ]

    def configure(self, args: Arguments) -> None:
        self.config = self._load_config(args.config)
        if args.check_only:
            self.logger.info("configuration valid")
            raise SystemExit(0)
        self.create_metrics(self.config.metrics.values())
        self._set_static_metrics()

    async def on_application_startup(
        self, application: Application
    ) -> None:  # pragma: nocover
        if self.config.parallel.enabled:
            query_executor = ParallelQueryExecutor(
                self.config,
                self.registry,
                self.logger,
                pool_size=self.config.parallel.pool_size,
                max_overflow=self.config.parallel.max_overflow,
            )
            self.logger.info(
                "parallel execution enabled",
                pool_size=self.config.parallel.pool_size,
                max_overflow=self.config.parallel.max_overflow,
            )
        else:
            query_executor = QueryExecutor(self.config, self.registry, self.logger)
        application[EXPORTER_APP_KEY].set_metric_update_handler(
            partial(self._update_handler, query_executor)
        )
        application[QUERY_EXECUTOR_APP_KEY] = query_executor
        await query_executor.start()

    async def on_application_shutdown(
        self, application: Application
    ) -> None:  # pragma: nocover
        await application[QUERY_EXECUTOR_APP_KEY].stop()

    async def _update_handler(
        self, query_executor: QueryExecutor, metrics: list[MetricConfig]
    ) -> None:  # pragma: nocover
        """Run queries with no specified schedule on each request."""
        await query_executor.run_aperiodic_queries()
        query_executor.clear_expired_series()

    def _load_config(self, paths: list[Path]) -> Config:
        """Load the application configuration."""
        try:
            return load_config(paths, self.logger)
        except (InvalidMetricType, ConfigError) as error:
            self.logger.error("configuration invalid", error=str(error))
            if isinstance(error, ConfigError):
                for details in error.details:
                    self.logger.error("configuration invalid", **details)
            raise SystemExit(1)

    def _set_static_metrics(self) -> None:
        query_interval_metric = t.cast(
            Gauge, self.registry.get_metric(QUERY_INTERVAL_METRIC_NAME)
        )
        for query in self.config.queries.values():
            if query.interval:
                query_interval_metric.labels(query=query.name).set(
                    query.interval
                )


script = QueryExporterScript()
