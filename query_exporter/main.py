"""Script entry point."""

from functools import partial
from pathlib import Path

from aiohttp.web import Application
import click
from prometheus_aioexporter import (
    Arguments,
    InvalidMetricType,
    MetricConfig,
    PrometheusExporterScript,
)

from . import __version__
from .config import (
    Config,
    ConfigError,
    load_config,
)
from .loop import QueryLoop


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
            raise SystemExit(0)
        self.create_metrics(self.config.metrics.values())

    async def on_application_startup(self, application: Application) -> None:
        query_loop = QueryLoop(self.config, self.registry, self.logger)
        application["exporter"].set_metric_update_handler(
            partial(self._update_handler, query_loop)
        )
        application["query-loop"] = query_loop
        await query_loop.start()

    async def on_application_shutdown(self, application: Application) -> None:
        await application["query-loop"].stop()

    async def _update_handler(
        self, query_loop: QueryLoop, metrics: list[MetricConfig]
    ) -> None:
        """Run queries with no specified schedule on each request."""
        await query_loop.run_aperiodic_queries()
        query_loop.clear_expired_series()

    def _load_config(self, paths: list[Path]) -> Config:
        """Load the application configuration."""
        try:
            return load_config(paths, self.logger)
        except (InvalidMetricType, ConfigError) as error:
            self.logger.error("invalid config", error=str(error))
            raise SystemExit(1)


script = QueryExporterScript()
