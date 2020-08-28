"""Script entry point."""

import argparse
from typing import (
    IO,
    List,
)

from aiohttp.web import Application
from argcomplete import autocomplete
from prometheus_aioexporter import (
    MetricConfig,
    PrometheusExporterScript,
)
from prometheus_aioexporter.metric import InvalidMetricType
from toolrack.script import ErrorExitMessage

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
    description = __doc__
    default_port = 9560

    def configure_argument_parser(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "config", type=argparse.FileType("r"), help="configuration file"
        )
        parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=f"%(prog)s {__version__}",
        )
        parser.add_argument(
            "--check-only",
            action="store_true",
            help="only check configuration, don't run the exporter",
        )
        autocomplete(parser)

    def configure(self, args: argparse.Namespace):
        config = self._load_config(args.config)
        if args.check_only:
            self.exit()
        self.create_metrics(config.metrics.values())
        self.query_loop = QueryLoop(config, self.registry, self.logger)

    async def on_application_startup(self, application: Application):
        application["exporter"].set_metric_update_handler(self._update_handler)
        await self.query_loop.start()

    async def on_application_shutdown(self, application: Application):
        await self.query_loop.stop()

    async def _update_handler(self, metrics: List[MetricConfig]):
        """Run queries with no specified schedule on each request."""
        await self.query_loop.run_aperiodic_queries()

    def _load_config(self, config_file: IO) -> Config:
        """Load the application configuration."""
        try:
            config = load_config(config_file, self.logger)
        except (InvalidMetricType, ConfigError) as error:
            raise ErrorExitMessage(str(error))
        finally:
            config_file.close()
        return config


script = QueryExporterScript()
