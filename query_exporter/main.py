"""Script entry point."""

import argparse
from functools import partial
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
        self.config = self._load_config(args.config)
        if args.check_only:
            self.exit()
        self.create_metrics(self.config.metrics.values())

    async def on_application_startup(self, application: Application):
        self.logger.info(f"version {__version__} starting up")
        query_loop = QueryLoop(self.config, self.registry, self.logger)
        application["exporter"].set_metric_update_handler(
            partial(self._update_handler, query_loop)
        )
        application["query-loop"] = query_loop
        await query_loop.start()

    async def on_application_shutdown(self, application: Application):
        await application["query-loop"].stop()

    async def _update_handler(self, query_loop: QueryLoop, metrics: List[MetricConfig]):
        """Run queries with no specified schedule on each request."""
        await query_loop.run_aperiodic_queries()
        query_loop.clear_expired_series()

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
