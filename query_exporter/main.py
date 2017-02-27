import argparse

from prometheus_aioexporter.script import PrometheusExporterScript

from .config import load_config
from .loop import QueryLoop


class QueryExporterScript(PrometheusExporterScript):
    '''Periodically run database queries and export results to Prometheus.'''

    name = 'query-exporter'

    description = __doc__

    def configure_argument_parser(self, parser):
        parser.add_argument(
            'config', type=argparse.FileType('r'),
            help='configuration file')

    def configure(self, args):
        config = self._load_config(args.config)
        metrics = self.create_metrics(config.metrics)
        self.query_loop = QueryLoop(config, metrics, self.logger, self.loop)

    def on_application_startup(self, application):
        self.query_loop.start()

    async def on_application_shutdown(self, application):
        await self.query_loop.stop()

    def _load_config(self, config_file):
        '''Load the application configuration.'''
        config = load_config(config_file)
        config_file.close()
        return config


script = QueryExporterScript()
