import argparse
from datetime import datetime

from prometheus_aioexporter.script import PrometheusExporterScript

from toolrack.async import PeriodicCall

from .config import load_config


class QueryExporterScript(PrometheusExporterScript):
    '''Periodically run database queries and export results to Prometheus.'''

    name = 'query-exporter'

    description = __doc__

    def configure_argument_parser(self, parser):
        parser.add_argument(
            'config', type=argparse.FileType('r'),
            help='configuration file')

    def configure(self, args):
        self.config = self._load_config(args.config)
        self.metrics = self.create_metrics(self.config.metrics)
        self.periodic_call = PeriodicCall(self.loop, self._main_loop)

    def on_application_startup(self, application):
        self.periodic_call.start(10)

    async def on_application_shutdown(self, application):
        await self.periodic_call.stop()

    def _load_config(self, config_file):
        '''Load the application configuration.'''
        config = load_config(config_file)
        config_file.close()
        return config

    def _main_loop(self):
        self.loop.create_task(self._main_loop_task())

    async def _main_loop_task(self):
        [database] = self.config.databases  # XXX
        for query in self.config.queries:
            async with database:
                results = await database.execute(query)
                for metric, value in results.items():
                    self.metrics[metric].set(value)


script = QueryExporterScript()
