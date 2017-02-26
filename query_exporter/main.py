import argparse
from datetime import datetime

from prometheus_aioexporter.script import PrometheusExporterScript

from toolrack.async import PeriodicCall

from .config import load_config
from .db import DataBase


class QueryExporterScript(PrometheusExporterScript):
    '''Export Prometheus metrics generated from SQL queries.'''

    name = 'query-exporter'

    def configure_argument_parser(self, parser):
        parser.add_argument(
            'config', type=argparse.FileType('r'),
            help='configuration file')

    def configure(self, args):
        config = self._load_config(args.config)
        metrics = self.create_metrics(config.metrics)
        self.periodic_call = PeriodicCall(
            self.loop, _loop, self.loop, config, metrics)

    def on_application_startup(self, application):
        self.periodic_call.start(10)

    async def on_application_shutdown(self, application):
        await self.periodic_call.stop()

    def _load_config(self, config_file):
        '''Load the application configuration.'''
        config = load_config(config_file)
        config_file.close()
        return config


def _loop(loop, config, metrics):
    print('>>', datetime.now())
    loop.create_task(_loop2(config, metrics))


async def _loop2(config, metrics):
    [database] = config.databases  # XXX
    for query in config.queries:
        async with database:
            results = await database.execute(query)
            for metric, value in results.items():
                metrics[metric].set(value)


script = QueryExporterScript()
