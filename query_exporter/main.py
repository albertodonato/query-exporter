import argparse
from datetime import datetime

from prometheus_aioexporter.script import PrometheusExporterScript
from prometheus_aioexporter.metric import MetricConfig

from toolrack.async import PeriodicCall


class QueryExporterScript(PrometheusExporterScript):
    '''Export Prometheus metrics generated from SQL queries.'''

    name = 'query-exporter'

    def configure_argument_parser(self, parser):
        parser.add_argument(
            'config', type=argparse.FileType('r'),
            help='configuration file')

    def configure(self, args):
        metric_configs = [
            MetricConfig('sample_metric1', 'Sample metric 1', 'gauge', {}),
            MetricConfig('sample_metric2', 'Sample metric 2', 'gauge', {})]
        metrics = self.create_metrics(metric_configs)
        self.periodic_call = PeriodicCall(self.loop, _loop, self.loop, metrics)

    def on_application_startup(self, application):
        self.periodic_call.start(10)

    async def on_application_shutdown(self, application):
        await self.periodic_call.stop()


def _loop(loop, metrics):
    print('>>', datetime.now())
    loop.create_task(_loop2(metrics))


async def _loop2(metrics):
    from .db import DataBase, Query
    q = Query('test-query', 20, ['sample_metric1'], 'SELECT random() * 1000')
    async with DataBase('db', 'dbname=ack') as db:
        results = await db.execute(q)
        for metric, value in results.items():
            metrics[metric].set(value)


script = QueryExporterScript()
