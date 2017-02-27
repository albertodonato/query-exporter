import argparse
from time import time

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
        self._setup_config(args.config)
        self.periodic_call = PeriodicCall(self.loop, self._call)

    def on_application_startup(self, application):
        self.periodic_call.start(10)

    async def on_application_shutdown(self, application):
        await self.periodic_call.stop()

    def _setup_config(self, config_file):
        '''Setup attribute with different configuration components.'''
        config = self._load_config(config_file)
        self.metrics = self.create_metrics(config.metrics)
        self.metric_configs = {
            metric_config.name: metric_config
            for metric_config in config.metrics}
        self.databases = {
            database.name: database for database in config.databases}
        self.queries = config.queries
        # Track last execution time of each query on each database
        keys = (
            (query.name, db)
            for query in self.queries for db in query.databases)
        self.queries_db_last_time = dict.fromkeys(keys, 0)

    def _load_config(self, config_file):
        '''Load the application configuration.'''
        config = load_config(config_file)
        config_file.close()
        return config

    def _call(self):
        '''The periodic task function.'''
        now = time()
        for query in self.queries:
            name = query.name
            for dbname in query.databases:
                last_time = self.queries_db_last_time[name, dbname]
                if last_time + query.interval <= now:
                    self.loop.create_task(self._run_query(query, dbname))

    async def _run_query(self, query, dbname):
        self.logger.debug(
            "running query '{}' on database '{}'".format(query.name, dbname))
        async with self.databases[dbname].connect() as conn:
            results = await conn.execute(query)
        for name, value in results.items():
            self._update_metric(name, value)
        self.queries_db_last_time[(query.name, dbname)] = time()

    def _update_metric(self, name, value):
        '''Update value for a metric.'''
        metric_methods = {
            'counter': 'inc',
            'gauge': 'set',
            'histogram': 'observe',
            'summary': 'observe'}
        method = metric_methods[self.metric_configs[name].type]
        self.logger.debug(
            "metric update for '{}': {} {}".format(name, method, value))
        getattr(self.metrics[name], method)(value)


script = QueryExporterScript()
