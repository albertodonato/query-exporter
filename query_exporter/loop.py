"""Loop to periodically execute queries."""

import asyncio

from toolrack.aio import PeriodicCall

from .db import DataBaseError


class QueryLoop:
    """Periodically performs queries."""

    _METRIC_METHODS = {
        'counter': 'inc',
        'gauge': 'set',
        'histogram': 'observe',
        'summary': 'observe'}

    def __init__(self, config, registry, logger, loop):
        self.loop = loop
        self._logger = logger
        self._registry = registry
        self._periodic_queries = []
        self._aperiodic_queries = []
        self._periodic_calls = []
        self._setup(config)

    async def start(self):
        """Start periodic queries execution."""
        for db in self._databases.values():
            try:
                await db.connect()
            except DataBaseError as error:
                self._log_db_error(db.name, error)
            else:
                self._logger.debug(
                    'connected to database "{}"'.format(db.name))

        for query in self._periodic_queries:
            call = PeriodicCall(self.loop, self._run_query, query)
            self._periodic_calls.append(call)
            call.start(query.interval)

    async def stop(self):
        """Stop periodic query execution."""
        coros = (call.stop() for call in self._periodic_calls)
        await asyncio.gather(*coros, loop=self.loop)
        self._periodic_calls = []
        coros = (db.close() for db in self._databases.values())
        await asyncio.gather(*coros, loop=self.loop)

    async def run_aperiodic_queries(self):
        coros = (
            self._execute_query(query, dbname)
            for query in self._aperiodic_queries
            for dbname in query.databases)
        await asyncio.gather(*coros, loop=self.loop)

    def _setup(self, config):
        """Initialize instance attributes."""
        self._metric_configs = {
            metric_config.name: metric_config
            for metric_config in config.metrics}
        self._databases = {
            database.name: database for database in config.databases}

        for query in config.queries:
            if query.interval is None:
                self._aperiodic_queries.append(query)
            else:
                self._periodic_queries.append(query)

    def _run_query(self, query):
        """Periodic task to run a query."""
        for dbname in query.databases:
            self.loop.create_task(self._execute_query(query, dbname))

    async def _execute_query(self, query, dbname):
        """'Execute a Query on a DataBase."""
        self._logger.debug(
            'running query "{}" on database "{}"'.format(query.name, dbname))
        try:
            results = await self._databases[dbname].execute(query)
        except DataBaseError as error:
            self._log_query_error(query.name, dbname, error)
            return

        for name, values in results.items():
            for value in values:
                self._update_metric(name, value, dbname)

    def _log_query_error(self, name, dbname, error):
        """Log an error related to database query."""
        prefix = 'query "{}" on database "{}" failed:'.format(name, dbname)
        self._logger.error('{} {}'.format(prefix, error))

    def _log_db_error(self, name, error):
        """Log a failed database query."""
        prefix = 'error from database "{}":'.format(name)
        self._logger.error('{} {}'.format(prefix, error))

    def _update_metric(self, name, value, dbname):
        """Update value for a metric."""
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        method = self._METRIC_METHODS[self._metric_configs[name].type]
        self._logger.debug(
            'updating metric "{}" {}({})'.format(name, method, value))
        metric = self._registry.get_metric(name, labels={'database': dbname})
        getattr(metric, method)(value)
