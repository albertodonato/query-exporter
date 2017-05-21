"""Loop to periodically execute queries."""

import asyncio

from toolrack.async import PeriodicCall

from .db import DataBaseError


class QueryLoop:
    """Periodically performs queries."""

    def __init__(self, config, metrics, logger, loop):
        self.loop = loop
        self._logger = logger
        self._metrics = metrics
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

        for query in self._queries:
            call = PeriodicCall(self.loop, self._run_query, query)
            self._periodic_calls.append(call)
            call.start(query.interval)

    async def stop(self):
        """Stop periodic query execution."""
        coroutines = (call.stop() for call in self._periodic_calls)
        await asyncio.gather(*coroutines, loop=self.loop)
        self._periodic_calls = []
        coroutines = (db.close() for db in self._databases.values())
        await asyncio.gather(*coroutines, loop=self.loop)

    def _setup(self, config):
        """Initialize instance attributes."""
        self._metric_configs = {
            metric_config.name: metric_config
            for metric_config in config.metrics}
        self._databases = {
            database.name: database for database in config.databases}
        self._queries = config.queries

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
        metric_methods = {
            'counter': 'inc',
            'gauge': 'set',
            'histogram': 'observe',
            'summary': 'observe'}
        method = metric_methods[self._metric_configs[name].type]
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        self._logger.debug(
            'updating metric "{}" {}({})'.format(name, method, value))
        metric = self._metrics[name].labels(database=dbname)
        getattr(metric, method)(value)
