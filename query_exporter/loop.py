"""Loop to periodically execute queries."""

import asyncio
from logging import Logger
from typing import (
    Any,
    List,
)

from prometheus_aioexporter import MetricsRegistry
from toolrack.aio import PeriodicCall

from .config import Config
from .db import (
    DataBaseError,
    Query,
)


class QueryLoop:
    """Periodically performs queries."""

    _METRIC_METHODS = {
        'counter': 'inc',
        'gauge': 'set',
        'histogram': 'observe',
        'summary': 'observe',
        'enum': 'state'
    }

    def __init__(
            self, config: Config, registry: MetricsRegistry, logger: Logger,
            loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self._logger = logger
        self._registry = registry
        self._periodic_queries: List[Query] = []
        self._aperiodic_queries: List[Query] = []
        self._periodic_calls: List[PeriodicCall] = []
        self._setup(config)

    async def start(self):
        """Start periodic queries execution."""
        for db in self._databases.values():
            try:
                await db.connect()
            except DataBaseError as error:
                self._log_db_error(db.name, error)
            else:
                self._logger.debug(f'connected to database "{db.name}"')

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
            for query in self._aperiodic_queries for dbname in query.databases)
        await asyncio.gather(*coros, loop=self.loop)

    def _setup(self, config: Config):
        """Initialize instance attributes."""
        self._metric_configs = {
            metric_config.name: metric_config
            for metric_config in config.metrics
        }
        self._databases = {
            database.name: database
            for database in config.databases
        }

        for query in config.queries:
            if query.interval is None:
                self._aperiodic_queries.append(query)
            else:
                self._periodic_queries.append(query)

    def _run_query(self, query: Query):
        """Periodic task to run a query."""
        for dbname in query.databases:
            self.loop.create_task(self._execute_query(query, dbname))

    async def _execute_query(self, query: Query, dbname: str):
        """'Execute a Query on a DataBase."""
        self._logger.debug(
            f'running query "{query.name}" on database "{dbname}"')
        try:
            results = await self._databases[dbname].execute(query)
        except DataBaseError as error:
            self._log_query_error(query.name, dbname, error)
            return

        for name, values in results.items():
            for value in values:
                self._update_metric(name, value, dbname)

    def _log_query_error(self, name: str, dbname: str, error: Exception):
        """Log an error related to database query."""
        self._logger.error(
            f'query "{name}" on database "{dbname}" failed: {str(error)}')

    def _log_db_error(self, name: str, error: Exception):
        """Log a failed database query."""
        self._logger.error(f'error from database "{name}": {str(error)}')

    def _update_metric(self, name: str, value: Any, dbname: str):
        """Update value for a metric."""
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        method = self._METRIC_METHODS[self._metric_configs[name].type]
        self._logger.debug(f'updating metric "{name}" {method}({value})')
        metric = self._registry.get_metric(name, labels={'database': dbname})
        getattr(metric, method)(value)
