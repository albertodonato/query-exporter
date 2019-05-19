"""Loop to periodically execute queries."""

import asyncio
from logging import Logger
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
)

from prometheus_aioexporter import MetricsRegistry
from toolrack.aio import PeriodicCall

from .config import (
    Config,
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
)
from .db import (
    DATABASE_LABEL,
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
        # map query names to their PeriodicCall
        self._periodic_calls: Dict[str, PeriodicCall] = {}
        self._doomed_queries: Set[str] = set()
        self._setup(config)

    async def start(self):
        """Start periodic queries execution."""
        for db in self._databases.values():
            try:
                await db.connect(loop=self.loop)
            except DataBaseError as error:
                self._log_db_error(db.name, error)
                self._increment_db_error_count(db.name)
            else:
                self._logger.debug(f'connected to database "{db.name}"')

        for query in self._periodic_queries:
            call = PeriodicCall(self.loop, self._run_query, query)
            self._periodic_calls[query.name] = call
            call.start(query.interval)

    async def stop(self):
        """Stop periodic query execution."""
        coros = (call.stop() for call in self._periodic_calls.values())
        await asyncio.gather(*coros, loop=self.loop)
        self._periodic_calls = {}
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
        if await self._remove_if_dooomed(query):
            return

        self._logger.debug(
            f'running query "{query.name}" on database "{dbname}"')
        try:
            results = await self._databases[dbname].execute(query)
        except DataBaseError as error:
            self._log_query_error(query.name, dbname, error)
            self._increment_queries_count(dbname, 'error')
            if error.fatal:
                self._logger.debug(f'removing doomed query "{query.name}"')
                self._doomed_queries.add(query.name)
            return

        for result in results:
            self._update_metric(
                result.metric, result.value, dbname, labels=result.labels)
        self._increment_queries_count(dbname, 'success')

    async def _remove_if_dooomed(self, query: Query) -> bool:
        """Remove a query if it will never work.

        Return whether the query has been removed.

        """
        if query.name not in self._doomed_queries:
            return False

        self._doomed_queries.remove(query.name)
        if query.interval is None:
            self._aperiodic_queries.remove(query)
        else:
            self._periodic_queries.remove(query)
            call = self._periodic_calls.pop(query.name, None)
            if call is not None:
                await call.stop()
        return True

    def _log_query_error(self, name: str, dbname: str, error: Exception):
        """Log an error related to database query."""
        self._logger.error(
            f'query "{name}" on database "{dbname}" failed: {str(error)}')

    def _log_db_error(self, dbname: str, error: Exception):
        """Log a failed database query."""
        self._logger.error(f'error from database "{dbname}": {str(error)}')

    def _update_metric(
            self,
            name: str,
            value: Any,
            dbname: str,
            labels: Optional[Mapping[str, str]] = None):
        """Update value for a metric."""
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        method = self._METRIC_METHODS[self._metric_configs[name].type]
        self._logger.debug(f'updating metric "{name}" {method}({value})')
        all_labels = {DATABASE_LABEL: dbname}
        if labels:
            all_labels.update(labels)
        metric = self._registry.get_metric(name, all_labels)
        getattr(metric, method)(value)

    def _increment_queries_count(self, dbname: str, status: str):
        """Increment count of queries in a status for a database."""
        self._update_metric(
            QUERIES_METRIC_NAME, 1, dbname, labels={'status': status})

    def _increment_db_error_count(self, dbname: str):
        """Increment number of errors for a database."""
        self._update_metric(DB_ERRORS_METRIC_NAME, 1, dbname)
