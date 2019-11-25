"""Loop to periodically execute queries."""

import asyncio
from collections import defaultdict
from logging import Logger
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
)

from prometheus_aioexporter import (
    MetricConfig,
    MetricsRegistry,
)
from toolrack.aio import PeriodicCall

from .config import (
    Config,
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
)
from .db import (
    DataBase,
    DATABASE_LABEL,
    DataBaseError,
    Query,
)


class QueryLoop:
    """Periodically performs queries."""

    _METRIC_METHODS = {
        "counter": "inc",
        "gauge": "set",
        "histogram": "observe",
        "summary": "observe",
        "enum": "state",
    }

    def __init__(
        self, config: Config, registry: MetricsRegistry, logger: Logger,
    ):
        self.loop = asyncio.get_event_loop()
        self._logger = logger
        self._metric_configs: Dict[str, MetricConfig] = {}
        self._databases: Dict[str, DataBase] = {}
        self._registry = registry
        self._periodic_queries: List[Query] = []
        self._aperiodic_queries: List[Query] = []
        # map query names to their PeriodicCall
        self._periodic_calls: Dict[str, PeriodicCall] = {}
        # map query names to list of database names
        self._doomed_queries: Dict[str, Set[str]] = defaultdict(set)
        self._setup(config)

    async def start(self):
        """Start periodic queries execution."""
        for db in self._databases.values():
            try:
                await db.connect()
            except DataBaseError:
                self._increment_db_error_count(db.name)

        for query in self._periodic_queries:
            call = PeriodicCall(self.loop, self._run_query, query)
            self._periodic_calls[query.name] = call
            call.start(query.interval)

    async def stop(self):
        """Stop periodic query execution."""
        coros = (call.stop() for call in self._periodic_calls.values())
        await asyncio.gather(*coros, return_exceptions=True)
        self._periodic_calls.clear()
        coros = (db.close() for db in self._databases.values())
        await asyncio.gather(*coros, return_exceptions=True)

    async def run_aperiodic_queries(self):
        """Run queries that don't have a period set."""
        coros = (
            self._execute_query(query, dbname)
            for query in self._aperiodic_queries
            for dbname in query.databases
        )
        await asyncio.gather(*coros, return_exceptions=True)

    def _setup(self, config: Config):
        """Initialize instance attributes."""
        self._metric_configs = {
            metric_config.name: metric_config for metric_config in config.metrics
        }
        for database in config.databases:
            database.set_logger(self._logger)
            self._databases[database.name] = database

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
        if await self._remove_if_dooomed(query, dbname):
            return

        try:
            results = await self._databases[dbname].execute(query)
        except DataBaseError as error:
            self._increment_queries_count(dbname, "error")
            if error.fatal:
                self._logger.debug(
                    f'removing doomed query "{query.name}" ' f'for database "{dbname}"'
                )
                self._doomed_queries[query.name].add(dbname)
            return

        for result in results:
            self._update_metric(
                result.metric, result.value, dbname, labels=result.labels
            )
        self._increment_queries_count(dbname, "success")

    async def _remove_if_dooomed(self, query: Query, dbname: str) -> bool:
        """Remove a query if it will never work.

        Return whether the query has been removed for the database.

        """
        if dbname not in self._doomed_queries[query.name]:
            return False

        if set(query.databases) == self._doomed_queries[query.name]:
            # the query has failed on all databases
            if query.interval is None:
                self._aperiodic_queries.remove(query)
            else:
                self._periodic_queries.remove(query)
                call = self._periodic_calls.pop(query.name, None)
                if call is not None:
                    await call.stop()
        return True

    def _update_metric(
        self,
        name: str,
        value: Any,
        dbname: str,
        labels: Optional[Mapping[str, str]] = None,
    ):
        """Update value for a metric."""
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        method = self._METRIC_METHODS[self._metric_configs[name].type]
        all_labels = {DATABASE_LABEL: dbname}
        if labels:
            all_labels.update(labels)
        labels_string = ",".join(
            f'{label}="{value}"' for label, value in sorted(all_labels.items())
        )
        self._logger.debug(
            f'updating metric "{name}" {method} {value} {{{labels_string}}}'
        )
        metric = self._registry.get_metric(name, all_labels)
        getattr(metric, method)(value)

    def _increment_queries_count(self, dbname: str, status: str):
        """Increment count of queries in a status for a database."""
        self._update_metric(QUERIES_METRIC_NAME, 1, dbname, labels={"status": status})

    def _increment_db_error_count(self, dbname: str):
        """Increment number of errors for a database."""
        self._update_metric(DB_ERRORS_METRIC_NAME, 1, dbname)
