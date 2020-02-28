"""Loop to periodically execute queries."""

import asyncio
from collections import defaultdict
from decimal import Decimal
from logging import Logger
from typing import (
    Any,
    Dict,
    Iterable,
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
        self._config = config
        self._registry = registry
        self._logger = logger
        self._periodic_queries: List[Query] = []
        self._aperiodic_queries: List[Query] = []
        # map query names to their PeriodicCall
        self._periodic_calls: Dict[str, PeriodicCall] = {}
        # map query names to list of database names
        self._doomed_queries: Dict[str, Set[str]] = defaultdict(set)
        self._setup()

    async def start(self):
        """Start periodic queries execution."""
        for db in self._databases:
            try:
                await db.connect()
            except DataBaseError:
                self._increment_db_error_count(db)

        for query in self._periodic_queries:
            call = PeriodicCall(self.loop, self._run_query, query)
            self._periodic_calls[query.name] = call
            call.start(query.interval)

    async def stop(self):
        """Stop periodic query execution."""
        coros = (call.stop() for call in self._periodic_calls.values())
        await asyncio.gather(*coros, return_exceptions=True)
        self._periodic_calls.clear()
        coros = (db.close() for db in self._databases)
        await asyncio.gather(*coros, return_exceptions=True)

    async def run_aperiodic_queries(self):
        """Run queries that don't have a period set."""
        coros = (
            self._execute_query(query, dbname)
            for query in self._aperiodic_queries
            for dbname in query.databases
        )
        await asyncio.gather(*coros, return_exceptions=True)

    @property
    def _databases(self) -> Iterable[DataBase]:
        """Return an iterable with defined Databases."""
        return self._config.databases.values()

    def _setup(self):
        """Initialize instance attributes."""
        for database in self._databases:
            database.set_logger(self._logger)

        for query in self._config.queries.values():
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

        db = self._config.databases[dbname]
        try:
            results = await db.execute(query)
        except DataBaseError as error:
            self._increment_queries_count(db, "error")
            if error.fatal:
                self._logger.debug(
                    f'removing doomed query "{query.name}" ' f'for database "{dbname}"'
                )
                self._doomed_queries[query.name].add(dbname)
            return

        for result in results:
            self._update_metric(db, result.metric, result.value, labels=result.labels)
        self._increment_queries_count(db, "success")

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
        database: DataBase,
        name: str,
        value: Any,
        labels: Optional[Mapping[str, str]] = None,
    ):
        """Update value for a metric."""
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        elif isinstance(value, Decimal):
            value = float(value)
        method = self._METRIC_METHODS[self._config.metrics[name].type]
        all_labels = {DATABASE_LABEL: database.name}
        all_labels.update(database.labels)
        if labels:
            all_labels.update(labels)
        labels_string = ",".join(
            f'{label}="{value}"' for label, value in sorted(all_labels.items())
        )
        self._logger.debug(
            f'updating metric "{name}" {method} {value} {{{labels_string}}}'
        )
        metric = self._registry.get_metric(name, labels=all_labels)
        getattr(metric, method)(value)

    def _increment_queries_count(self, database: DataBase, status: str):
        """Increment count of queries in a status for a database."""
        self._update_metric(database, QUERIES_METRIC_NAME, 1, labels={"status": status})

    def _increment_db_error_count(self, database: DataBase):
        """Increment number of errors for a database."""
        self._update_metric(database, DB_ERRORS_METRIC_NAME, 1)
