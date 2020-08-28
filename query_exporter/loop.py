"""Loop to execute database queries and collect metrics."""

import asyncio
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from logging import Logger
import time
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
)

from croniter import croniter
from dateutil.tz import gettz
from prometheus_aioexporter import MetricsRegistry
from toolrack.aio import (
    PeriodicCall,
    TimedCall,
)

from .config import (
    Config,
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
    QUERY_LATENCY_METRIC_NAME,
)
from .db import (
    DataBase,
    DATABASE_LABEL,
    DataBaseError,
    Query,
    QueryTimeoutExpired,
)


class QueryLoop:
    """Run database queries and collect metrics."""

    _METRIC_METHODS = {
        "counter": "inc",
        "gauge": "set",
        "histogram": "observe",
        "summary": "observe",
        "enum": "state",
    }

    def __init__(
        self,
        config: Config,
        registry: MetricsRegistry,
        logger: Logger,
    ):
        self._config = config
        self._registry = registry
        self._logger = logger
        self._timed_queries: List[Query] = []
        self._aperiodic_queries: List[Query] = []
        # map query names to their TimedCalls
        self._timed_calls: Dict[str, TimedCall] = {}
        # map query names to list of database names
        self._doomed_queries: Dict[str, Set[str]] = defaultdict(set)
        self._loop = asyncio.get_event_loop()
        self._setup()

    async def start(self):
        """Start timed queries execution."""
        for db in self._databases:
            try:
                await db.connect()
            except DataBaseError:
                self._increment_db_error_count(db)

        for query in self._timed_queries:
            if query.interval:
                call = PeriodicCall(self._run_query, query)
                call.start(query.interval)
            else:
                call = TimedCall(self._run_query, query)
                now = datetime.now().replace(tzinfo=gettz())
                cron_iter = croniter(query.schedule, now)

                def times_iter():
                    while True:
                        delta = next(cron_iter) - time.time()
                        yield self._loop.time() + delta

                call.start(times_iter())
            self._timed_calls[query.name] = call

    async def stop(self):
        """Stop timed query execution."""
        coros = (call.stop() for call in self._timed_calls.values())
        await asyncio.gather(*coros, return_exceptions=True)
        self._timed_calls.clear()
        coros = (db.close() for db in self._databases)
        await asyncio.gather(*coros, return_exceptions=True)

    async def run_aperiodic_queries(self):
        """Run queries on request."""
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
            if query.timed:
                self._timed_queries.append(query)
            else:
                self._aperiodic_queries.append(query)

    def _run_query(self, query: Query):
        """Periodic task to run a query."""
        for dbname in query.databases:
            self._loop.create_task(self._execute_query(query, dbname))

    async def _execute_query(self, query: Query, dbname: str):
        """'Execute a Query on a DataBase."""
        if await self._remove_if_dooomed(query, dbname):
            return

        db = self._config.databases[dbname]
        try:
            metric_results = await db.execute(query)
        except QueryTimeoutExpired:
            self._increment_queries_count(db, query, "timeout")
        except DataBaseError as error:
            self._increment_queries_count(db, query, "error")
            if error.fatal:
                self._logger.debug(
                    f'removing doomed query "{query.name}" ' f'for database "{dbname}"'
                )
                self._doomed_queries[query.name].add(dbname)
        else:
            for result in metric_results.results:
                self._update_metric(
                    db, result.metric, result.value, labels=result.labels
                )
            if metric_results.latency:
                self._update_query_latency_metric(db, query, metric_results.latency)
            self._increment_queries_count(db, query, "success")

    async def _remove_if_dooomed(self, query: Query, dbname: str) -> bool:
        """Remove a query if it will never work.

        Return whether the query has been removed for the database.

        """
        if dbname not in self._doomed_queries[query.name]:
            return False

        if set(query.databases) == self._doomed_queries[query.name]:
            # the query has failed on all databases
            if query.timed:
                self._timed_queries.remove(query)
                call = self._timed_calls.pop(query.name, None)
                if call is not None:
                    await call.stop()
            else:
                self._aperiodic_queries.remove(query)
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

    def _increment_queries_count(self, database: DataBase, query: Query, status: str):
        """Increment count of queries in a status for a database."""
        self._update_metric(
            database,
            QUERIES_METRIC_NAME,
            1,
            labels={"query": query.config_name, "status": status},
        )

    def _increment_db_error_count(self, database: DataBase):
        """Increment number of errors for a database."""
        self._update_metric(database, DB_ERRORS_METRIC_NAME, 1)

    def _update_query_latency_metric(
        self, database: DataBase, query: Query, latency: float
    ):
        """Update latency metric for a query on a database."""
        self._update_metric(
            database,
            QUERY_LATENCY_METRIC_NAME,
            latency,
            labels={"query": query.config_name},
        )
