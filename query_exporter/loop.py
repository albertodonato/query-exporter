"""Loop to execute database queries and collect metrics."""

import asyncio
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from logging import Logger
import time
from typing import (
    Any,
    cast,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
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
    DataBaseConnectError,
    DataBaseError,
    Query,
    QueryTimeoutExpired,
)


class MetricsLastSeen:
    """Track last seen times for metrics.

    It assumes labels are sorted by name in metrics.

    """

    def __init__(self, expirations: Dict[str, Optional[int]]):
        self._expirations = expirations
        self._last_seen: Dict[str, Dict[Tuple[str, ...], float]] = defaultdict(dict)

    def update(
        self,
        name: str,
        labels: Dict[str, str],
        timestamp: float,
    ):
        """Update last seen for a metric with a set of labels to given timestamp."""
        if not self._expirations.get(name):
            return

        # sort by label name
        label_values = tuple(value for _, value in sorted(labels.items()))
        self._last_seen[name][label_values] = timestamp

    def expire_series(self, timestamp: float) -> Dict[str, List[Tuple[str, ...]]]:
        """Expire and return expired metric series at the given timestamp.

        Expired series are removed internally.

        Return a dict mapping metric names to a list of tuples of sorted label
        values for expired series.

        """
        expired = {}
        for name, metric_last_seen in self._last_seen.items():
            expiration = cast(int, self._expirations[name])
            expired[name] = [
                label_values
                for label_values, last_seen in metric_last_seen.items()
                if timestamp > last_seen + expiration
            ]
        # clear expired series from tracking
        for name, series_labels in expired.items():
            for label_values in series_labels:
                del self._last_seen[name][label_values]
        return expired


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
        self._last_seen = MetricsLastSeen(
            {
                name: metric.config.get("expiration")
                for name, metric in self._config.metrics.items()
            }
        )
        self._databases: Dict[str, DataBase] = {
            db_config.name: DataBase(db_config, logger=self._logger)
            for db_config in self._config.databases.values()
        }

        for query in self._config.queries.values():
            if query.timed:
                self._timed_queries.append(query)
            else:
                self._aperiodic_queries.append(query)

    async def start(self):
        """Start timed queries execution."""
        for query in self._timed_queries:
            if query.interval:
                call = PeriodicCall(self._run_query, query)
                call.start(query.interval)
            else:
                call = TimedCall(self._run_query, query)
                call.start(self._loop_times_iter(query.schedule))
            self._timed_calls[query.name] = call

    async def stop(self):
        """Stop timed query execution."""
        coros = (call.stop() for call in self._timed_calls.values())
        await asyncio.gather(*coros, return_exceptions=True)
        self._timed_calls.clear()
        coros = (db.close() for db in self._databases.values())
        await asyncio.gather(*coros, return_exceptions=True)

    def clear_expired_series(self):
        """Clear metric series that have expired."""
        expired_series = self._last_seen.expire_series(self._timestamp())
        for name, label_values in expired_series.items():
            metric = self._registry.get_metric(name)
            for values in label_values:
                metric.remove(*values)

    async def run_aperiodic_queries(self):
        """Run queries on request."""
        coros = (
            self._execute_query(query, dbname)
            for query in self._aperiodic_queries
            for dbname in query.databases
        )
        await asyncio.gather(*coros, return_exceptions=True)

    def _loop_times_iter(self, schedule: str):
        """Wrap a croniter iterator to sync time with the loop clock."""
        cron_iter = croniter(schedule, self._now())
        while True:
            cc = next(cron_iter)
            t = time.time()
            delta = cc - t
            yield self._loop.time() + delta

    def _run_query(self, query: Query):
        """Periodic task to run a query."""
        for dbname in query.databases:
            self._loop.create_task(self._execute_query(query, dbname))

    async def _execute_query(self, query: Query, dbname: str):
        """'Execute a Query on a DataBase."""
        if await self._remove_if_dooomed(query, dbname):
            return

        db = self._databases[dbname]
        try:
            metric_results = await db.execute(query)
        except DataBaseConnectError:
            self._increment_db_error_count(db)
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
        all_labels = {DATABASE_LABEL: database.config.name}
        all_labels.update(database.config.labels)
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
        self._last_seen.update(name, all_labels, self._timestamp())

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

    def _now(self) -> datetime:
        """Return the current time with local timezone."""
        return datetime.now().replace(tzinfo=gettz())

    def _timestamp(self) -> float:
        """Return the current timestamp."""
        return self._now().timestamp()
