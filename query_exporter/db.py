"""Database wrapper."""

import asyncio
from itertools import chain
import logging
import sys
from time import perf_counter
from traceback import format_tb
from typing import (
    Any,
    cast,
    Dict,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
)

from croniter import croniter
from sqlalchemy import (
    create_engine,
    event,
    text,
)
from sqlalchemy.exc import (
    ArgumentError,
    NoSuchModuleError,
)
from sqlalchemy_aio import ASYNCIO_STRATEGY
from sqlalchemy_aio.asyncio import AsyncioEngine
from sqlalchemy_aio.base import (
    AsyncConnection,
    AsyncResultProxy,
)

#: Timeout for a query
QueryTimeout = Union[int, float]


#: Label used to tag metrics by database
DATABASE_LABEL = "database"


class DataBaseError(Exception):
    """A databease error.

    if `fatal` is True, it means the Query will never succeed.
    """

    def __init__(self, message: str, fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal


class DataBaseConnectError(DataBaseError):
    """Database connection error."""


class DataBaseQueryError(DataBaseError):
    """Database query error."""


class QueryTimeoutExpired(Exception):
    """Query execution timeout expired."""

    def __init__(self, query_name: str, timeout: QueryTimeout):
        super().__init__(
            f'Execution for query "{query_name}" expired after {timeout} seconds'
        )


class InvalidResultCount(Exception):
    """Number of results from a query don't match metrics count."""

    def __init__(self, expected: int, got: int):
        super().__init__(
            f"Wrong result count from query: expected {expected}, got {got}"
        )


class InvalidResultColumnNames(Exception):
    """Invalid column names in query results."""

    def __init__(self):
        super().__init__("Wrong column names from query")


class InvalidQueryParameters(Exception):
    """Query parameter names don't match those in query SQL."""

    def __init__(self, query_name: str):
        super().__init__(
            f'Parameters for query "{query_name}" don\'t match those from SQL'
        )


class InvalidQuerySchedule(Exception):
    """Query schedule is wrong or both schedule and interval specified."""

    def __init__(self, query_name: str, message: str):
        super().__init__(f'Invalid schedule for query "{query_name}": {message}')


# database errors that mean the query won't ever succeed.  Not all possible
# fatal errors are tracked here, because some DBAPI errors can happen in
# circumstances which can be fatal or not.  Since there doesn't seem to be a
# reliable way to know, there might be cases when a query will never succeed
# but will end up being retried.
FATAL_ERRORS = (InvalidResultCount, InvalidResultColumnNames)


def create_db_engine(dsn: str, **kwargs) -> AsyncioEngine:
    """Create the database engine, validating the DSN"""
    try:
        return create_engine(dsn, **kwargs)
    except ImportError as error:
        raise DataBaseError(f'module "{error.name}" not found')
    except (ArgumentError, ValueError, NoSuchModuleError):
        raise DataBaseError(f'Invalid database DSN: "{dsn}"')


class QueryMetric(NamedTuple):
    """Metric details for a Query."""

    name: str
    labels: List[str]


class QueryResults(NamedTuple):
    """Results of a database query."""

    keys: List[str]
    rows: List[Tuple]
    latency: Optional[float] = None

    @classmethod
    async def from_results(cls, results: AsyncResultProxy):
        """Return a QueryResults from results for a query."""
        conn_info = results._result_proxy.connection.info
        latency = conn_info.get("query_latency", None)
        return cls(await results.keys(), await results.fetchall(), latency=latency)


class MetricResult(NamedTuple):
    """A result for a metric from a query."""

    metric: str
    value: Any
    labels: Dict[str, str]


class MetricResults(NamedTuple):
    """Collection of metric results for a query."""

    results: List[MetricResult]
    latency: Optional[float] = None


class Query:
    """Query definition and configuration."""

    def __init__(
        self,
        name: str,
        databases: List[str],
        metrics: List[QueryMetric],
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: Optional[QueryTimeout] = None,
        interval: Optional[int] = None,
        schedule: Optional[str] = None,
        config_name: Optional[str] = None,
    ):
        self.name = name
        self.databases = databases
        self.metrics = metrics
        self.sql = sql
        self.parameters = parameters or {}
        self.timeout = timeout
        self.interval = interval
        self.schedule = schedule
        self.config_name = config_name or name
        self._check_schedule()
        self._check_query_parameters()

    @property
    def timed(self) -> bool:
        """Whether the query is run periodically via interval or schedule."""
        return bool(self.interval or self.schedule)

    def labels(self) -> FrozenSet[str]:
        """Resturn all labels for metrics in the query."""
        return frozenset(chain(*(metric.labels for metric in self.metrics)))

    def results(self, query_results: QueryResults) -> MetricResults:
        """Return MetricResults from a query."""
        if not query_results.rows:
            return MetricResults([])

        result_keys = sorted(query_results.keys)
        labels = self.labels()
        metrics = [metric.name for metric in self.metrics]
        expected_keys = sorted(set(metrics) | labels)
        if len(expected_keys) != len(result_keys):
            raise InvalidResultCount(len(expected_keys), len(result_keys))
        if result_keys != expected_keys:
            raise InvalidResultColumnNames()
        results = []
        for row in query_results.rows:
            values = dict(zip(query_results.keys, row))
            for metric in self.metrics:
                metric_result = MetricResult(
                    metric.name,
                    values[metric.name],
                    {label: values[label] for label in metric.labels},
                )
                results.append(metric_result)

        return MetricResults(results, latency=query_results.latency)

    def _check_schedule(self):
        if self.interval and self.schedule:
            raise InvalidQuerySchedule(
                self.name, "both interval and schedule specified"
            )
        if self.schedule and not croniter.is_valid(self.schedule):
            raise InvalidQuerySchedule(self.name, "invalid schedule format")

    def _check_query_parameters(self):
        expr = text(self.sql)
        query_params = set(expr.compile().params)
        if set(self.parameters) != query_params:
            raise InvalidQueryParameters(self.name)


class DataBase:
    """A database to perform Queries."""

    _engine: AsyncioEngine
    _conn: Optional[AsyncConnection] = None
    _pending_queries: int = 0

    def __init__(
        self,
        config,
        logger: logging.Logger = logging.getLogger(),
    ):
        self.config = config
        self.logger = logger
        self._connect_lock = asyncio.Lock()
        self._engine = create_db_engine(
            self.config.dsn,
            strategy=ASYNCIO_STRATEGY,
            execution_options={"autocommit": self.config.autocommit},
        )

        # XXX workaround https://github.com/RazerM/sqlalchemy_aio/pull/37
        self._engine.hide_parameters = self._engine.sync_engine.hide_parameters
        self._engine.url = self._engine.sync_engine.url

        self._setup_query_latency_tracking()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @property
    def connected(self) -> bool:
        """Whether the database is connected."""
        return self._conn is not None

    async def connect(self):
        """Connect to the database."""
        async with self._connect_lock:
            if self.connected:
                return

            try:
                self._conn = await self._engine.connect()
            except Exception as error:
                raise self._db_error(error, exc_class=DataBaseConnectError)

            self.logger.debug(f'connected to database "{self.config.name}"')
            for sql in self.config.connect_sql:
                try:
                    await self.execute_sql(sql)
                except Exception as error:
                    await self._close()
                    raise self._db_error(
                        f'failed executing query "{sql}": {error}',
                        exc_class=DataBaseQueryError,
                    )

    async def close(self):
        """Close the database connection."""
        async with self._connect_lock:
            if not self.connected:
                return
            await self._close()

    async def execute(self, query: Query) -> MetricResults:
        """Execute a query."""
        await self.connect()
        self.logger.debug(
            f'running query "{query.name}" on database "{self.config.name}"'
        )
        self._pending_queries += 1
        self._conn: AsyncConnection
        try:
            result = await self._execute_query(query)
            return query.results(await QueryResults.from_results(result))
        except asyncio.TimeoutError:
            raise self._query_timeout_error(
                query.name, cast(QueryTimeout, query.timeout)
            )
        except Exception as error:
            raise self._query_db_error(
                query.name, error, fatal=isinstance(error, FATAL_ERRORS)
            )
        finally:
            assert self._pending_queries >= 0, "pending queries is negative"
            self._pending_queries -= 1
            if not self.config.keep_connected and not self._pending_queries:
                await self.close()

    async def execute_sql(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: Optional[QueryTimeout] = None,
    ) -> AsyncResultProxy:
        """Execute a raw SQL query."""
        if parameters is None:
            parameters = {}
        self._conn: AsyncConnection
        return await asyncio.wait_for(
            self._conn.execute(text(sql), parameters),
            timeout=timeout,
        )

    async def _execute_query(self, query: Query) -> AsyncResultProxy:
        """Execute a query."""
        return await self.execute_sql(
            query.sql, parameters=query.parameters, timeout=query.timeout
        )

    async def _close(self):
        # ensure the connection with the DB is actually closed
        self._conn.sync_connection.detach()
        await self._conn.close()
        self._conn = None
        self._pending_queries = 0
        self.logger.debug(f'disconnected from database "{self.config.name}"')

    def _setup_query_latency_tracking(self):
        engine = self._engine.sync_engine

        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            conn.info["query_start_time"] = perf_counter()

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            conn.info["query_latency"] = perf_counter() - conn.info.pop(
                "query_start_time"
            )

    def _query_db_error(
        self,
        query_name: str,
        error: Union[str, Exception],
        fatal: bool = False,
    ) -> DataBaseError:
        """Create and log a DataBaseError for a failed query."""
        message = self._error_message(error)
        self.logger.error(
            f'query "{query_name}" on database "{self.config.name}" failed: ' + message
        )
        _, _, traceback = sys.exc_info()
        self.logger.debug("".join(format_tb(traceback)))
        return DataBaseQueryError(message, fatal=fatal)

    def _query_timeout_error(
        self, query_name: str, timeout: QueryTimeout
    ) -> QueryTimeoutExpired:
        error = QueryTimeoutExpired(query_name, timeout)
        self.logger.warning(str(error))
        raise error

    def _db_error(
        self,
        error: Union[str, Exception],
        exc_class: Type[DataBaseError] = DataBaseError,
        fatal: bool = False,
    ) -> DataBaseError:
        """Create and log a DataBaseError."""
        message = self._error_message(error)
        self.logger.error(f'error from database "{self.config.name}": {message}')
        return exc_class(message, fatal=fatal)

    def _error_message(self, error: Union[str, Exception]) -> str:
        """Return a message from an error."""
        message = str(error).strip()
        if not message and isinstance(error, Exception):
            message = error.__class__.__name__
        return message
