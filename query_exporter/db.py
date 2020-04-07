"""Database wrapper."""

import asyncio
from itertools import chain
import logging
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy
from sqlalchemy.engine import (
    Connection,
    ResultProxy,
)
from sqlalchemy.exc import (
    ArgumentError,
    NoSuchModuleError,
)
from sqlalchemy_aio import ASYNCIO_STRATEGY
from sqlalchemy_aio.engine import AsyncioEngine

# label used to tag metrics by database
DATABASE_LABEL = "database"


class DataBaseError(Exception):
    """A databease error.

    if `fatal` is True, it means the Query will never succeed.
    """

    def __init__(self, message: str, fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal


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


# database errors that mean the query won't ever succeed.  Not all possible
# fatal errors are tracked here, because some DBAPI errors can happen in
# circumstances which can be fatal or not.  Since there doesn't seem to be a
# reliable way to know, there might be cases when a query will never succeed
# but will end up being retried.
FATAL_ERRORS = (InvalidResultCount, InvalidResultColumnNames)


class QueryMetric(NamedTuple):
    """Metric details for a Query."""

    name: str
    labels: List[str]


class QueryResults(NamedTuple):
    """Results of a database query."""

    keys: List[str]
    rows: List[Tuple]

    @classmethod
    async def from_results(cls, results: ResultProxy):
        """Return a QueryResults from results for a query."""
        return cls(await results.keys(), await results.fetchall())


class MetricResult(NamedTuple):
    """A result for a metric from a query."""

    metric: str
    value: Any
    labels: Dict[str, str]


class Query:
    """Query definition and configuration."""

    def __init__(
        self,
        name: str,
        interval: int,
        databases: List[str],
        metrics: List[QueryMetric],
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.interval = interval
        self.databases = databases
        self.metrics = metrics
        self.sql = sql
        self.parameters = parameters or {}
        self._check_parameters()

    def labels(self) -> FrozenSet[str]:
        """Resturn all labels for metrics in the query."""
        return frozenset(chain(*(metric.labels for metric in self.metrics)))

    def results(self, query_results: QueryResults) -> List[MetricResult]:
        """Return MetricResults from a query."""
        if not query_results.rows:
            return []

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
        return results

    def _check_parameters(self):
        expr = sqlalchemy.text(self.sql)
        query_params = set(expr.compile().params)
        if set(self.parameters) != query_params:
            raise InvalidQueryParameters(self.name)


class DataBase:
    """A database to perform Queries."""

    _engine: AsyncioEngine
    _conn: Optional[Connection] = None
    _logger: logging.Logger = logging.getLogger()
    _pending_queries: int = 0

    def __init__(
        self,
        name: str,
        dsn: str,
        connect_sql: Optional[List[str]] = None,
        keep_connected: Optional[bool] = True,
        autocommit: Optional[bool] = True,
        labels: Optional[Dict[str, str]] = None,
    ):
        self.name = name
        self.dsn = dsn
        self.connect_sql = connect_sql or []
        self.keep_connected = keep_connected
        self.autocommit = autocommit
        self.labels = labels or {}
        self._connect_lock = asyncio.Lock()
        try:
            self._engine = sqlalchemy.create_engine(
                dsn,
                strategy=ASYNCIO_STRATEGY,
                execution_options={"autocommit": self.autocommit},
            )
        except ImportError as error:
            raise self._db_error(f'module "{error.name}" not found', fatal=True)
        except (ArgumentError, ValueError, NoSuchModuleError):
            raise self._db_error(f'Invalid database DSN: "{self.dsn}"', fatal=True)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @property
    def connected(self) -> bool:
        """Whether the database is connected."""
        return self._conn is not None

    def set_logger(self, logger: logging.Logger):
        """Set a logger for the DataBase"""
        self._logger = logger

    async def connect(self):
        """Connect to the database."""
        async with self._connect_lock:
            if self.connected:
                return

            try:
                self._conn = await self._engine.connect()
            except Exception as error:
                raise self._db_error(error)

            self._logger.debug(f'connected to database "{self.name}"')
            for sql in self.connect_sql:
                try:
                    await self.execute_sql(sql)
                except Exception as error:
                    await self._close()
                    raise self._db_error(f'failed executing query "{sql}": {error}')

    async def close(self):
        """Close the database connection."""
        async with self._connect_lock:
            if not self.connected:
                return
            await self._close()

    async def execute(self, query: Query) -> List[MetricResult]:
        """Execute a query."""
        await self.connect()
        self._logger.debug(f'running query "{query.name}" on database "{self.name}"')
        self._pending_queries += 1
        self._conn: Connection
        try:
            result = await self._execute_query(query)
            return query.results(await QueryResults.from_results(result))
        except Exception as error:
            raise self._query_db_error(
                query.name, error, fatal=isinstance(error, FATAL_ERRORS)
            )
        finally:
            assert self._pending_queries >= 0, "pending queries is negative"
            self._pending_queries -= 1
            if not self.keep_connected and not self._pending_queries:
                await self.close()

    async def execute_sql(
        self, sql: str, parameters: Optional[Dict[str, Any]] = None
    ) -> ResultProxy:
        """Execute a raw SQL query."""
        if parameters is None:
            parameters = {}
        self._conn: Connection
        return await self._conn.execute(sqlalchemy.text(sql), parameters)

    async def _execute_query(self, query: Query) -> ResultProxy:
        """Execute a query."""
        return await self.execute_sql(query.sql, parameters=query.parameters)

    async def _close(self):
        await self._conn.close()
        self._conn = None
        self._pending_queries = 0
        self._logger.debug(f'disconnected from database "{self.name}"')

    def _query_db_error(
        self, query_name: str, error: Union[str, Exception], fatal: bool = False,
    ):
        """Create and log a DataBaseError for a failed query."""
        message = str(error).strip()
        self._logger.error(
            f'query "{query_name}" on database "{self.name}" failed: ' + message
        )
        return DataBaseError(message, fatal=fatal)

    def _db_error(self, error: Union[str, Exception], fatal: bool = False):
        """Create and log a DataBaseError."""
        message = str(error).strip()
        self._logger.error(f'error from database "{self.name}": {message}')
        return DataBaseError(message, fatal=fatal)
