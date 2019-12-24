"""Database wrapper."""

from itertools import chain
import logging
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import sqlalchemy
from sqlalchemy.engine import (
    Engine,
    ResultProxy,
)
from sqlalchemy.engine.url import _parse_rfc1738_args
from sqlalchemy.exc import (
    ArgumentError,
    OperationalError,
)
from sqlalchemy_aio import ASYNCIO_STRATEGY

# the label used to filter metrics by database
DATABASE_LABEL = "database"

# labels that are automatically added to metrics
AUTOMATIC_LABELS = frozenset([DATABASE_LABEL])


class DataBaseError(Exception):
    """A databease error.

    if `fatal` is True, it means the Query will never succeed.
    """

    def __init__(self, message: str, fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal


class InvalidDatabaseDSN(Exception):
    """Database DSN is invalid."""

    def __init__(self, dsn: str):
        super().__init__(f'Invalid database DSN: "{dsn}"')


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


# databse errors that mean the query won't ever succeed
FATAL_ERRORS = (InvalidResultCount, InvalidResultColumnNames, OperationalError)


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


QueryParameters = List[Union[List[Sequence], Dict[str, Any]]]


class Query(NamedTuple):
    """Query definition and configuration."""

    name: str
    interval: int
    databases: List[str]
    metrics: List[QueryMetric]
    sql: str
    parameters: Optional[QueryParameters] = []

    def labels(self) -> FrozenSet[str]:
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
        if labels:
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
        elif set(metrics) == set(query_results.keys):
            # column names match metric names, use column order
            return self._metrics_results(query_results.keys, query_results)
        else:
            # use declared metrics name order
            return self._metrics_results(metrics, query_results)

    def _metrics_results(
        self, metric_names: List[str], query_results: QueryResults
    ) -> List[MetricResult]:
        return [
            MetricResult(name, value, {})
            for row in query_results.rows
            for name, value in zip(metric_names, row)
        ]


class _DataBase(NamedTuple):

    name: str
    dsn: str
    keep_connected: bool = True


class DataBase(_DataBase):
    """A database to perform Queries."""

    _conn: Union[Engine, None] = None
    _logger: logging.Logger = logging.getLogger()
    _pending_queries: int = 0

    async def __aenter__(self):
        return await self.connect()

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
        try:
            engine = sqlalchemy.create_engine(
                self.dsn,
                strategy=ASYNCIO_STRATEGY,
                execution_options={"autocommit": True},
            )
        except ImportError as error:
            raise self._db_error(f'module "{error.name}" not found', fatal=True)

        try:
            self._conn = await engine.connect()
        except Exception as error:
            raise self._db_error(error)
        self._logger.debug(f'connected to database "{self.name}"')
        return self

    async def close(self):
        """Close the database connection."""
        if not self.connected:
            return
        await self._conn.close()
        self._conn = None
        self._pending_queries = 0
        self._logger.debug(f'disconnected from database "{self.name}"')

    async def execute(self, query: Query) -> List[MetricResult]:
        """Execute a query."""
        if not self.connected:
            await self.connect()

        self._logger.debug(f'running query "{query.name}" on database "{self.name}"')
        self._pending_queries += 1
        self._conn: Engine
        try:
            result = await self.execute_sql(query.sql, query.parameters)
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
        self, sql: str, parameters: Optional[QueryParameters] = None
    ) -> ResultProxy:
        """Execute a raw SQL query."""
        if parameters is None:
            parameters = []
        self._conn: Engine
        return await self._conn.execute(sql, parameters)

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


def validate_dsn(dsn: str):
    """Validate a database DSN.

    Raises InvalideDatabaseDSN if invalid.

    """
    try:
        _parse_rfc1738_args(dsn)
    except (ArgumentError, ValueError):
        raise InvalidDatabaseDSN(dsn)
