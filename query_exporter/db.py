"""Database wrapper."""

import asyncio
from collections import namedtuple
from typing import (
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy
from sqlalchemy.engine import (
    Engine,
    ResultProxy,
)
from sqlalchemy.engine.url import _parse_rfc1738_args
from sqlalchemy.exc import ArgumentError
from sqlalchemy_aio import ASYNCIO_STRATEGY


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
        super().__init__(f"Invalid database DSN: '{dsn}'")


class InvalidResultCount(Exception):
    """Number of results from a query don't match metrics count."""

    def __init__(self):
        super().__init__('Wrong result count from query')


class QueryResults(NamedTuple):
    """Results of a database query."""

    keys: List[str]
    rows: List[Tuple]

    @classmethod
    async def from_results(cls, results: ResultProxy):
        """Return a QueryResults from results for a query."""
        return cls(await results.keys(), await results.fetchall())


# Result values for metrics from a query
MetricsResults = Dict[str, Tuple]


class Query(NamedTuple):
    """Query configuration and definition."""

    name: str
    interval: int
    databases: List[str]
    metrics: List[str]
    sql: str

    def results(self, query_results: QueryResults) -> MetricsResults:
        """Return a dict with a tuple of values for each metric."""
        if not query_results.rows:
            return {}

        if len(self.metrics) != len(query_results.keys):
            raise InvalidResultCount()
        if set(self.metrics) == set(query_results.keys):
            # column names match metric names, use column order
            metric_names = query_results.keys
        else:
            # use declared metrics name order
            metric_names = self.metrics
        return dict(zip(metric_names, zip(*query_results.rows)))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):
    """A database to perform Queries."""

    _conn: Union[Engine, None] = None

    async def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """Connect to the database."""
        try:
            engine = sqlalchemy.create_engine(
                self.dsn, strategy=ASYNCIO_STRATEGY, loop=loop)
        except ImportError as error:
            raise DataBaseError(f'module "{error.name}" not found', fatal=True)

        try:
            self._conn = await engine.connect()
        except Exception as error:
            raise DataBaseError(str(error).strip())

    async def close(self):
        """Close the database connection."""
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None

    async def execute(self, query: Query) -> MetricsResults:
        """Execute a query."""
        if self._conn is None:
            await self.connect()

        self._conn: Engine
        try:
            result = await self._conn.execute(query.sql)
            return query.results(await QueryResults.from_results(result))
        except Exception as error:
            fatal = isinstance(error, InvalidResultCount)
            raise DataBaseError(str(error).strip(), fatal=fatal)


def validate_dsn(dsn: str):
    """Validate a database DSN.

    Raises InvalideDatabaseDSN if invalid.

    """
    try:
        _parse_rfc1738_args(dsn)
    except (ArgumentError, ValueError):
        raise InvalidDatabaseDSN(dsn)
