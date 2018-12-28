"""Database wrapper."""

from collections import namedtuple
from typing import (
    Dict,
    List,
    NamedTuple,
    Tuple,
    Union,
)

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy_aio import ASYNCIO_STRATEGY


class DataBaseError(Exception):
    """A database error."""


class InvalidResultCount(Exception):
    """Number of results from a query don't match metrics count."""

    def __init__(self):
        super().__init__('Wrong result count from the query')


class Query(NamedTuple):
    """Query configuration and definition."""

    name: str
    interval: int
    databases: List[str]
    metrics: List[str]
    sql: str

    def results(self, records: List[Tuple]) -> Dict[str, Tuple]:
        """Return a dict with a tuple of values for each metric."""
        if not records:
            return {}

        if len(self.metrics) != len(records[0]):
            raise InvalidResultCount()
        return dict(zip(self.metrics, zip(*records)))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):
    """A database to perform Queries."""

    _conn: Union[Engine, None] = None

    async def connect(self):
        """Connect to the database."""
        try:
            engine = sqlalchemy.create_engine(
                self.dsn, strategy=ASYNCIO_STRATEGY)
        except ImportError as error:
            raise DataBaseError(f'module "{error.name}" not found')

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

    async def execute(self, query: Query):
        """Execute a query."""
        if self._conn is None:
            await self.connect()

        self._conn: Engine
        try:
            result = await self._conn.execute(query.sql)
            return query.results(await result.fetchall())
        except Exception as error:
            raise DataBaseError(str(error).strip())
