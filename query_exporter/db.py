"""Database wrapper."""

from collections import namedtuple

import sqlalchemy

from sqlalchemy_aio import ASYNCIO_STRATEGY


class DataBaseError(Exception):
    """A database error."""


class InvalidResultCount(Exception):
    """Number of results from a query don't match metrics count."""

    def __init__(self):
        super().__init__('Wrong result count from the query')


Query = namedtuple(
    'Query', ['name', 'interval', 'databases', 'metrics', 'sql'])


class Query(Query):

    def results(self, records):
        """Return a dict with a tuple of values for each metric."""
        if not records:
            return {}

        if len(self.metrics) != len(records[0]):
            raise InvalidResultCount()
        return dict(zip(self.metrics, zip(*records)))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):
    """A database to perform Queries."""

    sqlalchemy = sqlalchemy  # for testing

    _conn = None

    async def connect(self):
        """Connect to the database."""
        try:
            engine = self.sqlalchemy.create_engine(
                self.dsn, strategy=ASYNCIO_STRATEGY)
        except ImportError as error:
            raise DataBaseError('module "{}" not found'.format(error.name))

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

    async def execute(self, query):
        """Execute a query."""
        if self._conn is None:
            await self.connect()

        try:
            result = await self._conn.execute(query.sql)
            return query.results(await result.fetchall())
        except Exception as error:
            raise DataBaseError(str(error).strip())
