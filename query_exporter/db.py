"""Database wrapper."""

from collections import namedtuple

import asyncpg


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

    asyncpg = asyncpg  # for testing

    _pool = None

    async def connect(self):
        """Connect to the database."""
        try:
            self._pool = await self.asyncpg.create_pool(self.dsn)
        except Exception as error:
            raise DataBaseError(str(error))

    async def close(self):
        """Close the database connection."""
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    async def execute(self, query):
        """Execute a query."""
        if self._pool is None:
            await self.connect()

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                try:
                    rows = await connection.fetch(query.sql)
                    return query.results(rows)
                except Exception as error:
                    raise DataBaseError(str(error))
