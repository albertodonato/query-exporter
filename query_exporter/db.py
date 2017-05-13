from collections import namedtuple

import asyncpg


class DataBaseError(Exception):
    '''A database error.'''

    def __init__(self, error):
        message, *self.details = error.splitlines()
        super().__init__(message)


class InvalidResultCount(Exception):
    '''Number of results from a query don't match metrics count.'''

    def __init__(self):
        super().__init__('Wrong result count from the query')


Query = namedtuple(
    'Query', ['name', 'interval', 'databases', 'metrics', 'sql'])


class Query(Query):

    def results(self, records):
        '''Return a dict with a tuple of values for each metric.'''
        if not records:
            return {}

        if len(self.metrics) != len(records[0]):
            raise InvalidResultCount()
        return dict(zip(self.metrics, zip(*records)))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):
    '''A database to perform Queries.'''

    asyncpg = asyncpg  # for testing

    def connect(self):
        '''Connect to the database.

        It should be used as a context manager and returns a
        DataBaseConnection.

        '''
        connection = DataBaseConnection(self.name, self.dsn)
        connection.asyncpg = self.asyncpg
        return connection


class DataBaseConnection(namedtuple('DataBaseConnection', ['name', 'dsn'])):
    '''A database connection.

    It supports the context manager protocol.

    '''

    asyncpg = asyncpg   # for testing

    _pool = None
    _conn = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        '''Connect to the database.'''
        try:
            self._pool = await self.asyncpg.create_pool(self.dsn)
            self._conn = await self._pool.acquire()
        except Exception as error:
            raise DataBaseError(str(error))

    async def close(self):
        '''Close the database connection.'''
        await self._pool.close()
        self._pool = None
        await self._conn.close()
        self._conn = None

    async def execute(self, query):
        '''Execute a query.'''
        async with self._conn.transaction():
            try:
                records = await self._conn.fetch(query.sql)
                return query.results(records)
            except Exception as error:
                raise DataBaseError(str(error))
