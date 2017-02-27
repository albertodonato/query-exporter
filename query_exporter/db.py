from collections import namedtuple

import aiopg


Query = namedtuple(
    'Query', ['name', 'interval', 'databases', 'metrics', 'sql'])


class Query(Query):

    def results(self, row):
        '''Return a dict with metric values from a result row.'''
        return dict(zip(self.metrics, row))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):
    '''A database to perform Queries.'''

    def connect(self):
        '''Connect to the database.

        It should be used as a context manager and returns a
        DataBaseConnection.

        '''
        return DataBaseConnection(self.name, self.dsn)


class DataBaseConnection(namedtuple('DataBaseConnection', ['name', 'dsn'])):
    '''A database connection.

    It supports the context manager protocol.

    '''

    _pool = None
    _conn = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        '''Connect to the database.'''
        self._pool = await aiopg.create_pool(self.dsn)
        self._conn = await self._pool.acquire()

    async def close(self):
        '''Close the database connection.'''
        self._pool.close()
        self._pool = None
        await self._conn.close()
        self._conn = None

    async def execute(self, query):
        '''Execute a query.'''
        async with self._conn.cursor() as cursor:
            await cursor.execute(query.sql)
            return query.results(await cursor.fetchone())
