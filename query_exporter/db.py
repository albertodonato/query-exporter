from collections import namedtuple

import aiopg


class Query(namedtuple('Query', ['name', 'interval', 'metrics', 'sql'])):

    def results(self, row):
        '''Return a dict with metric values from a result row.'''
        return dict(zip(self.metrics, row))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):

    _pool = None
    _conn = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        self._pool = await aiopg.create_pool(self.dsn)
        self._conn = await self._pool.acquire()

    async def close(self):
        self._pool.close()
        self._pool = None
        await self._conn.close()
        self._conn = None

    async def execute(self, query):
        '''Execute a query.'''
        async with self._conn.cursor() as cursor:
            await cursor.execute(query.sql)
            return query.results(await cursor.fetchone())
