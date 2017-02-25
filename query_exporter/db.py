from collections import namedtuple

import aiopg


class Query(namedtuple('Query', ['name', 'metrics', 'interval', 'sql'])):

    def result(self, row):
        import pprint
        pprint.pprint(dict(zip(self.metrics, row)))


class DataBase(namedtuple('DataBase', ['name', 'dsn'])):

    _pool = None
    _conn = None

    async def connect(self):
        self._pool = await aiopg.create_pool(self.dsn)
        self._conn = await self._pool.acquire()

    async def disconnect(self):
        await self._pool.close()
        self._pool = None
        await self._conn.close()
        self._conn = None

    async def execute(self, query):
        '''Execute a query.'''
        async with self._conn.cursor() as cursor:
            await cursor.execute(query.sql)
            query.result(await cursor.fetchone())
