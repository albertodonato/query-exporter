from unittest import TestCase

from toolrack.testing.async import LoopTestCase

from .fakes import FakeAiopg, FakePool, FakeConnection, FakeCursor
from ..db import (
    DataBaseError,
    Query,
    DataBase,
    DataBaseConnection,
    InvalidResultCount)


class DataBaseErrorTests(TestCase):

    def test_message(self):
        '''The DataBaseError splits error message and details.'''
        error = 'an error happened\nsome more\ndetails here'
        exception = DataBaseError(error)
        self.assertEqual(str(exception), 'an error happened')
        self.assertEqual(
            exception.details, ['some more', 'details here'])


class QueryTests(TestCase):

    def test_instantiate(self):
        '''A query can be instantiated with the specified arguments.'''
        query = Query(
            'query', 20, ['db1', 'db2'], ['metric1', 'metric2'], 'SELECT 1')
        self.assertEqual(query.name, 'query')
        self.assertEqual(query.interval, 20)
        self.assertEqual(query.databases, ['db1', 'db2'])
        self.assertEqual(query.metrics, ['metric1', 'metric2'])
        self.assertEqual(query.sql, 'SELECT 1')

    def test_results(self):
        '''The results method returns a dict mapping metrics to results.'''
        query = Query('query', 20, ['db'], ['metric1', 'metric2'], 'SELECT 1')
        rows = [(11, 22), (33, 44)]
        self.assertEqual(
            query.results(rows), {'metric1': (11, 33), 'metric2': (22, 44)})

    def test_results_wrong_result_count(self):
        '''An error is raised if the result column count is wrong.'''
        query = Query('query', 20, ['db'], ['metric'], 'SELECT 1, 2')
        rows = [(1, 2)]
        with self.assertRaises(InvalidResultCount):
            query.results(rows)

    def test_results_empty(self):
        '''No error is raised if the result set is empty'''
        query = Query('query', 20, ['db'], ['metric'], 'SELECT 1, 2')
        rows = []
        self.assertEqual(query.results(rows), {})


class DataBaseTests(TestCase):

    def test_instantiate(self):
        '''A DataBase can be instantiated with the specified arguments.'''
        db = DataBase('db', 'dbname=foo')
        self.assertEqual(db.name, 'db')
        self.assertEqual(db.dsn, 'dbname=foo')

    def test_connect(self):
        '''The connect method returns a DatabaseConnection with same config.'''
        db = DataBase('db', 'dbname=foo')
        connection = db.connect()
        self.assertIsInstance(connection, DataBaseConnection)
        self.assertEqual(connection.name, 'db')
        self.assertEqual(connection.dsn, 'dbname=foo')


class DataBaseConnectionTests(LoopTestCase):

    def setUp(self):
        super().setUp()
        self.connection = DataBaseConnection('db', 'dbname=foo')
        self.connection.aiopg = FakeAiopg()

    def test_instantiate(self):
        '''A DataBaseConnection can be instantiated.'''
        self.assertEqual(self.connection.name, 'db')
        self.assertEqual(self.connection.dsn, 'dbname=foo')

    async def test_context_manager(self):
        '''The DataBaseConnection can be used as context manager.'''
        calls = []

        async def connect():
            calls.append('connect')

        async def close():
            calls.append('close')

        self.connection.connect = connect
        self.connection.close = close
        async with self.connection:
            self.assertEqual(calls, ['connect'])
        self.assertEqual(calls, ['connect', 'close'])

    async def test_connect(self):
        '''The connect method connects to the database.'''
        await self.connection.connect()
        self.assertIsInstance(self.connection._pool, FakePool)
        self.assertEqual(self.connection._pool.dsn, 'dbname=foo')
        self.assertIsInstance(self.connection._conn, FakeConnection)

    async def test_connect_error(self):
        '''A DataBaseError is raise if database connection fails.'''
        self.connection.aiopg = FakeAiopg(
            connect_error='some error\nmore details')
        with self.assertRaises(DataBaseError) as cm:
            await self.connection.connect()
        self.assertEqual(str(cm.exception), 'some error')
        self.assertEqual(cm.exception.details, ['more details'])

    async def test_close(self):
        '''The close method closes database connection and pool.'''
        await self.connection.connect()
        pool = self.connection._pool
        conn = self.connection._conn
        await self.connection.close()
        self.assertTrue(pool.closed)
        self.assertTrue(conn.closed)
        self.assertIsNone(self.connection._pool)
        self.assertIsNone(self.connection._conn)

    async def test_execute(self):
        '''The execute method executes a query.'''
        query = Query('query', 20, ['db'], ['metric1', 'metric2'], 'SELECT 1')
        cursor = FakeCursor(results=[(10, 20), (30, 40)])
        async with self.connection:
            self.connection._conn.curr = cursor
            result = await self.connection.execute(query)
        self.assertEqual(result, {'metric1': (10, 30), 'metric2': (20, 40)})
        self.assertEqual(cursor.sql, 'SELECT 1')

    async def test_execute_query_error(self):
        '''The execute method executes a query.'''
        query = Query('query', 20, ['db'], ['metric'], 'WRONG')
        cursor = FakeCursor(query_error='wrong query\nmore details')
        with self.assertRaises(DataBaseError) as cm:
            async with self.connection:
                self.connection._conn.curr = cursor
                await self.connection.execute(query)
        self.assertEqual(str(cm.exception), 'wrong query')
        self.assertEqual(cm.exception.details, ['more details'])
