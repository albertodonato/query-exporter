from unittest import TestCase

from psycopg2 import OperationalError, ProgrammingError

from toolrack.testing.async import LoopTestCase

from ..db import DBError, Query, DataBase, DataBaseConnection


class FakeAiopg:

    dsn = None

    def __init__(self, connect_error=None, query_error=None):
        self.connect_error = connect_error
        self.query_error = query_error

    async def create_pool(self, dsn):
        self.dsn = dsn
        if self.connect_error:
            raise OperationalError(self.connect_error)
        return FakePool(dsn, query_error=self.query_error)


class FakePool:

    closed = False

    def __init__(self, dsn, query_error=None):
        self.dsn = dsn
        self.query_error = query_error

    async def acquire(self):
        return FakeConnection(query_error=self.query_error)

    def close(self):
        self.closed = True


class FakeConnection:

    closed = False
    curr = None

    def __init__(self, query_error=None):
        self.query_error = query_error

    async def close(self):
        self.closed = True

    def cursor(self):
        if not self.curr:
            self.curr = FakeCursor()
        return self.curr


class FakeCursor:

    sql = None

    def __init__(self, results=None):
        self.results = results

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def execute(self, sql):
        self.sql = sql

    async def fetchone(self):
        return self.results


class DBErrorTests(TestCase):

    def test_message(self):
        '''The DBError splits error message and details.'''
        error = 'an error happened\nsome more\ndetails here'
        exception = DBError(error)
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
        self.assertEqual(
            query.results((11, 22)), {'metric1': 11, 'metric2': 22})


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
        '''A DBError is raise if database connection fails.'''
        self.connection.aiopg = FakeAiopg(
            connect_error='some error\nmore details')
        with self.assertRaises(DBError) as cm:
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
        async with self.connection:
            self.connection._conn.curr = FakeCursor(results=(10, 20))
            result = await self.connection.execute(query)
        self.assertEqual(result, {'metric1': 10, 'metric2': 20})
