from unittest import TestCase

from toolrack.testing.async import LoopTestCase

from ..db import Query, DataBase, DataBaseConnection


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
