import logging

import yaml

from fixtures import LoggerFixture

from prometheus_client import CollectorRegistry

from prometheus_aioexporter.metric import create_metrics

from toolrack.testing import TempDirFixture
from toolrack.testing.async import LoopTestCase

from .fakes import FakeAiopg
from ..config import load_config
from ..loop import QueryLoop


class QueryLoopTests(LoopTestCase):

    def setUp(self):
        super().setUp()
        self.tempdir = self.useFixture(TempDirFixture())
        self.logger = self.useFixture(LoggerFixture(level=logging.DEBUG))
        config_struct = {
            'databases': {'db': {'dsn': 'dbname=foo'}},
            'metrics': {'m': {'type': 'gauge'}},
            'queries': {
                'q': {
                    'interval': 10,
                    'databases': ['db'],
                    'metrics': ['m'],
                    'sql': 'SELECT 1'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config_struct))
        with open(config_file) as fh:
            config = load_config(fh)

        registry = CollectorRegistry(auto_describe=True)
        metrics = create_metrics(config.metrics, registry)
        self.query_loop = QueryLoop(config, metrics, logging, self.loop)

    def mock_execute_query(self):
        '''Don't actually execute queries.'''
        self.query_exec = []

        async def _execute_query(*args):
            self.query_exec.append(args)

        self.query_loop._execute_query = _execute_query

    async def test_start(self):
        '''The start method starts periodic calls for queries.'''
        self.mock_execute_query()
        self.query_loop.start()
        [periodic_call] = self.query_loop._periodic_calls
        self.assertTrue(periodic_call.running)
        await self.query_loop.stop()

    async def test_stop(self):
        '''The stop method stops periodic calls for queries.'''
        self.mock_execute_query()
        self.query_loop.start()
        await self.query_loop.stop()
        [periodic_call] = self.query_loop._periodic_calls
        self.assertFalse(periodic_call.running)

    async def test_run_query(self):
        '''Queries are run and update metrics.'''
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(query_results=[(100.0,)])
        self.query_loop.start()
        await self.query_loop.stop()
        metric = self.query_loop._metrics['m']
        # the metric is updated
        [(_, _, value)] = metric._samples()
        self.assertEqual(value, 100.0)

    async def test_run_query_null_value(self):
        '''A null value in query results is treated like a zero.'''
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(query_results=[(None,)])
        self.query_loop.start()
        await self.query_loop.stop()
        metric = self.query_loop._metrics['m']
        [(_, _, value)] = metric._samples()
        self.assertEqual(value, 0)

    async def test_run_query_log(self):
        '''Debug messages are logged on query execution.'''
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(query_results=[(100.0,)])
        self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn("running query 'q' on database 'db'", self.logger.output)
        self.assertIn("updating metric 'm': set 100.0", self.logger.output)

    async def test_run_query_log_error(self):
        '''Query errors are logged.'''
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(connect_error='error\nconnect failed')
        self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn(
            "query 'q' failed: error\nconnect failed\n", self.logger.output)

    async def test_run_query_log_invalid_result_count(self):
        '''An error is logged if result count doesn't match metrics count.'''
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(query_results=[(100.0, 200.0)])
        self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn(
            "query 'q' failed: Wrong result count from the query",
            self.logger.output)

    async def test_run_query_periodically(self):
        '''Queies are run at the specified time interval.'''
        self.mock_execute_query()
        database = self.query_loop._databases['db']
        database.aiopg = FakeAiopg(query_results=[(100.0,)])
        self.query_loop.start()
        # the query has been run once
        self.assertEqual(len(self.query_exec), 1)
        self.loop.advance(5)
        # no more runs yet
        self.assertEqual(len(self.query_exec), 1)
        # now the query runs again
        self.loop.advance(5)
        self.assertEqual(len(self.query_exec), 2)
        await self.query_loop.stop()
