import logging

from asynctest import ClockedTestCase
from fixtures import (
    LoggerFixture,
    TestWithFixtures,
)
from prometheus_aioexporter.metric import MetricsRegistry
from toolrack.testing import TempDirFixture
import yaml

from .fakes import FakeSQLAlchemy
from ..config import load_config
from ..loop import QueryLoop


class QueryLoopTests(ClockedTestCase, TestWithFixtures):

    def setUp(self):
        super().setUp()
        self.tempdir = self.useFixture(TempDirFixture())
        self.logger = self.useFixture(LoggerFixture(level=logging.DEBUG))
        self.registry = MetricsRegistry()
        config_struct = {
            'databases': {'db': {'dsn': 'postgres:///foo'}},
            'metrics': {'m': {'type': 'gauge'}},
            'queries': {
                'q': {
                    'interval': 10,
                    'databases': ['db'],
                    'metrics': ['m'],
                    'sql': 'SELECT 1'},
                'q-no-interval': {
                    'databases': ['db'],
                    'metrics': ['m'],
                    'sql': 'SELECT 2'}
            }}
        config_file = self.tempdir.mkfile(content=yaml.dump(config_struct))
        with config_file.open() as fh:
            config = load_config(fh)

        self.registry.create_metrics(config.metrics)
        self.query_loop = QueryLoop(config, self.registry, logging, self.loop)
        self.query_loop._databases['db'].sqlalchemy = FakeSQLAlchemy()

    def mock_execute_query(self):
        """Don't actually execute queries."""
        self.query_exec = []

        async def _execute_query(*args):
            self.query_exec.append(args)

        self.query_loop._execute_query = _execute_query

    async def test_start(self):
        """The start method starts periodic calls for queries."""
        self.mock_execute_query()
        await self.query_loop.start()
        self.addCleanup(self.query_loop.stop)
        [periodic_call] = self.query_loop._periodic_calls
        self.assertTrue(periodic_call.running)

    async def test_stop(self):
        """The stop method stops periodic calls for queries."""
        self.mock_execute_query()
        await self.query_loop.start()
        [periodic_call] = self.query_loop._periodic_calls
        await self.query_loop.stop()
        self.assertFalse(periodic_call.running)

    async def test_run_query(self):
        """Queries are run and update metrics."""
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(100.0,)])
        await self.query_loop.start()
        await self.query_loop.stop()
        metric = self.registry.get_metric('m')
        # the metric is updated
        [(_, _, value)] = metric._samples()
        self.assertEqual(value, 100.0)

    async def test_run_query_null_value(self):
        """A null value in query results is treated like a zero."""
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(None,)])
        await self.query_loop.start()
        await self.query_loop.stop()
        metric = self.registry.get_metric('m')
        [(_, _, value)] = metric._samples()
        self.assertEqual(value, 0)

    async def test_run_query_log(self):
        """Debug messages are logged on query execution."""
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(100.0,)])
        await self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn('running query "q" on database "db"', self.logger.output)
        self.assertIn('updating metric "m" set(100.0)', self.logger.output)

    async def test_run_query_log_error(self):
        """Query errors are logged."""
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(connect_error='error')
        await self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn(
            'query "q" on database "db" failed: error', self.logger.output)

    async def test_run_query_log_invalid_result_count(self):
        """An error is logged if result count doesn't match metrics count."""
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(100.0, 200.0)])
        await self.query_loop.start()
        await self.query_loop.stop()
        self.assertIn(
            'query "q" on database "db" failed: Wrong result count from the'
            ' query',
            self.logger.output)

    async def test_run_query_periodically(self):
        """Queries are run at the specified time interval."""
        self.mock_execute_query()
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(100.0,)])
        await self.query_loop.start()
        await self.advance(0)  # kick the first run
        self.addCleanup(self.query_loop.stop)
        # the query has been run once
        self.assertEqual(len(self.query_exec), 1)
        await self.advance(5)
        # no more runs yet
        self.assertEqual(len(self.query_exec), 1)
        # now the query runs again
        await self.advance(5)
        self.assertEqual(len(self.query_exec), 2)

    async def test_run_aperiodic_queries(self):
        """Queries with null interval can be run explicitly."""
        self.mock_execute_query()
        database = self.query_loop._databases['db']
        database.sqlalchemy = FakeSQLAlchemy(query_results=[(100.0,)])
        await self.query_loop.start()
        self.addCleanup(self.query_loop.stop)
        await self.query_loop.run_aperiodic_queries()
        self.assertEqual(len(self.query_exec), 2)
        await self.query_loop.run_aperiodic_queries()
        self.assertEqual(len(self.query_exec), 3)
