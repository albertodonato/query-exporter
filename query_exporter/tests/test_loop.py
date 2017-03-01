import logging

import yaml

from fixtures import LoggerFixture

from prometheus_client import CollectorRegistry

from prometheus_aioexporter.metric import create_metrics

from toolrack.testing import TempDirFixture
from toolrack.testing.async import LoopTestCase

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
                    'interval': '10',
                    'databases': ['db'],
                    'metrics': ['m'],
                    'sql': 'SELECT 1'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config_struct))
        with open(config_file) as fh:
            config = load_config(fh)

        registry = CollectorRegistry(auto_describe=True)
        metrics = create_metrics(config.metrics, registry)
        self.query_loop = QueryLoop(config, metrics, logging, self.loop)

    def mock_run_query(self):
        '''Don't actually run queries.'''
        async def _run_query(*args):
            return

        # don't actually run queries
        self.query_loop._run_query = _run_query

    async def test_start(self):
        '''The start method starts the periodic call.'''
        self.mock_run_query()
        self.query_loop.start()
        self.assertTrue(self.query_loop._periodic_call.running)
        await self.query_loop.stop()

    async def test_stop(self):
        '''The stop method stops the periodic call.'''
        self.mock_run_query()
        self.query_loop.start()
        await self.query_loop.stop()
        self.assertFalse(self.query_loop._periodic_call.running)
