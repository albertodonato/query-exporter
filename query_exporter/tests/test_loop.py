from collections import defaultdict
import logging

from prometheus_aioexporter import MetricsRegistry
import pytest
import yaml

from ..config import load_config
from ..loop import QueryLoop
from .fakes import FakeSQLAlchemy


@pytest.fixture
def config(tmpdir):
    config_data = {
        'databases': {
            'db': {
                'dsn': 'postgres:///foo'
            }
        },
        'metrics': {
            'm': {
                'type': 'gauge'
            }
        },
        'queries': {
            'q': {
                'interval': 10,
                'databases': ['db'],
                'metrics': ['m'],
                'sql': 'SELECT 1'
            },
            'q-no-interval': {
                'databases': ['db'],
                'metrics': ['m'],
                'sql': 'SELECT 2'
            }
        }
    }
    config_file = (tmpdir / 'config.yaml')
    config_file.write_text(yaml.dump(config_data), 'utf-8')
    with config_file.open() as fh:
        config = load_config(fh)
    yield config


@pytest.fixture
def registry(config):
    registry = MetricsRegistry()
    registry.create_metrics(config.metrics)
    yield registry


@pytest.fixture
def fake_sqlalchemy():
    yield FakeSQLAlchemy()


@pytest.fixture
async def query_loop(mocker, event_loop, config, registry, fake_sqlalchemy):
    mocker.patch('query_exporter.db.sqlalchemy', fake_sqlalchemy)
    query_loop = QueryLoop(config, registry, logging, event_loop)
    yield query_loop
    await query_loop.stop()


@pytest.fixture
def mocked_queries(query_loop):
    queries = []

    async def _execute_query(*args):
        queries.append(args)

    query_loop._execute_query = _execute_query
    yield queries


def metric_values(metric, by_labels=()):
    """Return values for the metric."""
    if metric._type == 'gauge':
        suffix = ''
    elif metric._type == 'counter':
        suffix = '_total'

    values = defaultdict(list)
    for sample_suffix, labels, value in metric._samples():
        if sample_suffix == suffix:
            if by_labels:
                label_values = tuple(labels[label] for label in by_labels)
                values[label_values] = value
            else:
                values[sample_suffix].append(value)

    return values if by_labels else values[suffix]


@pytest.mark.asyncio
class TestQueryLoop:

    async def test_start(self, query_loop):
        """The start method starts periodic calls for queries."""
        await query_loop.start()
        # self.addCleanup(self.query_loop.stop)
        [periodic_call] = query_loop._periodic_calls
        assert periodic_call.running

    async def test_stop(self, query_loop):
        """The stop method stops periodic calls for queries."""
        await query_loop.start()
        [periodic_call] = query_loop._periodic_calls
        await query_loop.stop()
        assert not periodic_call.running

    async def test_run_query(self, query_loop, registry, fake_sqlalchemy):
        """Queries are run and update metrics."""
        fake_sqlalchemy.query_results = [(100.0, )]
        await query_loop.start()
        await query_loop.stop()
        # the metric is updated
        metric = registry.get_metric('m')
        assert metric_values(metric) == [100.0]
        # the number of queries is updated
        queries_metric = registry.get_metric('queries')
        assert metric_values(
            queries_metric, by_labels=('status', )) == {
                ('success', ): 1.0
            }

    async def test_run_query_null_value(
            self, query_loop, registry, fake_sqlalchemy):
        """A null value in query results is treated like a ze1ro."""
        fake_sqlalchemy.query_results = [(None, )]
        await query_loop.start()
        await query_loop.stop()
        metric = registry.get_metric('m')
        assert metric_values(metric) == [0]

    async def test_run_query_log(self, caplog, query_loop, fake_sqlalchemy):
        """Debug messages are logged on query execution."""
        caplog.set_level(logging.DEBUG)
        fake_sqlalchemy.query_results = [(100.0, )]
        await query_loop.start()
        await query_loop.stop()
        assert caplog.messages == [
            'connected to database "db"', 'running query "q" on database "db"',
            'updating metric "m" set(100.0)',
            'updating metric "queries" inc(1)'
        ]

    async def test_run_query_log_error(
            self, caplog, query_loop, fake_sqlalchemy):
        """Query errors are logged."""
        caplog.set_level(logging.DEBUG)
        fake_sqlalchemy.connect_error = 'error'
        await query_loop.start()
        await query_loop.stop()
        assert 'query "q" on database "db" failed: error' in caplog.messages

    async def test_run_query_log_invalid_result_count(
            self, caplog, query_loop, registry, fake_sqlalchemy):
        """An error is logged if result count doesn't match metrics count."""
        caplog.set_level(logging.DEBUG)
        fake_sqlalchemy.query_results = [(100.0, 200.0)]
        await query_loop.start()
        await query_loop.stop()
        assert (
            'query "q" on database "db" failed: Wrong result count from the '
            'query' in caplog.messages)

    async def test_run_query_increase_db_erorr_count(
            self, query_loop, registry, fake_sqlalchemy):
        """Query errors are logged."""
        fake_sqlalchemy.connect_error = 'error'
        await query_loop.start()
        await query_loop.stop()
        queries_metric = registry.get_metric('database_errors')
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_error_count(
            self, query_loop, registry, fake_sqlalchemy):
        fake_sqlalchemy.query_results = [(100.0, 200.0)]
        await query_loop.start()
        await query_loop.stop()
        queries_metric = registry.get_metric('queries')
        assert metric_values(
            queries_metric, by_labels=('status', )) == {
                ('error', ): 1.0
            }

    async def test_run_query_periodically(
            self, query_loop, fake_sqlalchemy, mocked_queries, advance_time):
        """Queries are run at the specified time interval."""
        fake_sqlalchemy.query_results = [(100.0, )]
        await query_loop.start()
        await advance_time(0)  # kick the first run
        # the query has been run once
        assert len(mocked_queries) == 1
        await advance_time(5)
        # no more runs yet
        assert len(mocked_queries) == 1
        # now the query runs again
        await advance_time(5)
        assert len(mocked_queries) == 2

    async def test_run_aperiodic_queries(
            self, query_loop, fake_sqlalchemy, mocked_queries):
        """Queries with null interval can be run explicitly."""
        fake_sqlalchemy.query_results = [(100.0, )]
        await query_loop.start()
        await query_loop.run_aperiodic_queries()
        assert len(mocked_queries) == 2
        await query_loop.run_aperiodic_queries()
        assert len(mocked_queries) == 3
