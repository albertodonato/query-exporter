import asyncio
from collections import defaultdict
import logging

from prometheus_aioexporter import MetricsRegistry
import yaml

import pytest

from ..config import load_config
from ..db import DataBase
from ..loop import QueryLoop


@pytest.fixture
def config_data():
    yield {
        "databases": {"db": {"dsn": "sqlite://"}},
        "metrics": {"m": {"type": "gauge"}},
        "queries": {
            "q": {
                "interval": 10,
                "databases": ["db"],
                "metrics": ["m"],
                "sql": "SELECT 100.0",
            },
        },
    }


@pytest.fixture
def registry():
    yield MetricsRegistry()


@pytest.fixture
async def make_query_loop(tmpdir, config_data, registry):
    query_loops = []

    def make_loop():
        config_file = tmpdir / "config.yaml"
        config_file.write_text(yaml.dump(config_data), "utf-8")
        with config_file.open() as fh:
            config = load_config(fh)
        registry.create_metrics(config.metrics)
        query_loop = QueryLoop(config, registry, logging)
        query_loops.append(query_loop)
        return query_loop

    yield make_loop
    await asyncio.gather(
        *(query_loop.stop() for query_loop in query_loops), return_exceptions=True,
    )


@pytest.fixture
async def query_loop(make_query_loop):
    yield make_query_loop()


def metric_values(metric, by_labels=()):
    """Return values for the metric."""
    if metric._type == "gauge":
        suffix = ""
    elif metric._type == "counter":
        suffix = "_total"

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
        periodic_call = query_loop._periodic_calls["q"]
        assert periodic_call.running

    async def test_stop(self, query_loop):
        """The stop method stops periodic calls for queries."""
        await query_loop.start()
        periodic_call = query_loop._periodic_calls["q"]
        await query_loop.stop()
        assert not periodic_call.running

    async def test_run_query(self, query_tracker, query_loop, registry):
        """Queries are run and update metrics."""
        await query_loop.start()
        await query_tracker.wait_results()
        # the metric is updated
        metric = registry.get_metric("m")
        assert metric_values(metric) == [100.0]
        # the number of queries is updated
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("success",): 1.0
        }

    @pytest.mark.parametrize(
        "query,params",
        [
            ("SELECT :param AS m", [{"param": 10.0}, {"param": 20.0}]),
            ("SELECT ? AS m", [[10.0], [20.0]]),
        ],
    )
    async def test_run_query_with_parameters(
        self, query_tracker, registry, config_data, make_query_loop, query, params,
    ):
        """Queries are run with declared parameters."""
        config_data["metrics"]["m"]["type"] = "counter"
        config_data["queries"]["q"]["sql"] = query
        config_data["queries"]["q"]["parameters"] = params
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        # the metric is updated
        metric = registry.get_metric("m")
        # the sum is recorded
        assert metric_values(metric) == [30.0]
        # the number of queries is updated
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("success",): 2.0
        }

    async def test_run_query_null_value(
        self, query_tracker, registry, config_data, make_query_loop
    ):
        """A null value in query results is treated like a zero."""
        config_data["queries"]["q"]["sql"] = "SELECT NULL"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("m")
        assert metric_values(metric) == [0]

    async def test_run_query_log(self, caplog, query_tracker, query_loop):
        """Debug messages are logged on query execution."""
        caplog.set_level(logging.DEBUG)
        await query_loop.start()
        await query_tracker.wait_queries()
        assert caplog.messages == [
            'connected to database "db"',
            'running query "q" on database "db"',
            'updating metric "m" set 100.0 {database="db"}',
            'updating metric "queries" inc 1 {database="db",status="success"}',
        ]

    async def test_run_query_log_labels(
        self, caplog, query_tracker, config_data, make_query_loop
    ):
        """Debug messages include metric labels."""
        config_data["metrics"]["m"]["labels"] = ["l"]
        config_data["queries"]["q"]["sql"] = 'SELECT 100.0 AS m, "foo" AS l'
        query_loop = make_query_loop()
        caplog.set_level(logging.DEBUG)
        await query_loop.start()
        await query_tracker.wait_queries()
        assert caplog.messages == [
            'connected to database "db"',
            'running query "q" on database "db"',
            'updating metric "m" set 100.0 {database="db",l="foo"}',
            'updating metric "queries" inc 1 {database="db",status="success"}',
        ]

    async def test_run_query_increase_db_error_count(
        self, query_tracker, config_data, make_query_loop, registry
    ):
        """Query errors are logged."""
        config_data["databases"]["db"]["dsn"] = f"sqlite:////invalid"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("database_errors")
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_error_count(
        self, query_tracker, config_data, make_query_loop, registry
    ):
        """Count of errored queries is incremented on error."""
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {("error",): 1.0}

    async def test_run_query_at_interval(self, advance_time, query_tracker, query_loop):
        """Queries are run at the specified time interval."""
        await query_loop.start()
        await advance_time(0)  # kick the first run
        # the query has been run once
        assert len(query_tracker.queries) == 1
        await advance_time(5)
        # no more runs yet
        assert len(query_tracker.queries) == 1
        # now the query runs again
        await advance_time(5)
        assert len(query_tracker.queries) == 2

    async def test_run_periodic_queries_invalid_result_count(
        self, query_tracker, config_data, make_query_loop, advance_time
    ):
        """Periodic queries returning invalid elements count are removed."""
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        query_loop = make_query_loop()
        await query_loop.start()
        await advance_time(0)  # kick the first run
        assert len(query_tracker.queries) == 1
        assert len(query_tracker.results) == 0
        # the query is not run again
        await advance_time(5)
        assert len(query_tracker.results) == 0
        await advance_time(5)
        assert len(query_tracker.results) == 0

    async def test_run_periodic_queries_invalid_result_count_stop_task(
        self, query_tracker, config_data, make_query_loop
    ):
        """Periodic queries returning invalid result counts are stopped."""
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        config_data["queries"]["q"]["interval"] = 1.0
        query_loop = make_query_loop()
        await query_loop.start()
        periodic_call = query_loop._periodic_calls["q"]
        await asyncio.sleep(1.1)
        await query_tracker.wait_failures()
        # the query has been stopped and removed
        assert not periodic_call.running
        assert query_loop._periodic_calls == {}

    async def test_run_periodic_queries_not_removed_if_not_failing_on_all_dbs(
        self, tmpdir, query_tracker, config_data, make_query_loop
    ):
        """Periodic queries are removed when they fail on all databases."""
        db1 = tmpdir / "db1.sqlite"
        db2 = tmpdir / "db2.sqlite"
        config_data["databases"] = {
            "db1": {"dsn": f"sqlite:///{db1}"},
            "db2": {"dsn": f"sqlite:///{db2}"},
        }
        # the table is only found on one database
        async with DataBase("db1", f"sqlite:///{db1}") as db:
            await db.execute_sql("CREATE TABLE test (x INTEGER)")
            await db.execute_sql("INSERT INTO test VALUES (10)")
        config_data["queries"]["q"]["sql"] = "SELECT x FROM test"
        config_data["queries"]["q"]["interval"] = 1.0
        config_data["queries"]["q"]["databases"] = ["db1", "db2"]
        query_loop = make_query_loop()
        await query_loop.start()
        await asyncio.sleep(0.1)
        await query_tracker.wait_failures()
        assert len(query_tracker.queries) == 2
        assert len(query_tracker.results) == 1
        assert len(query_tracker.failures) == 1
        await asyncio.sleep(1.1)
        # succeeding query is run again, failing one is not
        assert len(query_tracker.results) == 2
        assert len(query_tracker.failures) == 1

    async def test_run_aperiodic_queries(
        self, query_tracker, config_data, make_query_loop
    ):
        """Queries with null interval can be run explicitly."""
        del config_data["queries"]["q"]["interval"]
        query_loop = make_query_loop()
        await query_loop.run_aperiodic_queries()
        assert len(query_tracker.queries) == 1
        await query_loop.run_aperiodic_queries()
        assert len(query_tracker.queries) == 2

    async def test_run_aperiodic_queries_invalid_result_count(
        self, query_tracker, config_data, make_query_loop
    ):
        """Aperiodic queries returning invalid elements count are removed."""
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        del config_data["queries"]["q"]["interval"]
        query_loop = make_query_loop()
        await query_loop.run_aperiodic_queries()
        assert len(query_tracker.queries) == 1
        # the query is not run again
        await query_loop.run_aperiodic_queries()
        await query_tracker.wait_failures()
        assert len(query_tracker.queries) == 1

    async def test_run_aperiodic_queries_not_removed_if_not_failing_on_all_dbs(
        self, tmpdir, query_tracker, config_data, make_query_loop
    ):
        """Periodic queries are removed when they fail on all databases."""
        db1 = tmpdir / "db1.sqlite"
        db2 = tmpdir / "db2.sqlite"
        config_data["databases"] = {
            "db1": {"dsn": f"sqlite:///{db1}"},
            "db2": {"dsn": f"sqlite:///{db2}"},
        }
        # the table is only found on one database
        async with DataBase("db1", f"sqlite:///{db1}") as db:
            await db.execute_sql("CREATE TABLE test (x INTEGER)")
            await db.execute_sql("INSERT INTO test VALUES (10)")
        config_data["queries"]["q"]["sql"] = "SELECT x FROM test"
        config_data["queries"]["q"]["interval"] = None
        config_data["queries"]["q"]["databases"] = ["db1", "db2"]
        query_loop = make_query_loop()
        await query_loop.run_aperiodic_queries()
        await query_tracker.wait_failures()
        assert len(query_tracker.queries) == 2
        assert len(query_tracker.results) == 1
        assert len(query_tracker.failures) == 1
        await query_loop.run_aperiodic_queries()
        # succeeding query is run again, failing one is not
        assert len(query_tracker.results) == 2
        assert len(query_tracker.failures) == 1
