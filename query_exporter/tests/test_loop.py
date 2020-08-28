import asyncio
from collections import defaultdict
from decimal import Decimal
import logging
import re

from prometheus_aioexporter import MetricsRegistry
import pytest
import yaml

from .. import loop
from ..config import load_config
from ..db import DataBase


class re_match:
    """Assert that comparison matches the specified regexp."""

    def __init__(self, pattern, flags=0):
        self._re = re.compile(pattern, flags)

    def __eq__(self, string):
        return bool(self._re.match(string))

    def __repr__(self):
        return self._re.pattern  # pragma: nocover


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
                "sql": "SELECT 100.0 AS m",
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
            config = load_config(fh, logging.getLogger())
        registry.create_metrics(config.metrics.values())
        query_loop = loop.QueryLoop(config, registry, logging)
        query_loops.append(query_loop)
        return query_loop

    yield make_loop
    await asyncio.gather(
        *(query_loop.stop() for query_loop in query_loops),
        return_exceptions=True,
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
        """The start method starts timed calls for queries."""
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        assert timed_call.running

    async def test_stop(self, query_loop):
        """The stop method stops timed calls for queries."""
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        await query_loop.stop()
        assert not timed_call.running

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

    async def test_run_scheduled_query(
        self,
        mocker,
        event_loop,
        advance_time,
        query_tracker,
        registry,
        config_data,
        make_query_loop,
    ):
        """Queries are run and update metrics."""

        def croniter(*args):
            while True:
                # sync croniter time with the loop one
                yield event_loop.time() + 60

        mock_croniter = mocker.patch.object(loop, "croniter")
        mock_croniter.side_effect = croniter
        # ensure that both clocks advance in sync
        mocker.patch.object(loop.time, "time", lambda: event_loop.time())

        del config_data["queries"]["q"]["interval"]
        config_data["queries"]["q"]["schedule"] = "*/2 * * * *"
        query_loop = make_query_loop()
        await query_loop.start()
        mock_croniter.assert_called_once()

    async def test_run_query_with_parameters(
        self, query_tracker, registry, config_data, make_query_loop
    ):
        """Queries are run with declared parameters."""
        config_data["metrics"]["m"]["type"] = "counter"
        config_data["queries"]["q"]["sql"] = "SELECT :param AS m"
        config_data["queries"]["q"]["parameters"] = [{"param": 10.0}, {"param": 20.0}]
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
        config_data["queries"]["q"]["sql"] = "SELECT NULL AS m"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("m")
        assert metric_values(metric) == [0]

    async def test_run_query_metrics_with_database_labels(
        self, query_tracker, registry, config_data, make_query_loop
    ):
        """If databases have extra labels, they're set for metrics."""
        config_data["databases"] = {
            "db1": {"dsn": "sqlite://", "labels": {"l1": "v1", "l2": "v2"}},
            "db2": {"dsn": "sqlite://", "labels": {"l1": "v3", "l2": "v4"}},
        }
        config_data["queries"]["q"]["databases"] = ["db1", "db2"]
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("m")
        assert metric_values(metric, by_labels=("database", "l1", "l2")) == {
            ("db1", "v1", "v2"): 100.0,
            ("db2", "v3", "v4"): 100.0,
        }

    async def test_update_metric_decimal_value(self, registry, make_query_loop):
        """A Decimal value in query results is converted to float."""
        db = DataBase("db", "sqlite://")
        query_loop = make_query_loop()
        query_loop._update_metric(db, "m", Decimal("100.123"))
        metric = registry.get_metric("m")
        [value] = metric_values(metric)
        assert value == 100.123
        assert isinstance(value, float)

    async def test_run_query_log(self, caplog, query_tracker, query_loop):
        """Debug messages are logged on query execution."""
        caplog.set_level(logging.DEBUG)
        await query_loop.start()
        await query_tracker.wait_queries()
        assert caplog.messages == [
            'connected to database "db"',
            'running query "q" on database "db"',
            'updating metric "m" set 100.0 {database="db"}',
            re_match(
                r'updating metric "query_latency" observe .* \{database="db",query="q"\}'
            ),
            'updating metric "queries" inc 1 {database="db",query="q",status="success"}',
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
            re_match(
                r'updating metric "query_latency" observe .* \{database="db",query="q"\}'
            ),
            'updating metric "queries" inc 1 {database="db",query="q",status="success"}',
        ]

    async def test_run_query_increase_db_error_count(
        self, query_tracker, config_data, make_query_loop, registry
    ):
        """Query errors are logged."""
        config_data["databases"]["db"]["dsn"] = "sqlite:////invalid"
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

    async def test_run_query_increase_timeout_count(
        self, query_tracker, config_data, make_query_loop, registry
    ):
        """Count of errored queries is incremented on timeout."""
        config_data["queries"]["q"]["timeout"] = 0.1
        query_loop = make_query_loop()
        await query_loop.start()

        async def execute(sql, parameters):
            await asyncio.sleep(1)  # longer than timeout

        query_loop._config.databases["db"]._conn.execute = execute

        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("timeout",): 1.0
        }

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

    async def test_run_timed_queries_invalid_result_count(
        self, query_tracker, config_data, make_query_loop, advance_time
    ):
        """Timed queries returning invalid elements count are removed."""
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

    async def test_run_timed_queries_invalid_result_count_stop_task(
        self, query_tracker, config_data, make_query_loop
    ):
        """Timed queries returning invalid result counts are stopped."""
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        config_data["queries"]["q"]["interval"] = 1.0
        query_loop = make_query_loop()
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        await asyncio.sleep(1.1)
        await query_tracker.wait_failures()
        # the query has been stopped and removed
        assert not timed_call.running
        assert query_loop._timed_calls == {}

    async def test_run_timed_queries_not_removed_if_not_failing_on_all_dbs(
        self, tmpdir, query_tracker, config_data, make_query_loop
    ):
        """Timed queries are removed when they fail on all databases."""
        db1 = tmpdir / "db1.sqlite"
        db2 = tmpdir / "db2.sqlite"
        config_data["databases"] = {
            "db1": {"dsn": f"sqlite:///{db1}"},
            "db2": {"dsn": f"sqlite:///{db2}"},
        }
        config_data["queries"]["q"].update(
            {"databases": ["db1", "db2"], "sql": "SELECT * FROM test", "interval": 1.0}
        )
        async with DataBase("db", f"sqlite:///{db1}") as db:
            await db.execute_sql("CREATE TABLE test (m INTEGER)")
            await db.execute_sql("INSERT INTO test VALUES (10)")
        # the query on the second database returns more columns
        async with DataBase("db", f"sqlite:///{db2}") as db:
            await db.execute_sql("CREATE TABLE test (m INTEGER, other INTERGER)")
            await db.execute_sql("INSERT INTO test VALUES (10, 20)")
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
        config_data["queries"]["q"].update(
            {
                "databases": ["db1", "db2"],
                "sql": "SELECT * FROM test",
                "interval": None,
            }
        )
        async with DataBase("db", f"sqlite:///{db1}") as db:
            await db.execute_sql("CREATE TABLE test (m INTEGER)")
            await db.execute_sql("INSERT INTO test VALUES (10)")
        # the query on the second database returns more columns
        async with DataBase("db", f"sqlite:///{db2}") as db:
            await db.execute_sql("CREATE TABLE test (m INTEGER, other INTERGER)")
            await db.execute_sql("INSERT INTO test VALUES (10, 20)")
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
