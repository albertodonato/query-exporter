import asyncio
from collections import defaultdict
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
import typing as t
from unittest.mock import ANY

from prometheus_aioexporter import MetricsRegistry
import pytest
from pytest_structlog import StructuredLogCapture
import yaml

from query_exporter import loop
from query_exporter.config import (
    DataBaseConfig,
    load_config,
)
from query_exporter.db import DataBase


@pytest.fixture
def config_data() -> Iterator[dict[str, t.Any]]:
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
def registry() -> Iterator[MetricsRegistry]:
    yield MetricsRegistry()


@pytest.fixture
async def make_query_loop(
    tmp_path: Path, config_data: dict[str, t.Any], registry: MetricsRegistry
) -> Iterator[Callable[[], MetricsRegistry]]:
    query_loops = []

    def make_loop() -> loop.QueryLoop:
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config_data), "utf-8")
        config = load_config(config_file)
        registry.create_metrics(config.metrics.values())
        query_loop = loop.QueryLoop(config, registry)
        query_loops.append(query_loop)
        return query_loop

    yield make_loop
    await asyncio.gather(
        *(query_loop.stop() for query_loop in query_loops),
        return_exceptions=True,
    )


@pytest.fixture
async def query_loop(
    make_query_loop: Callable[[], loop.QueryLoop],
) -> Iterator[loop.QueryLoop]:
    yield make_query_loop()


def metric_values(metric, by_labels=()):
    """Return values for the metric."""
    if metric._type == "gauge":
        suffix = ""
    elif metric._type == "counter":
        suffix = "_total"

    values = defaultdict(list)
    for sample_suffix, labels, value, *_ in metric._samples():
        if sample_suffix == suffix:
            if by_labels:
                label_values = tuple(labels[label] for label in by_labels)
                values[label_values] = value
            else:
                values[sample_suffix].append(value)

    return values if by_labels else values[suffix]


async def run_queries(db_file: Path, *queries: str) -> None:
    config = DataBaseConfig(name="db", dsn=f"sqlite:///{db_file}")
    async with DataBase(config) as db:
        for query in queries:
            await db.execute_sql(query)


class TestMetricsLastSeen:
    def test_update(self) -> None:
        last_seen = loop.MetricsLastSeen({"m1": 50, "m2": 100})
        last_seen.update("m1", {"l1": "v1", "l2": "v2"}, 100)
        last_seen.update("m1", {"l1": "v3", "l2": "v4"}, 200)
        last_seen.update("other", {"l3": "v100"}, 300)
        assert last_seen._last_seen == {
            "m1": {
                ("v1", "v2"): 100,
                ("v3", "v4"): 200,
            }
        }

    def test_update_label_values_sorted_by_name(self) -> None:
        last_seen = loop.MetricsLastSeen({"m1": 50})
        last_seen.update("m1", {"l2": "v2", "l1": "v1"}, 100)
        assert last_seen._last_seen == {"m1": {("v1", "v2"): 100}}

    def test_expire_series_not_expired(self) -> None:
        last_seen = loop.MetricsLastSeen({"m1": 50})
        last_seen.update("m1", {"l1": "v1", "l2": "v2"}, 10)
        last_seen.update("m1", {"l1": "v3", "l2": "v4"}, 20)
        assert last_seen.expire_series(30) == {}
        assert last_seen._last_seen == {
            "m1": {
                ("v1", "v2"): 10,
                ("v3", "v4"): 20,
            }
        }

    def test_expire_series(self) -> None:
        last_seen = loop.MetricsLastSeen({"m1": 50, "m2": 100})
        last_seen.update("m1", {"l1": "v1", "l2": "v2"}, 10)
        last_seen.update("m1", {"l1": "v3", "l2": "v4"}, 100)
        last_seen.update("m2", {"l3": "v100"}, 100)
        assert last_seen.expire_series(120) == {"m1": [("v1", "v2")]}
        assert last_seen._last_seen == {
            "m1": {("v3", "v4"): 100},
            "m2": {("v100",): 100},
        }

    def test_expire_no_labels(self) -> None:
        last_seen = loop.MetricsLastSeen({"m1": 50})
        last_seen.update("m1", {}, 10)
        expired = last_seen.expire_series(120)
        assert expired == {"m1": [()]}
        assert last_seen._last_seen == {}


class TestQueryLoop:
    async def test_start(self, query_tracker, query_loop) -> None:
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        assert timed_call.running
        await query_tracker.wait_results()

    async def test_stop(self, query_loop) -> None:
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        await query_loop.stop()
        assert not timed_call.running

    async def test_run_query(
        self, query_tracker, query_loop, registry
    ) -> None:
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
        advance_time,
        query_tracker,
        registry,
        config_data,
        make_query_loop,
    ) -> None:
        event_loop = asyncio.get_running_loop()

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
    ) -> None:
        config_data["metrics"]["m"]["type"] = "counter"
        config_data["metrics"]["m"]["labels"] = ["l"]
        config_data["queries"]["q"]["sql"] = "SELECT :param AS m, :label as l"
        config_data["queries"]["q"]["parameters"] = [
            {"param": 10.0, "label": "l1"},
            {"param": 20.0, "label": "l2"},
        ]
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        # the metric is updated
        metric = registry.get_metric("m")
        assert metric_values(metric, by_labels=("l",)) == {
            ("l1",): 10,
            ("l2",): 20,
        }
        # the number of queries is updated
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("success",): 2.0
        }

    async def test_run_query_null_value(
        self, query_tracker, registry, config_data, make_query_loop
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT NULL AS m"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("m")
        assert metric_values(metric) == [0]

    @pytest.mark.parametrize(
        "increment,value",
        [
            (True, 30.0),
            (False, 20.0),
        ],
    )
    async def test_run_query_counter(
        self,
        query_tracker,
        registry,
        config_data,
        make_query_loop,
        increment: bool,
        value: float,
    ) -> None:
        config_data["metrics"]["m"]["type"] = "counter"
        config_data["queries"]["q"]["sql"] = "SELECT :param AS m"
        config_data["metrics"]["m"]["increment"] = increment
        config_data["queries"]["q"]["parameters"] = [
            {"param": 10.0},
            {"param": 20.0},
        ]
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_results()
        # the metric is updated
        metric = registry.get_metric("m")
        assert metric_values(metric) == [value]

    async def test_run_query_metrics_with_database_labels(
        self, query_tracker, registry, config_data, make_query_loop
    ) -> None:
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

    async def test_update_metric_decimal_value(
        self, registry, make_query_loop
    ) -> None:
        db = DataBase(DataBaseConfig(name="db", dsn="sqlite://"))
        query_loop = make_query_loop()
        query_loop._update_metric(db, "m", Decimal("100.123"))
        metric = registry.get_metric("m")
        [value] = metric_values(metric)
        assert value == 100.123
        assert isinstance(value, float)

    async def test_run_query_log(
        self,
        log: StructuredLogCapture,
        query_tracker,
        query_loop,
    ) -> None:
        await query_loop.start()
        await query_tracker.wait_queries()
        assert [
            log.debug(
                "updating metric",
                metric="m",
                method="set",
                value=100.0,
                labels={"database": "db"},
            ),
            log.debug(
                "updating metric",
                metric="query_latency",
                method="observe",
                value=ANY,
                labels={"database": "db", "query": "q"},
            ),
            log.debug(
                "updating metric",
                metric="queries",
                method="inc",
                value=1,
                labels={"database": "db", "query": "q", "status": "success"},
            ),
        ] <= log.events

    async def test_run_query_log_labels(
        self,
        log: StructuredLogCapture,
        query_tracker,
        config_data,
        make_query_loop,
    ) -> None:
        config_data["metrics"]["m"]["labels"] = ["l"]
        config_data["queries"]["q"]["sql"] = 'SELECT 100.0 AS m, "foo" AS l'
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_queries()
        assert log.has(
            "updating metric",
            level="debug",
            metric="m",
            method="set",
            value=100.0,
            labels={"database": "db", "l": "foo"},
        )

    async def test_run_query_increase_db_error_count(
        self, query_tracker, config_data, make_query_loop, registry
    ) -> None:
        config_data["databases"]["db"]["dsn"] = "sqlite:////invalid"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("database_errors")
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_database_error_count(
        self, mocker, query_tracker, config_data, make_query_loop, registry
    ) -> None:
        query_loop = make_query_loop()
        db = query_loop._databases["db"]
        mock_connect = mocker.patch.object(db._conn.engine, "connect")
        mock_connect.side_effect = Exception("connection failed")
        await query_loop.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("database_errors")
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_query_error_count(
        self, query_tracker, config_data, make_query_loop, registry
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        query_loop = make_query_loop()
        await query_loop.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("error",): 1.0
        }

    async def test_run_query_increase_timeout_count(
        self, query_tracker, config_data, make_query_loop, registry
    ) -> None:
        config_data["queries"]["q"]["timeout"] = 0.1
        query_loop = make_query_loop()
        await query_loop.start()
        db = query_loop._databases["db"]
        await db.connect()

        async def execute(sql, parameters):
            await asyncio.sleep(1)  # longer than timeout

        db._conn.execute = execute

        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("timeout",): 1.0
        }

    async def test_run_query_at_interval(
        self, advance_time, query_tracker, query_loop
    ) -> None:
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
        self, query_tracker, config_data, make_query_loop
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        config_data["queries"]["q"]["interval"] = 1.0
        query_loop = make_query_loop()
        await query_loop.start()
        timed_call = query_loop._timed_calls["q"]
        await asyncio.sleep(1.1)
        await query_tracker.wait_failures()
        assert len(query_tracker.failures) == 1
        assert len(query_tracker.results) == 0
        # the query has been stopped and removed
        assert not timed_call.running
        await asyncio.sleep(1.1)
        await query_tracker.wait_failures()
        assert len(query_tracker.failures) == 1
        assert len(query_tracker.results) == 0

    async def test_run_timed_queries_invalid_result_count_stop_task(
        self, query_tracker, config_data, make_query_loop
    ) -> None:
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
        self, tmp_path, query_tracker, config_data, make_query_loop
    ) -> None:
        db1 = tmp_path / "db1.sqlite"
        db2 = tmp_path / "db2.sqlite"
        config_data["databases"] = {
            "db1": {"dsn": f"sqlite:///{db1}"},
            "db2": {"dsn": f"sqlite:///{db2}"},
        }
        config_data["queries"]["q"].update(
            {
                "databases": ["db1", "db2"],
                "sql": "SELECT * FROM test",
                "interval": 1.0,
            }
        )
        await run_queries(
            db1,
            "CREATE TABLE test (m INTEGER)",
            "INSERT INTO test VALUES (10)",
        )
        # the query on the second database returns more columns
        await run_queries(
            db2,
            "CREATE TABLE test (m INTEGER, other INTERGER)",
            "INSERT INTO test VALUES (10, 20)",
        )
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
    ) -> None:
        del config_data["queries"]["q"]["interval"]
        query_loop = make_query_loop()
        await query_loop.run_aperiodic_queries()
        assert len(query_tracker.queries) == 1
        await query_loop.run_aperiodic_queries()
        assert len(query_tracker.queries) == 2

    async def test_run_aperiodic_queries_invalid_result_count(
        self, query_tracker, config_data, make_query_loop
    ) -> None:
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
        self, tmp_path, query_tracker, config_data, make_query_loop
    ) -> None:
        db1 = tmp_path / "db1.sqlite"
        db2 = tmp_path / "db2.sqlite"
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
        await run_queries(
            db1,
            "CREATE TABLE test (m INTEGER)",
            "INSERT INTO test VALUES (10)",
        )
        # the query on the second database returns more columns
        await run_queries(
            db2,
            "CREATE TABLE test (m INTEGER, other INTERGER)",
            "INSERT INTO test VALUES (10, 20)",
        )
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

    async def test_clear_expired_series(
        self,
        tmp_path,
        advance_time,
        query_tracker,
        config_data,
        make_query_loop,
        registry,
    ) -> None:
        db = tmp_path / "db.sqlite"
        config_data["databases"]["db"]["dsn"] = f"sqlite:///{db}"
        config_data["metrics"]["m"].update(
            {
                "labels": ["l"],
                "expiration": 10,
            }
        )
        config_data["queries"]["q"]["sql"] = "SELECT * FROM test"
        del config_data["queries"]["q"]["interval"]

        await run_queries(
            db,
            "CREATE TABLE test (m INTEGER, l TEXT)",
            'INSERT INTO test VALUES (10, "foo")',
            'INSERT INTO test VALUES (20, "bar")',
        )
        query_loop = make_query_loop()
        await query_loop.run_aperiodic_queries()
        await query_tracker.wait_results()
        queries_metric = registry.get_metric("m")
        assert metric_values(queries_metric, by_labels=("l",)) == {
            ("foo",): 10.0,
            ("bar",): 20.0,
        }
        await run_queries(db, "DELETE FROM test WHERE m = 10")
        # go beyond expiration time
        await advance_time(20)
        await query_loop.run_aperiodic_queries()
        await query_tracker.wait_results()
        query_loop.clear_expired_series()
        assert metric_values(queries_metric, by_labels=("l",)) == {
            ("bar",): 20.0,
        }
