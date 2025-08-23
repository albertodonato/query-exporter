import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
from decimal import Decimal
from pathlib import Path
import typing as t
from unittest.mock import ANY

from prometheus_aioexporter import MetricsRegistry
import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from sqlalchemy.sql.elements import TextClause
import yaml

from query_exporter.config import load_config
from query_exporter.db import DataBase, DataBaseConfig
from query_exporter.executor import MetricsLastSeen, QueryExecutor

from .conftest import QueryTracker, metric_values

AdvanceTime = Callable[[float], t.Awaitable[None]]


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


MakeQueryExecutor = Callable[[], QueryExecutor]


@pytest.fixture
async def make_query_executor(
    tmp_path: Path, config_data: dict[str, t.Any], registry: MetricsRegistry
) -> AsyncIterator[MakeQueryExecutor]:
    query_executors = []

    def make_executor() -> QueryExecutor:
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config_data), "utf-8")
        config = load_config([config_file])
        registry.create_metrics(config.metrics.values())
        query_executor = QueryExecutor(config, registry)
        query_executors.append(query_executor)
        return query_executor

    yield make_executor
    await asyncio.gather(
        *(query_executor.stop() for query_executor in query_executors),
        return_exceptions=True,
    )


@pytest.fixture
async def query_executor(
    make_query_executor: MakeQueryExecutor,
) -> AsyncIterator[QueryExecutor]:
    yield make_query_executor()


async def run_queries(db_file: Path, *queries: str) -> None:
    config = DataBaseConfig(name="db", dsn=f"sqlite:///{db_file}")
    async with DataBase(config) as db:
        for query in queries:
            await db.execute_sql(query)


class TestMetricsLastSeen:
    def test_update(self) -> None:
        last_seen = MetricsLastSeen({"m1": 50, "m2": 100})
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
        last_seen = MetricsLastSeen({"m1": 50})
        last_seen.update("m1", {"l2": "v2", "l1": "v1"}, 100)
        assert last_seen._last_seen == {"m1": {("v1", "v2"): 100}}

    def test_expire_series_not_expired(self) -> None:
        last_seen = MetricsLastSeen({"m1": 50})
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
        last_seen = MetricsLastSeen({"m1": 50, "m2": 100})
        last_seen.update("m1", {"l1": "v1", "l2": "v2"}, 10)
        last_seen.update("m1", {"l1": "v3", "l2": "v4"}, 100)
        last_seen.update("m2", {"l3": "v100"}, 100)
        assert last_seen.expire_series(120) == {"m1": [("v1", "v2")]}
        assert last_seen._last_seen == {
            "m1": {("v3", "v4"): 100},
            "m2": {("v100",): 100},
        }

    def test_expire_no_labels(self) -> None:
        last_seen = MetricsLastSeen({"m1": 50})
        last_seen.update("m1", {}, 10)
        expired = last_seen.expire_series(120)
        assert expired == {"m1": [()]}
        assert last_seen._last_seen == {}


class TestQueryExecutor:
    async def test_start(
        self,
        query_tracker: QueryTracker,
        query_executor: QueryExecutor,
    ) -> None:
        await query_executor.start()
        timed_call = query_executor._timed_calls["q"]
        assert timed_call.running
        await query_tracker.wait_results()

    async def test_stop(self, query_executor: QueryExecutor) -> None:
        await query_executor.start()
        timed_call = query_executor._timed_calls["q"]
        await query_executor.stop()
        assert not timed_call.running

    async def test_run_query(
        self,
        query_tracker: QueryTracker,
        query_executor: QueryExecutor,
        registry: MetricsRegistry,
    ) -> None:
        await query_executor.start()
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
        mocker: MockerFixture,
        advance_time: AdvanceTime,
        query_tracker: QueryTracker,
        registry: MetricsRegistry,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        event_loop = asyncio.get_running_loop()

        def croniter(*args: t.Any) -> Iterator[float]:
            while True:
                # sync croniter time with the loop one
                yield event_loop.time() + 60

        mock_croniter = mocker.patch("query_exporter.executor.croniter")
        mock_croniter.side_effect = croniter
        # ensure that both clocks advance in sync
        mocker.patch(
            "query_exporter.executor.time.time",
            lambda: event_loop.time(),
        )

        del config_data["queries"]["q"]["interval"]
        config_data["queries"]["q"]["schedule"] = "*/2 * * * *"
        query_executor = make_query_executor()
        await query_executor.start()
        mock_croniter.assert_called_once()

    async def test_run_query_with_parameters(
        self,
        query_tracker: QueryTracker,
        registry: MetricsRegistry,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["metrics"]["m"]["type"] = "counter"
        config_data["metrics"]["m"]["labels"] = ["l"]
        config_data["queries"]["q"]["sql"] = "SELECT :param AS m, :label as l"
        config_data["queries"]["q"]["parameters"] = [
            {"param": 10.0, "label": "l1"},
            {"param": 20.0, "label": "l2"},
        ]
        query_executor = make_query_executor()
        await query_executor.start()
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

    @pytest.mark.parametrize(
        "increment,value",
        [
            (True, 30.0),
            (False, 20.0),
        ],
    )
    async def test_run_query_counter(
        self,
        query_tracker: QueryTracker,
        registry: MetricsRegistry,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
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
        query_executor = make_query_executor()
        await query_executor.start()
        await query_tracker.wait_results()
        # the metric is updated
        metric = registry.get_metric("m")
        assert metric_values(metric) == [value]

    @pytest.mark.parametrize(
        "metric_config,sql",
        [
            ({"type": "gauge"}, "SELECT 'invalid' AS m"),
            ({"type": "enum", "states": ["foo", "bar"]}, "SELECT 'baz' AS m"),
        ],
    )
    async def test_run_query_invalid_result(
        self,
        query_tracker: QueryTracker,
        registry: MetricsRegistry,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        metric_config: dict[str, t.Any],
        sql: str,
    ) -> None:
        config_data["metrics"]["m"] = metric_config
        config_data["queries"]["q"]["sql"] = sql
        query_executor = make_query_executor()
        await query_executor.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("queries")
        assert metric_values(metric, by_labels=("query", "status")) == {
            ("q", "invalid-value"): 1.0
        }

    async def test_run_query_metrics_with_database_labels(
        self,
        query_tracker: QueryTracker,
        registry: MetricsRegistry,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["databases"] = {
            "db1": {"dsn": "sqlite://", "labels": {"l1": "v1", "l2": "v2"}},
            "db2": {"dsn": "sqlite://", "labels": {"l1": "v3", "l2": "v4"}},
        }
        config_data["queries"]["q"]["databases"] = ["db1", "db2"]
        query_executor = make_query_executor()
        await query_executor.start()
        await query_tracker.wait_results()
        metric = registry.get_metric("m")
        assert metric_values(metric, by_labels=("database", "l1", "l2")) == {
            ("db1", "v1", "v2"): 100.0,
            ("db2", "v3", "v4"): 100.0,
        }

    @pytest.mark.parametrize(
        "result,metric_value",
        [
            (Decimal("100.123"), 100.123),
            (23, 23.0),
            ("100.123", 100.123),
            (None, 0),
        ],
    )
    async def test_update_metric_valid_not_float(
        self,
        registry: MetricsRegistry,
        make_query_executor: MakeQueryExecutor,
        result: t.Any,
        metric_value: float,
    ) -> None:
        db = DataBase(DataBaseConfig(name="db", dsn="sqlite://"))
        query_executor = make_query_executor()
        query_executor._update_metric(db, "m", result)
        metric = registry.get_metric("m")
        [value] = metric_values(metric)
        assert value == metric_value
        assert isinstance(value, float)

    async def test_run_query_log(
        self,
        log: StructuredLogCapture,
        query_tracker: QueryTracker,
        query_executor: QueryExecutor,
    ) -> None:
        await query_executor.start()
        await query_tracker.wait_queries()
        assert [
            log.debug(
                "updating metric",
                metric="query_latency",
                method="observe",
                value=ANY,
                labels={"database": "db", "query": "q"},
            ),
            log.debug(
                "updating metric",
                metric="m",
                method="set",
                value=100.0,
                labels={"database": "db"},
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
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["metrics"]["m"]["labels"] = ["l"]
        config_data["queries"]["q"]["sql"] = 'SELECT 100.0 AS m, "foo" AS l'
        query_executor = make_query_executor()
        await query_executor.start()
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
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        registry: MetricsRegistry,
    ) -> None:
        config_data["databases"]["db"]["dsn"] = "sqlite:////invalid"
        query_executor = make_query_executor()
        await query_executor.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("database_errors")
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_database_error_count(
        self,
        mocker: MockerFixture,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        registry: MetricsRegistry,
    ) -> None:
        query_executor = make_query_executor()
        db = query_executor._databases["db"]
        mock_connect = mocker.patch.object(db._conn.engine, "connect")
        mock_connect.side_effect = Exception("connection failed")
        await query_executor.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("database_errors")
        assert metric_values(queries_metric) == [1.0]

    async def test_run_query_increase_query_error_count(
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        registry: MetricsRegistry,
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        query_executor = make_query_executor()
        await query_executor.start()
        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("error",): 1.0
        }

    async def test_run_query_increase_timeout_count(
        self,
        mocker: MockerFixture,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        registry: MetricsRegistry,
    ) -> None:
        config_data["queries"]["q"]["timeout"] = 0.1
        query_executor = make_query_executor()
        await query_executor.start()
        db = query_executor._databases["db"]
        await db.connect()

        async def execute(
            sql: TextClause, parameters: dict[str, t.Any] | None
        ) -> None:
            await asyncio.sleep(1)  # longer than timeout

        mocker.patch.object(db._conn, "execute", execute)

        await query_tracker.wait_failures()
        queries_metric = registry.get_metric("queries")
        assert metric_values(queries_metric, by_labels=("status",)) == {
            ("timeout",): 1.0
        }

    async def test_run_query_at_interval(
        self,
        advance_time: AdvanceTime,
        query_tracker: QueryTracker,
        query_executor: QueryExecutor,
    ) -> None:
        await query_executor.start()
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
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        config_data["queries"]["q"]["interval"] = 1.0
        query_executor = make_query_executor()
        await query_executor.start()
        timed_call = query_executor._timed_calls["q"]
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
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        config_data["queries"]["q"]["interval"] = 1.0
        query_executor = make_query_executor()
        await query_executor.start()
        timed_call = query_executor._timed_calls["q"]
        await asyncio.sleep(1.1)
        await query_tracker.wait_failures()
        # the query has been stopped and removed
        assert not timed_call.running
        assert query_executor._timed_calls == {}

    async def test_run_timed_queries_not_removed_if_not_failing_on_all_dbs(
        self,
        tmp_path: Path,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
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
        query_executor = make_query_executor()
        await query_executor.start()
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
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        del config_data["queries"]["q"]["interval"]
        query_executor = make_query_executor()
        await query_executor.run_aperiodic_queries()
        assert len(query_tracker.queries) == 1
        await query_executor.run_aperiodic_queries()
        assert len(query_tracker.queries) == 2

    async def test_run_aperiodic_queries_invalid_result_count(
        self,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
    ) -> None:
        config_data["queries"]["q"]["sql"] = "SELECT 100.0 AS a, 200.0 AS b"
        del config_data["queries"]["q"]["interval"]
        query_executor = make_query_executor()
        await query_executor.run_aperiodic_queries()
        assert len(query_tracker.queries) == 1
        # the query is not run again
        await query_executor.run_aperiodic_queries()
        await query_tracker.wait_failures()
        assert len(query_tracker.queries) == 1

    async def test_run_aperiodic_queries_not_removed_if_not_failing_on_all_dbs(
        self,
        tmp_path: Path,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
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
        query_executor = make_query_executor()
        await query_executor.run_aperiodic_queries()
        await query_tracker.wait_failures()
        assert len(query_tracker.queries) == 2
        assert len(query_tracker.results) == 1
        assert len(query_tracker.failures) == 1
        await query_executor.run_aperiodic_queries()
        # succeeding query is run again, failing one is not
        assert len(query_tracker.results) == 2
        assert len(query_tracker.failures) == 1

    async def test_clear_expired_series(
        self,
        tmp_path: Path,
        advance_time: AdvanceTime,
        query_tracker: QueryTracker,
        config_data: dict[str, t.Any],
        make_query_executor: MakeQueryExecutor,
        registry: MetricsRegistry,
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
        query_executor = make_query_executor()
        await query_executor.run_aperiodic_queries()
        await query_tracker.wait_results()
        queries_metric = registry.get_metric("m")
        assert metric_values(queries_metric, by_labels=("l",)) == {
            ("foo",): 10.0,
            ("bar",): 20.0,
        }
        await run_queries(db, "DELETE FROM test WHERE m = 10")
        # go beyond expiration time
        await advance_time(20)
        await query_executor.run_aperiodic_queries()
        await query_tracker.wait_results()
        query_executor.clear_expired_series()
        assert metric_values(queries_metric, by_labels=("l",)) == {
            ("bar",): 20.0,
        }
