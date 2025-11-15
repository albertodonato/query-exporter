from collections.abc import Iterator
from contextlib import closing
import time
import typing as t

import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from sqlalchemy import (
    create_engine,
    text,
)

from query_exporter import schema
from query_exporter.db import (
    Database,
    DatabaseConnectError,
    DatabaseError,
    DatabaseQueryError,
    InvalidQueryParameters,
    InvalidQuerySchedule,
    InvalidResultColumnNames,
    InvalidResultCount,
    MetricResult,
    Query,
    QueryExecution,
    QueryMetric,
    QueryResults,
    QueryTimeoutExpired,
    create_db_engine,
)


class TestInvalidResultCount:
    def test_message(self) -> None:
        error = InvalidResultCount(1, 2)
        assert str(error) == "Wrong result count from query: expected 1, got 2"


class TestCreateDBEngine:
    def test_instantiate_missing_engine_module(self) -> None:
        config = schema.Database(dsn="postgresql:///foo")
        with pytest.raises(DatabaseError) as error:
            create_db_engine(config)
        assert str(error.value) == 'Module "psycopg2" not found'

    def test_instantiate_invalid_dsn(self) -> None:
        dsn = "unknown:///db"
        config = schema.Database(dsn=dsn)
        with pytest.raises(DatabaseError) as error:
            create_db_engine(config)
        assert str(error.value) == f'Invalid database DSN: "{dsn}"'


class TestQuery:
    def test_instantiate(self) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT 1",
        )
        assert query.name == "query"
        assert query.databases == ["db1", "db2"]
        assert query.metrics == [
            QueryMetric("metric1", ["label1", "label2"]),
            QueryMetric("metric2", ["label2"]),
        ]
        assert query.sql == "SELECT 1"
        assert query.interval is None
        assert query.timeout is None
        [query_execution] = query.executions
        assert query_execution.name == "query"

    def test_instantiate_with_parameters(self) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT metric1, metric2, label1, label2 FROM table"
            " WHERE x < :param1 AND  y > :param2",
            parameter_sets=[
                {"param1": 1, "param2": 2},
                {"param1": 3, "param2": 4},
            ],
        )
        qe_exec1, qe_exec2 = query.executions
        assert qe_exec1.name == "query[params1]"
        assert qe_exec1.parameters == {"param1": 1, "param2": 2}
        assert qe_exec2.name == "query[params2]"
        assert qe_exec2.parameters == {"param1": 3, "param2": 4}

    def test_instantiate_parameters_not_matching(self) -> None:
        with pytest.raises(InvalidQueryParameters):
            Query(
                "query",
                ["db1", "db2"],
                [
                    QueryMetric("metric1", ["label1", "label2"]),
                    QueryMetric("metric2", ["label2"]),
                ],
                "SELECT metric1, metric2, label1, label2 FROM table"
                " WHERE x < :param1 AND  y > :param3",
                parameter_sets=[{"param1": 1, "param2": 2}],
            )

    def test_instantiate_with_interval(self) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT 1",
            interval=20,
        )
        assert query.interval == 20

    def test_instantiate_with_schedule(self) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT 1",
            schedule="0 * * * *",
        )
        assert query.schedule == "0 * * * *"

    def test_instantiate_with_interval_and_schedule(self) -> None:
        with pytest.raises(InvalidQuerySchedule) as error:
            Query(
                "query",
                ["db1"],
                [QueryMetric("metric1", [])],
                "SELECT 1",
                interval=20,
                schedule="0 * * * *",
            )
        assert (
            str(error.value)
            == 'Invalid schedule for query "query": both interval and schedule specified'
        )

    def test_instantiate_with_invalid_schedule(self) -> None:
        with pytest.raises(InvalidQuerySchedule) as error:
            Query(
                "query",
                ["db1"],
                [QueryMetric("metric1", [])],
                "SELECT 1",
                schedule="wrong",
            )
        assert (
            str(error.value)
            == 'Invalid schedule for query "query": invalid schedule format'
        )

    def test_instantiate_with_timeout(self) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1",
            timeout=2.0,
        )
        assert query.timeout == 2.0

    @pytest.mark.parametrize(
        "kwargs,is_timed",
        [
            ({}, False),
            ({"interval": 20}, True),
            ({"schedule": "1 * * * *"}, True),
        ],
    )
    def test_timed(self, kwargs: dict[str, t.Any], is_timed: bool) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT 1",
            **kwargs,
        )
        assert query.timed == is_timed

    def test_labels(self) -> None:
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT 1",
        )
        assert query.labels() == frozenset(["label1", "label2"])

    def test_results_empty(self) -> None:
        query = Query("query", ["db"], [QueryMetric("metric", [])], "")
        query_results = QueryResults(["one"], [])
        metrics_results = query.results(query_results)
        assert metrics_results.results == []

    def test_results_metrics(self) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric1", []), QueryMetric("metric2", [])],
            "",
        )
        query_results = QueryResults(
            ["metric2", "metric1"], [(11, 22), (33, 44)]
        )
        metrics_results = query.results(query_results)
        assert metrics_results.results == [
            MetricResult("metric1", 22, {}),
            MetricResult("metric2", 11, {}),
            MetricResult("metric1", 44, {}),
            MetricResult("metric2", 33, {}),
        ]

    def test_results_metrics_with_labels(self) -> None:
        query = Query(
            "query",
            ["db"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "",
        )
        query_results = QueryResults(
            ["metric2", "metric1", "label2", "label1"],
            [(11, 22, "foo", "bar"), (33, 44, "baz", "bza")],
        )
        metrics_results = query.results(query_results)
        assert metrics_results.results == [
            MetricResult("metric1", 22, {"label1": "bar", "label2": "foo"}),
            MetricResult("metric2", 11, {"label2": "foo"}),
            MetricResult("metric1", 44, {"label1": "bza", "label2": "baz"}),
            MetricResult("metric2", 33, {"label2": "baz"}),
        ]

    def test_results_wrong_result_count(self) -> None:
        query = Query("query", ["db"], [QueryMetric("metric1", [])], "")
        query_results = QueryResults(["one", "two"], [(1, 2)])
        with pytest.raises(InvalidResultCount):
            query.results(query_results)

    def test_results_wrong_result_count_with_label(self) -> None:
        query = Query(
            "query", ["db"], [QueryMetric("metric1", ["label1"])], ""
        )
        query_results = QueryResults(["one"], [(1,)])
        with pytest.raises(InvalidResultCount):
            query.results(query_results)

    def test_results_wrong_names_with_labels(self) -> None:
        query = Query(
            "query", ["db"], [QueryMetric("metric1", ["label1"])], ""
        )
        query_results = QueryResults(["one", "two"], [(1, 2)])
        with pytest.raises(InvalidResultColumnNames) as error:
            query.results(query_results)
        assert str(error.value) == (
            "Wrong column names from query: "
            "expected (label1, metric1), got (one, two)"
        )


class TestQueryResults:
    def test_from_result(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS a, 2 AS b"))
            query_results = QueryResults.from_result(result)
        assert query_results.keys == ["a", "b"]
        assert query_results.rows == [(1, 2)]
        assert query_results.latency is None
        assert t.cast(float, query_results.timestamp) < time.time()

    def test_from_empty(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA auto_vacuum = 1"))
            query_results = QueryResults.from_result(result)
        assert query_results.keys == []
        assert query_results.rows == []
        assert query_results.latency is None

    def test_from_result_with_latency(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS a, 2 AS b"))
            # simulate latency tracking
            conn.info["query_latency"] = 1.2
            query_results = QueryResults.from_result(result)
        assert query_results.keys == ["a", "b"]
        assert query_results.rows == [(1, 2)]
        assert query_results.latency == 1.2
        assert t.cast(float, query_results.timestamp) < time.time()


@pytest.fixture
def db() -> Iterator[Database]:
    config = schema.Database(dsn="sqlite:///:memory:")
    with closing(Database("db", config)) as db:
        yield db


def make_query_execution() -> QueryExecution:
    query = Query(
        "query",
        ["db"],
        [QueryMetric("metric", [])],
        "SELECT 1.0 AS metric",
    )
    [execution] = query.executions
    return execution


class TestDatabase:
    async def test_connect_log(
        self, log: StructuredLogCapture, db: Database
    ) -> None:
        await db.execute(make_query_execution())
        assert log.has("connected", database="db", level="debug")

    async def test_diconnect_log(
        self, log: StructuredLogCapture, db: Database
    ) -> None:
        await db.execute(make_query_execution())
        db.close()
        assert log.has("disconnected", database="db", level="debug")

    async def test_connect_error(self) -> None:
        config = schema.Database(dsn="sqlite:////invalid")
        db = Database("db", config)
        with pytest.raises(DatabaseConnectError) as error:
            await db.execute(make_query_execution())
        assert "unable to open database file" in str(error.value)

    async def test_connect_sql(self, mocker: MockerFixture) -> None:
        config = schema.Database.model_validate(
            {
                "dsn": "sqlite:///:memory:",
                "connect-sql": [
                    "CREATE TABLE test (n INTEGER)",
                    "INSERT INTO test VALUES (10), (20), (30)",
                ],
            }
        )
        db = Database("db", config)

        # connect SQL is executed in a committed transaction
        result = await db.execute_sql("SELECT n FROM test ORDER BY n")
        assert result.rows == [(10,), (20,), (30,)]

    async def test_connect_sql_fail(self, log: StructuredLogCapture) -> None:
        config = schema.Database.model_validate(
            {
                "dsn": "sqlite:///:memory:",
                "connect-sql": ["WRONG"],
            }
        )
        db = Database("db", config)
        with pytest.raises(DatabaseQueryError) as error:
            await db.execute_sql("SELECT 100")
        assert "failed executing connect SQL" in str(error.value)

    async def test_close(
        self, log: StructuredLogCapture, db: Database
    ) -> None:
        await db.execute_sql("SELECT 100")
        db.close()
        assert log.has("disconnected", database="db")

    async def test_execute_log(
        self, log: StructuredLogCapture, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1.0 AS metric",
        )
        [query_execution] = query.executions
        await db.execute(query_execution)
        assert log.has("run query", query="query", database="db")

    async def test_execute(self, db: Database) -> None:
        sql = (
            "SELECT * FROM (SELECT 10 AS metric1, 20 AS metric2 UNION"
            " SELECT 30 AS metric1, 40 AS metric2)"
        )
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric1", []), QueryMetric("metric2", [])],
            sql,
        )
        [query_execution] = query.executions
        metric_results = await db.execute(query_execution)
        assert metric_results.results == [
            MetricResult("metric1", 10, {}),
            MetricResult("metric2", 20, {}),
            MetricResult("metric1", 30, {}),
            MetricResult("metric2", 40, {}),
        ]
        assert isinstance(metric_results.latency, float)

    async def test_execute_with_labels(self, db: Database) -> None:
        sql = """
            SELECT metric2, metric1, label2, label1 FROM (
              SELECT
                11 AS metric2,
                22 AS metric1,
                "foo" AS label2,
                "bar" AS label1
              UNION
              SELECT
                33 AS metric2,
                44 AS metric1,
                "baz" AS label2,
                "bza" AS label1
            )
            """
        query = Query(
            "query",
            ["db"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            sql,
        )
        [query_execution] = query.executions
        metric_results = await db.execute(query_execution)
        assert metric_results.results == [
            MetricResult("metric1", 22, {"label1": "bar", "label2": "foo"}),
            MetricResult("metric2", 11, {"label2": "foo"}),
            MetricResult("metric1", 44, {"label1": "bza", "label2": "baz"}),
            MetricResult("metric2", 33, {"label2": "baz"}),
        ]

    async def test_execute_fail(self, db: Database) -> None:
        query = Query("query", ["db"], [QueryMetric("metric", [])], "WRONG")
        [query_execution] = query.executions
        with pytest.raises(DatabaseQueryError) as error:
            await db.execute(query_execution)
        assert "syntax error" in str(error.value)

    async def test_execute_query_invalid_count(
        self, log: StructuredLogCapture, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1 AS metric, 2 AS other",
        )
        [query_execution] = query.executions
        with pytest.raises(DatabaseQueryError) as error:
            await db.execute(query_execution)
        assert (
            str(error.value)
            == "Wrong result count from query: expected 1, got 2"
        )
        assert error.value.fatal
        assert log.has(
            "query failed",
            level="error",
            query="query",
            database="db",
            error="Wrong result count from query: expected 1, got 2",
        )

    async def test_execute_query_invalid_count_with_labels(
        self, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", ["label"])],
            "SELECT 1 as metric",
        )
        [query_execution] = query.executions
        with pytest.raises(DatabaseQueryError) as error:
            await db.execute(query_execution)
        assert (
            str(error.value)
            == "Wrong result count from query: expected 2, got 1"
        )
        assert error.value.fatal

    async def test_execute_invalid_names_with_labels(
        self, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", ["label"])],
            'SELECT 1 AS foo, "bar" AS label',
        )
        [query_execution] = query.executions
        with pytest.raises(DatabaseQueryError) as error:
            await db.execute(query_execution)
        assert (
            str(error.value)
            == "Wrong column names from query: expected (label, metric), got (foo, label)"
        )
        assert error.value.fatal

    async def test_execute_debug_exception(
        self, mocker: MockerFixture, log: StructuredLogCapture, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1 AS metric",
        )
        [query_execution] = query.executions
        exception = Exception("boom!")
        mocker.patch.object(db, "execute_sql", side_effect=exception)

        with pytest.raises(DatabaseQueryError) as error:
            await db.execute(query_execution)
        assert str(error.value) == "boom!"
        assert not error.value.fatal
        assert log.has(
            "query failed",
            query="query",
            database="db",
            exception=exception,
            level="error",
        )

    async def test_execute_timeout(
        self, mocker: MockerFixture, log: StructuredLogCapture, db: Database
    ) -> None:
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1 AS metric",
            timeout=0.1,
        )
        [query_execution] = query.executions

        def execute_sync(
            self: t.Any,
            sql: str,
            parameters: dict[str, t.Any] | None = None,
        ) -> None:
            time.sleep(1)  # longer than timeout

        mocker.patch.object(db, "_execute_sync", execute_sync)

        with pytest.raises(QueryTimeoutExpired):
            await db.execute(query_execution)
        assert log.has(
            "query timeout",
            query="query",
            database="db",
            level="warning",
        )

    async def test_execute_sql(self, db: Database) -> None:
        result = await db.execute_sql("SELECT 10, 20")
        assert result.rows == [(10, 20)]

    @pytest.mark.parametrize(
        "error,message",
        [
            ("message", "message"),
            (Exception("message"), "message"),
            (Exception(), "Exception"),
        ],
    )
    def test_error_message(
        self, db: Database, error: str | Exception, message: str
    ) -> None:
        assert db._error_message(error) == message
