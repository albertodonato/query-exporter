import asyncio
import logging

import pytest
from sqlalchemy import create_engine
from sqlalchemy_aio import ASYNCIO_STRATEGY
from sqlalchemy_aio.base import AsyncConnection

from ..db import (
    DataBase,
    DataBaseError,
    InvalidQueryParameters,
    InvalidQuerySchedule,
    InvalidResultColumnNames,
    InvalidResultCount,
    MetricResult,
    Query,
    QueryMetric,
    QueryResults,
    QueryTimeoutExpired,
)


class TestInvalidResultCount:
    def test_message(self):
        """The error messagecontains counts."""
        error = InvalidResultCount(1, 2)
        assert str(error) == "Wrong result count from query: expected 1, got 2"


class TestQuery:
    def test_instantiate(self):
        """A query can be instantiated with the specified arguments."""
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
        assert query.config_name == "query"
        assert query.databases == ["db1", "db2"]
        assert query.metrics == [
            QueryMetric("metric1", ["label1", "label2"]),
            QueryMetric("metric2", ["label2"]),
        ]
        assert query.sql == "SELECT 1"
        assert query.parameters == {}
        assert query.interval is None
        assert query.timeout is None

    def test_instantiate_with_config_name(self):
        """A query can be instantiated with a config_name different from name."""
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT metric1 FROM table",
            config_name="query_config",
        )
        assert query.config_name == "query_config"

    def test_instantiate_with_parameters(self):
        """A query can be instantiated with parameters."""
        query = Query(
            "query",
            ["db1", "db2"],
            [
                QueryMetric("metric1", ["label1", "label2"]),
                QueryMetric("metric2", ["label2"]),
            ],
            "SELECT metric1, metric2, label1, label2 FROM table"
            " WHERE x < :param1 AND  y > :param2",
            parameters={"param1": 1, "param2": 2},
        )
        assert query.parameters == {"param1": 1, "param2": 2}

    def test_instantiate_parameters_not_matching(self):
        """If parameters don't match those in SQL, an error is raised."""
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
                parameters={"param1": 1, "param2": 2},
            )

    def test_instantiate_with_interval(self):
        """A query can be instantiated with an interval."""
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

    def test_instantiate_with_schedule(self):
        """A query can be instantiated with a schedule."""
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

    def test_instantiate_with_interval_and_schedule(self):
        """Interval and schedule can't be specified together."""
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

    def test_instantiate_with_invalid_schedule(self):
        """Invalid query schedule raises an error."""
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

    def test_instantiate_with_timeout(self):
        """A query can be instantiated with a timeout."""
        query = Query(
            "query", ["db"], [QueryMetric("metric", [])], "SELECT 1", timeout=2.0
        )
        assert query.timeout == 2.0

    @pytest.mark.parametrize(
        "kwargs,is_timed",
        [({}, False), ({"interval": 20}, True), ({"schedule": "1 * * * *"}, True)],
    )
    def test_timed(self, kwargs, is_timed):
        """Query.timed reports whether the query is run with a time schedule"""
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

    def test_labels(self):
        """All labels for the query can be returned."""
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

    def test_results_empty(self):
        """No error is raised if the result set is empty"""
        query = Query("query", ["db"], [QueryMetric("metric", [])], "")
        query_results = QueryResults(["one"], [])
        metrics_results = query.results(query_results)
        assert metrics_results.results == []

    def test_results_metrics(self):
        """The results method returns results by matching metrics name."""
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric1", []), QueryMetric("metric2", [])],
            "",
        )
        query_results = QueryResults(["metric2", "metric1"], [(11, 22), (33, 44)])
        metrics_results = query.results(query_results)
        assert metrics_results.results == [
            MetricResult("metric1", 22, {}),
            MetricResult("metric2", 11, {}),
            MetricResult("metric1", 44, {}),
            MetricResult("metric2", 33, {}),
        ]

    def test_results_metrics_with_labels(self):
        """The results method returns results by matching metrics name."""
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

    def test_results_wrong_result_count(self):
        """An error is raised if the result column count is wrong."""
        query = Query("query", ["db"], [QueryMetric("metric1", [])], "")
        query_results = QueryResults(["one", "two"], [(1, 2)])
        with pytest.raises(InvalidResultCount):
            query.results(query_results)

    def test_results_wrong_result_count_with_label(self):
        """An error is raised if the result column count is wrong."""
        query = Query("query", ["db"], [QueryMetric("metric1", ["label1"])], "")
        query_results = QueryResults(["one"], [(1,)])
        with pytest.raises(InvalidResultCount):
            query.results(query_results)

    def test_results_wrong_names_with_labels(self):
        """An error is raised if metric and labels names don't match."""
        query = Query("query", ["db"], [QueryMetric("metric1", ["label1"])], "")
        query_results = QueryResults(["one", "two"], [(1, 2)])
        with pytest.raises(InvalidResultColumnNames):
            query.results(query_results)


class TestQueryResults:
    @pytest.mark.asyncio
    async def test_from_results(self):
        """The from_results method creates a QueryResult."""
        engine = create_engine("sqlite://", strategy=ASYNCIO_STRATEGY)
        async with engine.connect() as conn:
            result = await conn.execute("SELECT 1 AS a, 2 AS b")
            query_results = await QueryResults.from_results(result)
        assert query_results.keys == ["a", "b"]
        assert query_results.rows == [(1, 2)]
        assert query_results.latency is None

    @pytest.mark.asyncio
    async def test_from_results_with_latency(self):
        """The from_results method creates a QueryResult."""
        engine = create_engine("sqlite://", strategy=ASYNCIO_STRATEGY)
        async with engine.connect() as conn:
            result = await conn.execute("SELECT 1 AS a, 2 AS b")
            # simulate latency tracking
            conn.sync_connection.info["query_latency"] = 1.2
            query_results = await QueryResults.from_results(result)
        assert query_results.keys == ["a", "b"]
        assert query_results.rows == [(1, 2)]
        assert query_results.latency == 1.2


@pytest.fixture
async def db():
    db = DataBase("db", "sqlite://")
    yield db
    await db.close()


class TestDataBase:
    def test_instantiate(self):
        """A DataBase can be instantiated with the specified arguments."""
        db = DataBase("db", "sqlite:///foo")
        assert db.name == "db"
        assert db.dsn == "sqlite:///foo"
        assert db.keep_connected
        assert db.labels == {}

    def test_instantiate_no_keep_connected(self):
        """keep_connected can be set to false."""
        db = DataBase("db", "sqlite:///foo", keep_connected=False)
        assert not db.keep_connected

    def test_instantiate_with_labels(self):
        """Static labels can be passed to a database."""
        db = DataBase("db", "sqlite:///foo", labels={"l1": "v1", "l2": "v2"})
        assert db.labels == {"l1": "v1", "l2": "v2"}

    def test_instantiate_missing_engine_module(self, caplog):
        """An error is raised if a module for the engine is missing."""
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DataBaseError) as error:
                DataBase("db", "postgresql:///foo")
        assert str(error.value) == 'module "psycopg2" not found'
        assert 'module "psycopg2" not found' in caplog.text

    def test_instantiate_autocommit_false(self):
        """Query autocommit can be set to False."""
        db = DataBase("db", "sqlite:///foo", autocommit=False)
        assert not db.autocommit

    @pytest.mark.parametrize("dsn", ["foo-bar", "unknown:///db"])
    def test_instantiate_invalid_dsn(self, caplog, dsn):
        """An error is raised if a the provided DSN is invalid."""
        with caplog.at_level(logging.ERROR), pytest.raises(DataBaseError) as error:
            DataBase("db", dsn)
        assert str(error.value) == f'Invalid database DSN: "{dsn}"'
        assert f'Invalid database DSN: "{dsn}"' in caplog.text

    @pytest.mark.asyncio
    async def test_as_context_manager(self, db):
        """The database can be used as an async context manager."""
        async with DataBase("db", "sqlite://") as db:
            result = await db.execute_sql("SELECT 10 AS a, 20 AS b")
            assert await result.fetchall() == [(10, 20)]
        # the db is closed at context exit
        assert not db.connected

    @pytest.mark.asyncio
    async def test_connect(self, caplog, db):
        """The connect connects to the database."""
        with caplog.at_level(logging.DEBUG):
            await db.connect()
        assert isinstance(db._conn, AsyncConnection)
        assert caplog.messages == ['connected to database "db"']

    @pytest.mark.asyncio
    async def test_connect_lock(self, caplog, db):
        """The connect method has a lock to prevent concurrent calls."""
        with caplog.at_level(logging.DEBUG):
            await asyncio.gather(db.connect(), db.connect())
        assert caplog.messages == ['connected to database "db"']

    @pytest.mark.asyncio
    async def test_connect_error(self):
        """A DataBaseError is raised if database connection fails."""
        db = DataBase("db", "sqlite:////invalid")
        with pytest.raises(DataBaseError) as error:
            await db.connect()
        assert "unable to open database file" in str(error.value)

    @pytest.mark.asyncio
    async def test_connect_sql(self):
        """If connect_sql is specified, it's run at connection."""
        db = DataBase("db", "sqlite://", connect_sql=["SELECT 1", "SELECT 2"])

        queries = []

        async def execute_sql(sql):
            queries.append(sql)

        db.execute_sql = execute_sql
        await db.connect()
        assert queries == ["SELECT 1", "SELECT 2"]
        await db.close()

    @pytest.mark.asyncio
    async def test_connect_sql_fail(self, caplog):
        """If the SQL at connection fails, an error is raised."""
        db = DataBase("db", "sqlite://", connect_sql=["WRONG"])
        with caplog.at_level(logging.DEBUG), pytest.raises(DataBaseError) as error:
            await db.connect()
        assert not db.connected
        assert 'failed executing query "WRONG"' in str(error.value)
        assert 'disconnected from database "db"' in caplog.messages

    @pytest.mark.asyncio
    async def test_close(self, caplog, db):
        """The close method closes database connection."""
        await db.connect()
        connection = db._conn
        with caplog.at_level(logging.DEBUG):
            await db.close()
        assert caplog.messages == ['disconnected from database "db"']
        assert connection.closed
        assert db._conn is None

    @pytest.mark.asyncio
    async def test_execute_log(self, caplog):
        """A message is logged about the query being executed."""
        db = DataBase("db", "sqlite://")
        query = Query(
            "query", ["db"], [QueryMetric("metric", [])], "SELECT 1.0 AS metric"
        )
        await db.connect()
        with caplog.at_level(logging.DEBUG):
            await db.execute(query)
        assert caplog.messages == ['running query "query" on database "db"']
        await db.close()

    @pytest.mark.parametrize("connected", [True, False])
    @pytest.mark.asyncio
    async def test_execute_keep_connected(self, connected):
        """If keep_connected is set to true, the db is not closed."""
        db = DataBase("db", "sqlite://", keep_connected=connected)
        query = Query(
            "query", ["db"], [QueryMetric("metric", [])], "SELECT 1.0 AS metric"
        )
        await db.connect()
        await db.execute(query)
        assert db.connected == connected
        await db.close()

    @pytest.mark.asyncio
    async def test_execute_no_keep_disconnect_after_pending_queries(self):
        """The db is disconnected only after pending queries are run."""
        db = DataBase("db", "sqlite://", keep_connected=False)
        query1 = Query(
            "query1", ["db"], [QueryMetric("metric1", [])], "SELECT 1.0 AS metric1"
        )
        query2 = Query(
            "query1", ["db"], [QueryMetric("metric2", [])], "SELECT 1.0 AS metric2"
        )
        await db.connect()
        await asyncio.gather(db.execute(query1), db.execute(query2))
        assert not db.connected

    @pytest.mark.asyncio
    async def test_execute_not_connected(self, db):
        """The execute recconnects to the database if not connected."""
        query = Query(
            "query", ["db"], [QueryMetric("metric", [])], "SELECT 1 AS metric"
        )
        metric_results = await db.execute(query)
        assert metric_results.results == [MetricResult("metric", 1, {})]
        # the connection is kept for reuse
        assert not db._conn.closed

    @pytest.mark.asyncio
    async def test_execute(self, db):
        """The execute method executes a query."""
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
        await db.connect()
        metric_results = await db.execute(query)
        assert metric_results.results == [
            MetricResult("metric1", 10, {}),
            MetricResult("metric2", 20, {}),
            MetricResult("metric1", 30, {}),
            MetricResult("metric2", 40, {}),
        ]
        assert isinstance(metric_results.latency, float)

    @pytest.mark.asyncio
    async def test_execute_with_labels(self, db):
        """The execute method executes a query with labels."""
        sql = """
            SELECT metric2, metric1, label2, label1 FROM (
              SELECT 11 AS metric2, 22 AS metric1,
                     "foo" AS label2, "bar" AS label1
              UNION
              SELECT 33 AS metric2, 44 AS metric1,
                     "baz" AS label2, "bza" AS label1
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
        await db.connect()
        metric_results = await db.execute(query)
        assert metric_results.results == [
            MetricResult("metric1", 22, {"label1": "bar", "label2": "foo"}),
            MetricResult("metric2", 11, {"label2": "foo"}),
            MetricResult("metric1", 44, {"label1": "bza", "label2": "baz"}),
            MetricResult("metric2", 33, {"label2": "baz"}),
        ]

    @pytest.mark.asyncio
    async def test_execute_query_invalid_count(self, caplog, db):
        """If the number of fields don't match, an error is raised."""
        query = Query(
            "query",
            20,
            [QueryMetric("metric", [])],
            "SELECT 1 AS metric, 2 AS other",
        )
        await db.connect()
        with caplog.at_level(logging.ERROR), pytest.raises(DataBaseError) as error:
            await db.execute(query)
        assert str(error.value) == "Wrong result count from query: expected 1, got 2"
        assert error.value.fatal
        assert caplog.messages == [
            'query "query" on database "db" failed: '
            "Wrong result count from query: expected 1, got 2"
        ]

    @pytest.mark.asyncio
    async def test_execute_query_invalid_count_with_labels(self, db):
        """If the number of fields don't match, an error is raised."""
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", ["label"])],
            "SELECT 1 as metric",
        )
        await db.connect()
        with pytest.raises(DataBaseError) as error:
            await db.execute(query)
        assert str(error.value) == "Wrong result count from query: expected 2, got 1"
        assert error.value.fatal

    @pytest.mark.asyncio
    async def test_execute_query_invalid_names_with_labels(self, db):
        """If the names of fields don't match, an error is raised."""
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", ["label"])],
            'SELECT 1 AS foo, "bar" AS label',
        )
        await db.connect()
        with pytest.raises(DataBaseError) as error:
            await db.execute(query)
        assert str(error.value) == "Wrong column names from query"
        assert error.value.fatal

    @pytest.mark.asyncio
    async def test_execute_timeout(self, caplog, db):
        """If the query times out, an error is raised and logged."""
        query = Query(
            "query",
            ["db"],
            [QueryMetric("metric", [])],
            "SELECT 1 AS metric",
            timeout=0.1,
        )
        await db.connect()

        async def execute(sql, parameters):
            await asyncio.sleep(1)  # longer than timeout

        db._conn.execute = execute

        with caplog.at_level(logging.WARNING), pytest.raises(
            QueryTimeoutExpired
        ) as error:
            await db.execute(query)
        assert (
            str(error.value) == 'Execution for query "query" expired after 0.1 seconds'
        )
        assert caplog.messages == [
            'Execution for query "query" expired after 0.1 seconds'
        ]

    @pytest.mark.asyncio
    async def test_execute_sql(self, db):
        """It's possible to execute raw SQL."""
        await db.connect()
        result = await db.execute_sql("SELECT 10, 20")
        assert await result.fetchall() == [(10, 20)]
