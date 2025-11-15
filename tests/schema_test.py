import typing as t

from pydantic import TypeAdapter, ValidationError
import pytest

from query_exporter.schema import (
    DSN,
    Buckets,
    BuiltinMetric,
    BuiltinMetrics,
    Database,
    ExporterConfig,
    Label,
    Metric,
    Port,
    QueryParameters,
    TimeInterval,
    Timeout,
)

from .conftest import ConfigWriter


def validate_field(field: type, value: t.Any) -> t.Any:
    return TypeAdapter(field).validate_python(value)


class InputsTest:
    field: type


def valid_inputs(
    *inputs: tuple[t.Any, t.Any],
    fixtures: t.Iterable[str] = (),
) -> t.Callable[[InputsTest, t.Any, t.Any], None]:
    @pytest.mark.parametrize("value,converted", inputs)
    def test(self: InputsTest, value: t.Any, converted: t.Any) -> None:
        assert validate_field(self.field, value) == converted

    if fixtures:
        test = pytest.mark.usefixtures(*fixtures)(test)

    return t.cast(t.Callable[[InputsTest, t.Any, t.Any], None], test)


def invalid_inputs(
    *inputs: tuple[t.Any, t.Any],
) -> t.Callable[[InputsTest, t.Any, str], None]:
    @pytest.mark.parametrize("value,message", inputs)
    def test(self: InputsTest, value: t.Any, message: t.Any) -> None:
        with pytest.raises(ValidationError) as err:
            validate_field(self.field, value)
        assert message in str(err.value)

    return t.cast(t.Callable[[InputsTest, t.Any, str], None], test)


class TestBuckets:
    field = Buckets

    test_valid = valid_inputs(
        ([0.1, 0.2, 0.5], [0.1, 0.2, 0.5]),
    )
    test_invalid = invalid_inputs(
        ([0.1, 0.2, 0.2, 0.5], "must not contain duplicate items"),
        ([0.1, 0.5, 0.2], "items must be sorted"),
    )


class TestLabel:
    field = Label

    test_valid = valid_inputs(
        ("foo_bar_baz", "foo_bar_baz"),
        ("FooBarBaz", "FooBarBaz"),
    )
    test_invalid = invalid_inputs(
        ("foo barbaz", "should match pattern"),
        ("foo-bar-baz", "should match pattern"),
    )


class TestTimeInterval:
    field = TimeInterval

    test_valid = valid_inputs(
        (10, 10),
        ("10", 10),
        ("10s", 10),
        ("10m", 600),
        ("1h", 3600),
        ("1d", 3600 * 24),
    )
    test_invalid = invalid_inputs(
        ("foo", "invalid interval definition"),
        ("123g", "invalid interval definition"),
        (0, "must be a positive number"),
        (-10, "must be a positive number"),
    )


class TestTimeout:
    field = Timeout

    test_valid = valid_inputs(
        (0.1, 0.1),
        (0.4, 0.4),
        (2.0, 2.0),
    )
    test_invalid = invalid_inputs(
        (0.24, "should be a multiple of 0.1"),
        (0, "should be greater than 0"),
        (-1.2, "should be greater than 0"),
    )


class TestPort:
    field = Port

    test_valid = valid_inputs(
        (10, 10),
        (65535, 65535),
    )
    test_invalid = invalid_inputs(
        (-10, "should be greater than or equal to 1"),
        (70000, "should be less than or equal to 65535"),
    )


class TestDSN:
    field = DSN

    test_valid_string = valid_inputs(
        ("sqlite:///db", "sqlite:///db"),
        ("postgresql://user:pass@host/db", "postgresql://user:pass@host/db"),
    )
    test_valid_dict = valid_inputs(
        ({"dialect": "sqlite"}, "sqlite://"),
        (
            {
                "dialect": "postgresql",
                "user": "user",
                "password": "secret",
                "host": "dbsever",
                "port": 1234,
                "database": "mydb",
                "options": {"foo": "bar", "baz": "bza"},
            },
            "postgresql://user:secret@dbsever:1234/mydb?baz=bza&foo=bar",
        ),
        (
            {
                "dialect": "sqlite",
                "database": "/path/to/file",
            },
            "sqlite:////path/to/file",
        ),
        (
            {
                "dialect": "postgresql",
                "user": "us%r",
                "password": "my+pass",
                "host": "dbsever",
                "database": "mydb",
            },
            "postgresql://us%25r:my+pass@dbsever/mydb",
        ),
        (
            {
                "dialect": "postgresql",
                "database": "mydb",
                "options": {
                    "foo": "a value",
                    "bar": "another/value",
                },
            },
            "postgresql:///mydb?bar=another%2Fvalue&foo=a+value",
        ),
        (
            {
                "dialect": "postgresql",
                "database": "mydb",
                "options": {
                    "foo": ["foo1", "foo2"],
                    "bar": "bar",
                },
            },
            "postgresql:///mydb?bar=bar&foo=foo1&foo=foo2",
        ),
    )
    test_invalid = invalid_inputs(
        ("wrong", "Invalid DSN format"),
        (None, "should be a valid string"),
    )


class TestBuiltinMetric:
    def test_buckets(self) -> None:
        metric = BuiltinMetric(buckets=[0.1, 0.5, 1.0])
        assert metric.buckets == [0.1, 0.5, 1.0]


class TestBuiltinMetrics:
    def test_as_dict_empty(self) -> None:
        assert BuiltinMetrics().as_dict() == {}

    def test_with_values(self) -> None:
        query_latency = BuiltinMetric(buckets=[0.1, 0.5, 1.0])
        metrics = BuiltinMetrics(query_latency=query_latency)
        assert metrics.as_dict() == {"query_latency": query_latency}


class TestDatabase:
    def test_defaults(self) -> None:
        db = Database.model_validate({"dsn": "sqlite:///db"})
        assert db.dsn == "sqlite:///db"
        assert db.connect_sql == []
        assert db.labels == {}
        assert db.connection_pool.size == 1
        assert db.connection_pool.max_overflow == 0

    def test_optional(self) -> None:
        config = {
            "dsn": "sqlite:///db",
            "connect-sql": [
                "PRAGMA application_id = 123",
                "PRAGMA auto_vacuum = 1",
            ],
            "labels": {"label1": "value1", "label2": "value2"},
            "connection-pool": {
                "size": 10,
                "max-overflow": 20,
            },
        }
        db = Database.model_validate(config)
        assert db.dsn == "sqlite:///db"
        assert db.connect_sql == [
            "PRAGMA application_id = 123",
            "PRAGMA auto_vacuum = 1",
        ]
        assert db.labels == {"label1": "value1", "label2": "value2"}
        assert db.connection_pool.size == 10
        assert db.connection_pool.max_overflow == 20

    def test_connection_pool_partial_defaults(self) -> None:
        db = Database.model_validate(
            {
                "dsn": "sqlite:///db",
                "connection-pool": {"size": 20},
            }
        )
        assert db.connection_pool.size == 20
        assert db.connection_pool.max_overflow == 0

        db = Database.model_validate(
            {"dsn": "sqlite:///db", "connection-pool": {"max-overflow": 20}}
        )
        assert db.connection_pool.size == 1
        assert db.connection_pool.max_overflow == 20

    def test_missing_dsn(self) -> None:
        with pytest.raises(ValidationError) as err:
            Database.model_validate({})
        assert "Field required" in str(err.value)

    def test_invalid_label_names(self) -> None:
        config = {
            "dsn": "sqlite:///db",
            "labels": {"not-valid": "bar"},
        }
        with pytest.raises(ValidationError) as err:
            Database.model_validate(config)
        assert "String should match pattern" in str(err.value)


class TestMetric:
    def test_defaults(self) -> None:
        metric = Metric.model_validate({"type": "counter"})
        assert metric.type == "counter"
        assert metric.description == ""
        assert metric.labels == []
        assert metric.buckets is None
        assert metric.expiration is None
        assert not metric.increment

    def test_optional(self) -> None:
        config = {
            "type": "counter",
            "description": "a counter",
            "labels": ["foo", "bar"],
            "buckets": [0.01, 0.05, 0.1],
            "expiration": 10,
        }
        metric = Metric.model_validate(config)
        assert metric.type == "counter"
        assert metric.description == "a counter"
        assert metric.labels == ["foo", "bar"]
        assert metric.buckets == [0.01, 0.05, 0.1]
        assert metric.expiration == 10

    def test_states_enum(self) -> None:
        metric = Metric.model_validate(
            {"type": "enum", "states": ["on", "off"]}
        )
        assert metric.states == ["on", "off"]

    def test_states_not_enum(self) -> None:
        with pytest.raises(ValueError) as err:
            Metric.model_validate(
                {"type": "histogram", "states": ["on", "off"]}
            )
        assert "states can only be set for enum metrics" in str(err.value)

    def test_increment_counter(self) -> None:
        metric = Metric.model_validate({"type": "counter", "increment": True})
        assert metric.increment

    def test_increment_not_counter(self) -> None:
        with pytest.raises(ValueError) as err:
            Metric.model_validate({"type": "histogram", "increment": True})
        assert "increment can only be set for counter metrics" in str(
            err.value
        )

    @pytest.mark.parametrize(
        "metric_config,config",
        [
            (
                {
                    "type": "counter",
                    "description": "a counter",
                },
                {},
            ),
            (
                {
                    "type": "counter",
                    "description": "a counter",
                    "labels": ["foo", "bar"],
                    "buckets": [0.01, 0.05, 0.1],
                    "expiration": 10,
                },
                {
                    "buckets": [0.01, 0.05, 0.1],
                    "expiration": 10,
                },
            ),
        ],
    )
    def test_config(
        self, metric_config: dict[str, t.Any], config: dict[str, t.Any]
    ) -> None:
        metric = Metric.model_validate(metric_config)
        assert metric.config == config

    def test_invalid_type(self) -> None:
        with pytest.raises(ValidationError) as err:
            Metric.model_validate({"type": "other"})
        assert (
            "Input should be 'counter', 'enum', 'gauge', 'histogram' or 'summary'"
            in str(err.value)
        )


class TestQueryParameters:
    field = QueryParameters

    test_convert = valid_inputs(
        (
            [
                {"param1": "label1", "param2": 10},
                {"param1": "label2", "param2": 20},
            ],
            [
                {"param1": "label1", "param2": 10},
                {"param1": "label2", "param2": 20},
            ],
        ),
        (
            {
                "marketplace": [{"name": "amazon"}, {"name": "ebay"}],
                "item": [{"status": "active"}, {"status": "inactive"}],
            },
            [
                {
                    "item__status": "active",
                    "marketplace__name": "amazon",
                },
                {
                    "item__status": "inactive",
                    "marketplace__name": "amazon",
                },
                {
                    "item__status": "active",
                    "marketplace__name": "ebay",
                },
                {
                    "item__status": "inactive",
                    "marketplace__name": "ebay",
                },
            ],
        ),
        (
            {
                "param1": [
                    {
                        "sub1": 100,
                        "sub2": "foo",
                    },
                    {
                        "sub1": 200,
                        "sub2": "bar",
                    },
                ],
                "param2": [
                    {
                        "sub3": "a",
                        "sub4": False,
                    },
                    {
                        "sub3": "b",
                        "sub4": True,
                    },
                ],
                "param3": [
                    {
                        "sub5": "X",
                    },
                    {
                        "sub5": "Y",
                    },
                ],
            },
            [
                {
                    "param1__sub1": 100,
                    "param1__sub2": "foo",
                    "param2__sub3": "a",
                    "param2__sub4": False,
                    "param3__sub5": "X",
                },
                {
                    "param1__sub1": 100,
                    "param1__sub2": "foo",
                    "param2__sub3": "a",
                    "param2__sub4": False,
                    "param3__sub5": "Y",
                },
                {
                    "param1__sub1": 100,
                    "param1__sub2": "foo",
                    "param2__sub3": "b",
                    "param2__sub4": True,
                    "param3__sub5": "X",
                },
                {
                    "param1__sub1": 100,
                    "param1__sub2": "foo",
                    "param2__sub3": "b",
                    "param2__sub4": True,
                    "param3__sub5": "Y",
                },
                {
                    "param1__sub1": 200,
                    "param1__sub2": "bar",
                    "param2__sub3": "a",
                    "param2__sub4": False,
                    "param3__sub5": "X",
                },
                {
                    "param1__sub1": 200,
                    "param1__sub2": "bar",
                    "param2__sub3": "a",
                    "param2__sub4": False,
                    "param3__sub5": "Y",
                },
                {
                    "param1__sub1": 200,
                    "param1__sub2": "bar",
                    "param2__sub3": "b",
                    "param2__sub4": True,
                    "param3__sub5": "X",
                },
                {
                    "param1__sub1": 200,
                    "param1__sub2": "bar",
                    "param2__sub3": "b",
                    "param2__sub4": True,
                    "param3__sub5": "Y",
                },
            ],
        ),
    )
    test_invalid = invalid_inputs(
        ([], "List should have at least 1 item"),
        ({}, "Dictionary should have at least 1 item"),
    )


class TestExporterConfig:
    @pytest.mark.parametrize("section", ["databases", "metrics", "queries"])
    def test_empty_sections(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        section: str,
    ) -> None:
        del sample_config[section]
        with pytest.raises(ValidationError) as err:
            ExporterConfig.model_validate(sample_config)
        assert "Field required" in str(err.value)

    def test_databases(self) -> None:
        cfg = {
            "databases": {
                "db1": {"dsn": "sqlite:///foo"},
                "db2": {
                    "dsn": "sqlite:///bar",
                },
            },
            "metrics": {},
            "queries": {},
        }
        config = ExporterConfig.model_validate(cfg)
        assert {"db1", "db2"} == set(config.databases)
        database1 = config.databases["db1"]
        database2 = config.databases["db2"]
        assert database1.dsn == "sqlite:///foo"
        assert database2.dsn == "sqlite:///bar"

    def test_metrics(self) -> None:
        cfg = {
            "databases": {},
            "metrics": {
                "metric1": {
                    "type": "summary",
                    "description": "metric one",
                    "labels": ["label1", "label2"],
                    "expiration": "2m",
                },
                "metric2": {
                    "type": "histogram",
                    "description": "metric two",
                    "buckets": [10.0, 100.0, 1000.0],
                },
                "metric3": {
                    "type": "enum",
                    "description": "metric three",
                    "states": ["on", "off"],
                    "expiration": 100,
                },
            },
            "queries": {},
        }
        config = ExporterConfig.model_validate(cfg)
        metric1 = config.metrics["metric1"]
        assert metric1.type == "summary"
        assert metric1.description == "metric one"
        assert metric1.labels == ["label1", "label2"]
        assert metric1.config == {"expiration": 120}
        metric2 = config.metrics["metric2"]
        assert metric2.type == "histogram"
        assert metric2.description == "metric two"
        assert metric2.labels == []
        assert metric2.config == {
            "buckets": [10.0, 100.0, 1000.0],
        }
        metric3 = config.metrics["metric3"]
        assert metric3.type == "enum"
        assert metric3.description == "metric three"
        assert metric3.labels == []
        assert metric3.config == {
            "states": ["on", "off"],
            "expiration": 100,
        }

    def test_metrics_invalid_name(self) -> None:
        cfg = {
            "databases": {},
            "metrics": {
                "is wrong": {
                    "type": "counter",
                },
            },
            "queries": {},
        }
        with pytest.raises(ValidationError) as err:
            ExporterConfig.model_validate(cfg)
        assert "String should match pattern '^[a-zA-Z_][a-zA-Z0-9_]*$'" in str(
            err.value
        )

    def test_queries(self) -> None:
        cfg = {
            "databases": {},
            "metrics": {},
            "queries": {
                "q1": {
                    "interval": 10,
                    "databases": ["db1"],
                    "metrics": ["m1"],
                    "sql": "SELECT 1",
                },
                "q2": {
                    "interval": 10,
                    "databases": ["db2"],
                    "metrics": ["m2"],
                    "sql": "SELECT 2",
                },
            },
        }
        config = ExporterConfig.model_validate(cfg)
        assert len(config.queries) == 2
        query1 = config.queries["q1"]
        assert query1.databases == ["db1"]
        assert query1.metrics == ["m1"]
        assert query1.sql == "SELECT 1"
        query2 = config.queries["q2"]
        assert query2.databases == ["db2"]
        assert query2.metrics == ["m2"]
        assert query2.sql == "SELECT 2"
