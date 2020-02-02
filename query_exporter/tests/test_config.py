from operator import attrgetter

import yaml

import pytest

from ..config import (
    ConfigError,
    DB_ERRORS_METRIC,
    load_config,
    QUERIES_METRIC,
)
from ..db import QueryMetric


@pytest.fixture
def config_full():
    yield {
        "databases": {"db": {"dsn": "sqlite://"}},
        "metrics": {"m": {"type": "gauge", "labels": ["l1", "l2"]}},
        "queries": {
            "q": {
                "interval": 10,
                "databases": ["db"],
                "metrics": ["m"],
                "sql": "SELECT 1",
            }
        },
    }


@pytest.fixture
def write_config(tmpdir):
    path = tmpdir / "config"

    def write(data):
        path.write_text(yaml.dump(data), "utf-8")
        return path

    yield write


CONFIG_UNKNOWN_DBS = {
    "databases": {},
    "metrics": {"m": {"type": "summary"}},
    "queries": {
        "q": {
            "interval": 10,
            "databases": ["db1", "db2"],
            "metrics": ["m"],
            "sql": "SELECT 1",
        }
    },
}

CONFIG_UNKNOWN_METRICS = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {},
    "queries": {
        "q": {
            "interval": 10,
            "databases": ["db"],
            "metrics": ["m1", "m2"],
            "sql": "SELECT 1",
        }
    },
}

CONFIG_MISSING_DB_KEY = {
    "databases": {},
    "metrics": {},
    "queries": {"q1": {"interval": 10}},
}

CONFIG_MISSING_METRIC_TYPE = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {"m": {}},
    "queries": {},
}

CONFIG_INVALID_METRIC_NAME = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {"is wrong": {"type": "gauge"}},
    "queries": {},
}

CONFIG_INVALID_LABEL_NAME = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {"m": {"type": "gauge", "labels": ["wrong-name"]}},
    "queries": {},
}

CONFIG_INVALID_METRICS_PARAMS_DIFFERENT_KEYS = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {"m": {"type": "gauge"}},
    "queries": {
        "q": {
            "interval": 10,
            "databases": ["db"],
            "metrics": ["m"],
            "sql": "SELECT :param AS m",
            "parameters": [{"foo": 1}, {"bar": 2}],
        },
    },
}


class TestLoadConfig:
    def test_load_databases_section(self, write_config):
        """The 'databases' section is loaded from the config file."""
        config = {
            "databases": {
                "db1": {"dsn": "sqlite:///foo"},
                "db2": {"dsn": "sqlite:///bar", "keep-connected": False},
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd)
        database1, database2 = sorted(result.databases, key=attrgetter("name"))
        assert database1.name == "db1"
        assert database1.dsn == "sqlite:///foo"
        assert database1.keep_connected
        assert database2.name == "db2"
        assert database2.dsn == "sqlite:///bar"
        assert not database2.keep_connected

    def test_load_databases_dsn_from_env(self, write_config):
        """The database DSN can be loaded from env."""
        config = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd, env={"FOO": "sqlite://"})
            [database] = config.databases
        assert database.dsn == "sqlite://"

    def test_load_databases_missing_dsn(self, write_config):
        """An error is raised if the 'dsn' key is missing for a database."""
        config = {"databases": {"db1": {}}, "metrics": {}, "queries": {}}
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert (
            str(err.value)
            == "Invalid config at databases/db1: 'dsn' is a required property"
        )

    def test_load_databases_invalid_dsn(self, write_config):
        """An error is raised if the DSN is invalid."""
        config = {
            "databases": {"db1": {"dsn": "invalid"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert str(err.value) == 'Invalid database DSN: "invalid"'

    def test_load_databases_dsn_invalid_env(self, write_config):
        """An error is raised if the DSN from environment is invalid."""
        config = {
            "databases": {"db1": {"dsn": "env:NOT-VALID"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert str(err.value) == 'Invalid variable name: "NOT-VALID"'

    def test_load_databases_dsn_undefined_env(self, write_config):
        """An error is raised if the environ variable for DSN is undefined."""
        config = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, env={})
        assert str(err.value) == 'Undefined variable: "FOO"'

    def test_load_metrics_section(self, write_config):
        """The 'metrics' section is loaded from the config file."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {
                "metric1": {
                    "type": "summary",
                    "description": "metric one",
                    "labels": ["label1", "label2"],
                },
                "metric2": {
                    "type": "histogram",
                    "description": "metric two",
                    "buckets": [10, 100, 1000],
                },
                "metric3": {
                    "type": "enum",
                    "description": "metric three",
                    "states": ["on", "off"],
                },
            },
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd)
        db_errors_metric, metric1, metric2, metric3, queries_metric = sorted(
            result.metrics, key=attrgetter("name")
        )
        assert metric1.type == "summary"
        assert metric1.description == "metric one"
        assert metric1.config == {"labels": ["database", "label1", "label2"]}
        assert metric2.type == "histogram"
        assert metric2.description == "metric two"
        assert metric2.config == {
            "labels": ["database"],
            "buckets": [10, 100, 1000],
        }
        assert metric3.type == "enum"
        assert metric3.description == "metric three"
        assert metric3.config == {
            "labels": ["database"],
            "states": ["on", "off"],
        }
        # global metrics
        assert db_errors_metric == DB_ERRORS_METRIC
        assert queries_metric == QUERIES_METRIC

    def test_load_metrics_reserved_label(self, write_config):
        """An error is raised if reserved labels are used."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge", "labels": ["database"]}},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert str(err.value) == 'Reserved labels declared for metric "m": database'

    def test_load_metrics_unsupported_type(self, write_config):
        """An error is raised if an unsupported metric type is passed."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {"metric1": {"type": "info", "description": "info metric"}},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert str(err.value) == (
            "Invalid config at metrics/metric1/type: 'info' is not one of "
            "['counter', 'enum', 'gauge', 'histogram', 'summary']"
        )

    def test_load_queries_section(self, write_config):
        """The 'queries' section is loaded from the config file."""
        config = {
            "databases": {
                "db1": {"dsn": "sqlite:///foo"},
                "db2": {"dsn": "sqlite:///bar"},
            },
            "metrics": {
                "m1": {"type": "summary", "labels": ["l1", "l2"]},
                "m2": {"type": "histogram"},
            },
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
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd)
        query1, query2 = sorted(result.queries, key=attrgetter("name"))
        assert query1.name == "q1"
        assert query1.databases == ["db1"]
        assert query1.metrics == [QueryMetric("m1", ["l1", "l2"])]
        assert query1.sql == "SELECT 1"
        assert query1.parameters == {}
        assert query2.name == "q2"
        assert query2.databases == ["db2"]
        assert query2.metrics == [QueryMetric("m2", [])]
        assert query2.sql == "SELECT 2"
        assert query2.parameters == {}

    def test_load_queries_section_with_parameters(self, write_config):
        """Queries can have parameters."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary", "labels": ["l"]}},
            "queries": {
                "q": {
                    "interval": 10,
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT :param1 AS l, :param2 AS m",
                    "parameters": [
                        {"param1": "label1", "param2": 10},
                        {"param1": "label2", "param2": 20},
                    ],
                },
            },
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd)
        query1, query2 = sorted(result.queries, key=attrgetter("name"))
        assert query1.name == "q[params0]"
        assert query1.databases == ["db"]
        assert query1.metrics == [QueryMetric("m", ["l"])]
        assert query1.sql == "SELECT :param1 AS l, :param2 AS m"
        assert query1.parameters == {
            "param1": "label1",
            "param2": 10,
        }
        assert query2.name == "q[params1]"
        assert query2.databases == ["db"]
        assert query2.metrics == [QueryMetric("m", ["l"])]
        assert query2.sql == "SELECT :param1 AS l, :param2 AS m"
        assert query2.parameters == {
            "param1": "label2",
            "param2": 20,
        }

    def test_load_queries_section_with_wrong_parameters(self, write_config):
        """An error is raised if query parameters don't match."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary", "labels": ["l"]}},
            "queries": {
                "q": {
                    "interval": 10,
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT :param1 AS l, :param3 AS m",
                    "parameters": [
                        {"param1": "label1", "param2": 10},
                        {"param1": "label2", "param2": 20},
                    ],
                },
            },
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert (
            str(err.value)
            == 'Parameters for query "q[params0]" don\'t match those from SQL'
        )

    @pytest.mark.parametrize(
        "config,error_message",
        [
            (CONFIG_UNKNOWN_DBS, 'Unknown databases for query "q": db1, db2'),
            (CONFIG_UNKNOWN_METRICS, 'Unknown metrics for query "q": m1, m2'),
            (
                CONFIG_MISSING_DB_KEY,
                "Invalid config at queries/q1: 'databases' is a required property",
            ),
            (
                CONFIG_MISSING_METRIC_TYPE,
                "Invalid config at metrics/m: 'type' is a required property",
            ),
            (
                CONFIG_INVALID_METRIC_NAME,
                "Invalid config at metrics: 'is wrong' does not match any "
                "of the regexes: '^[a-zA-Z_][a-zA-Z0-9_]*$'",
            ),
            (
                CONFIG_INVALID_LABEL_NAME,
                "Invalid config at metrics/m/labels/0: 'wrong-name' does not "
                "match '^[a-zA-Z_][a-zA-Z0-9_]*$'",
            ),
            (
                CONFIG_INVALID_METRICS_PARAMS_DIFFERENT_KEYS,
                'Invalid parameters definition for query "q": '
                "parameters dictionaries must all have the same keys",
            ),
        ],
    )
    def test_configuration_incorrect(self, config, error_message, write_config):
        """An error is raised if configuration is incorrect."""
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert str(err.value) == error_message

    def test_load_queries_missing_interval_default_to_none(self, write_config):
        """If the interval is not specified, it defaults to None."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary"}},
            "queries": {
                "q": {"databases": ["db"], "metrics": ["m"], "sql": "SELECT 1"}
            },
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd)
        assert config.queries[0].interval is None

    @pytest.mark.parametrize(
        "interval,value",
        [
            (10, 10),
            ("10", 10),
            ("10s", 10),
            ("10m", 600),
            ("1h", 3600),
            ("1d", 3600 * 24),
            (None, None),
        ],
    )
    def test_load_queries_interval(self, interval, value, config_full, write_config):
        """The query interval can be specified with suffixes."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with config_file.open() as fd:
            config = load_config(fd)
        [query] = config.queries
        assert query.interval == value

    def test_load_queries_interval_not_specified(self, config_full, write_config):
        """If the interval is not specified, it's set to None."""
        del config_full["queries"]["q"]["interval"]
        config_file = write_config(config_full)
        with config_file.open() as fd:
            config = load_config(fd)
        [query] = config.queries
        assert query.interval is None

    @pytest.mark.parametrize("interval", ["1x", "wrong", "1.5m"])
    def test_load_queries_invalid_interval_string(
        self, interval, config_full, write_config
    ):
        """An invalid string query interval raises an error."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert (
            str(err.value)
            == f"Invalid config at queries/q/interval: '{interval}' is not of type 'integer'"
        )

    @pytest.mark.parametrize("interval", [0, -20])
    def test_load_queries_invalid_interval_number(
        self, interval, config_full, write_config
    ):
        """An invalid integer query interval raises an error."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd)
        assert (
            str(err.value)
            == f"Invalid config at queries/q/interval: {interval} is less than the minimum of 1"
        )
