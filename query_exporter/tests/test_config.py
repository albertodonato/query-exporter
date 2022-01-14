import logging

import pytest
import yaml

from ..config import (
    _get_parameters_sets,
    _resolve_dsn,
    ConfigError,
    DB_ERRORS_METRIC_NAME,
    GLOBAL_METRICS,
    load_config,
    QUERIES_METRIC_NAME,
)
from ..db import QueryMetric


@pytest.fixture
def logger(caplog):
    with caplog.at_level("DEBUG"):
        yield logging.getLogger()
    caplog.clear()


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
                "sql": "SELECT 1 as m",
            }
        },
    }


@pytest.fixture
def write_config(tmp_path):
    path = tmp_path / "config"

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

CONFIG_INVALID_METRICS_PARAMS_MATRIX_DIFFERENT_KEYS = {
    "databases": {"db": {"dsn": "sqlite://"}},
    "metrics": {"m": {"type": "gauge"}},
    "queries": {
        "q": {
            "interval": 10,
            "databases": ["db"],
            "metrics": ["m"],
            "sql": "SELECT :param AS m",
            "parameters": {"a": [{"foo": 1}, {"bar": 2}]},
        },
    },
}


class TestLoadConfig:
    def test_load_databases_section(self, logger, write_config):
        """The 'databases' section is loaded from the config file."""
        config = {
            "databases": {
                "db1": {"dsn": "sqlite:///foo"},
                "db2": {
                    "dsn": "sqlite:///bar",
                    "keep-connected": False,
                    "autocommit": False,
                },
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger)
        assert {"db1", "db2"} == set(result.databases)
        database1 = result.databases["db1"]
        database2 = result.databases["db2"]
        assert database1.name == "db1"
        assert database1.dsn == "sqlite:///foo"
        assert database1.keep_connected
        assert database1.autocommit
        assert database2.name == "db2"
        assert database2.dsn == "sqlite:///bar"
        assert not database2.keep_connected
        assert not database2.autocommit

    def test_load_databases_dsn_from_env(self, logger, write_config):
        """The database DSN can be loaded from env."""
        config = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd, logger, env={"FOO": "sqlite://"})
        assert config.databases["db1"].dsn == "sqlite://"

    def test_load_databases_missing_dsn(self, logger, write_config):
        """An error is raised if the 'dsn' key is missing for a database."""
        config = {"databases": {"db1": {}}, "metrics": {}, "queries": {}}
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == "Invalid config at databases/db1: 'dsn' is a required property"
        )

    def test_load_databases_invalid_dsn(self, logger, write_config):
        """An error is raised if the DSN is invalid."""
        config = {
            "databases": {"db1": {"dsn": "invalid"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == 'Invalid database DSN: "invalid"'

    def test_load_databases_dsn_details(self, logger, write_config):
        """The DSN can be specified as a dictionary."""
        config = {
            "databases": {
                "db1": {
                    "dsn": {
                        "dialect": "sqlite",
                        "database": "/path/to/file",
                    }
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.databases["db1"].dsn == "sqlite:///path/to/file"

    def test_load_databases_dsn_details_only_dialect(self, logger, write_config):
        """Only the "dialect" key is required in DSN."""
        config = {
            "databases": {
                "db1": {
                    "dsn": {
                        "dialect": "sqlite",
                    }
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.databases["db1"].dsn == "sqlite://"

    def test_load_databases_dsn_invalid_env(self, logger, write_config):
        """An error is raised if the DSN from environment is invalid."""
        config = {
            "databases": {"db1": {"dsn": "env:NOT-VALID"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == 'Invalid variable name: "NOT-VALID"'

    def test_load_databases_dsn_undefined_env(self, logger, write_config):
        """An error is raised if the environ variable for DSN is undefined."""
        config = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger, env={})
        assert str(err.value) == 'Undefined variable: "FOO"'

    def test_load_databases_dsn_from_file(self, tmp_path, logger, write_config):
        """The database DSN can be loaded from a file."""
        dsn = "sqlite:///foo"
        dsn_path = tmp_path / "dsn"
        dsn_path.write_text(dsn)
        config = {
            "databases": {"db1": {"dsn": f"file:{dsn_path}"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.databases["db1"].dsn == dsn

    def test_load_databases_dsn_from_file_not_found(self, logger, write_config):
        """An error is raised if the DSN file can't be read."""
        config = {
            "databases": {"db1": {"dsn": "file:/not/found"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == 'Unable to read dsn file : "/not/found": No such file or directory'
        )

    def test_load_databases_no_labels(self, logger, write_config):
        """If no labels are defined, an empty dict is returned."""
        config = {
            "databases": {
                "db": {
                    "dsn": "sqlite://",
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger)
        db = result.databases["db"]
        assert db.labels == {}

    def test_load_databases_labels(self, logger, write_config):
        """Labels can be defined for databases."""
        config = {
            "databases": {
                "db": {
                    "dsn": "sqlite://",
                    "labels": {"label1": "value1", "label2": "value2"},
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger)
        db = result.databases["db"]
        assert db.labels == {"label1": "value1", "label2": "value2"}

    def test_load_databases_labels_not_all_same(self, logger, write_config):
        """If not all databases have the same labels, an error is raised."""
        config = {
            "databases": {
                "db1": {
                    "dsn": "sqlite://",
                    "labels": {"label1": "value1", "label2": "value2"},
                },
                "db2": {
                    "dsn": "sqlite://",
                    "labels": {"label2": "value2", "label3": "value3"},
                },
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger, env={})
        assert str(err.value) == "Not all databases define the same labels"

    def test_load_databases_connect_sql(self, logger, write_config):
        """Databases can have queries defined to run on connection."""
        config = {
            "databases": {
                "db": {"dsn": "sqlite://", "connect-sql": ["SELECT 1", "SELECT 2"]},
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger, env={})
        assert result.databases["db"].connect_sql == ["SELECT 1", "SELECT 2"]

    def test_load_metrics_section(self, logger, write_config):
        """The 'metrics' section is loaded from the config file."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
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
                    "buckets": [10, 100, 1000],
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
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger)
        metric1 = result.metrics["metric1"]
        assert metric1.type == "summary"
        assert metric1.description == "metric one"
        assert metric1.config == {
            "labels": ["database", "label1", "label2"],
            "expiration": 120,
        }
        metric2 = result.metrics["metric2"]
        assert metric2.type == "histogram"
        assert metric2.description == "metric two"
        assert metric2.config == {
            "labels": ["database"],
            "buckets": [10, 100, 1000],
            "expiration": None,
        }
        metric3 = result.metrics["metric3"]
        assert metric3.type == "enum"
        assert metric3.description == "metric three"
        assert metric3.config == {
            "labels": ["database"],
            "states": ["on", "off"],
            "expiration": 100,
        }
        # global metrics
        assert result.metrics.get(DB_ERRORS_METRIC_NAME) is not None
        assert result.metrics.get(QUERIES_METRIC_NAME) is not None

    def test_load_metrics_overlap_reserved_label(self, logger, write_config):
        """An error is raised if reserved labels are used."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge", "labels": ["database"]}},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == 'Labels for metric "m" overlap with reserved/database ones: database'
        )

    def test_load_metrics_overlap_database_label(self, logger, write_config):
        """An error is raised if database labels are used for metrics."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://", "labels": {"l1": "v1"}}},
            "metrics": {"m": {"type": "gauge", "labels": ["l1"]}},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == 'Labels for metric "m" overlap with reserved/database ones: l1'
        )

    @pytest.mark.parametrize("global_name", list(GLOBAL_METRICS))
    def test_load_metrics_reserved_name(self, config_full, write_config, global_name):
        """An error is raised if a reserved label name is used."""
        config_full["metrics"][global_name] = {"type": "counter"}
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == f'Label name "{global_name} is reserved for builtin metric'
        )

    def test_load_metrics_unsupported_type(self, logger, write_config):
        """An error is raised if an unsupported metric type is passed."""
        config = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {"metric1": {"type": "info", "description": "info metric"}},
            "queries": {},
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == (
            "Invalid config at metrics/metric1/type: 'info' is not one of "
            "['counter', 'enum', 'gauge', 'histogram', 'summary']"
        )

    def test_load_queries_section(self, logger, write_config):
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
            result = load_config(fd, logger)
        query1 = result.queries["q1"]
        assert query1.name == "q1"
        assert query1.databases == ["db1"]
        assert query1.metrics == [QueryMetric("m1", ["l1", "l2"])]
        assert query1.sql == "SELECT 1"
        assert query1.parameters == {}
        query2 = result.queries["q2"]
        assert query2.name == "q2"
        assert query2.databases == ["db2"]
        assert query2.metrics == [QueryMetric("m2", [])]
        assert query2.sql == "SELECT 2"
        assert query2.parameters == {}

    def test_load_queries_section_with_parameters(self, logger, write_config):
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
            result = load_config(fd, logger)
        query1 = result.queries["q[params0]"]
        assert query1.name == "q[params0]"
        assert query1.databases == ["db"]
        assert query1.metrics == [QueryMetric("m", ["l"])]
        assert query1.sql == "SELECT :param1 AS l, :param2 AS m"
        assert query1.parameters == {
            "param1": "label1",
            "param2": 10,
        }
        query2 = result.queries["q[params1]"]
        assert query2.name == "q[params1]"
        assert query2.databases == ["db"]
        assert query2.metrics == [QueryMetric("m", ["l"])]
        assert query2.sql == "SELECT :param1 AS l, :param2 AS m"
        assert query2.parameters == {
            "param1": "label2",
            "param2": 20,
        }

    def test_load_queries_section_with_parameters_matrix(self, logger, write_config):
        """Queries can have parameters matrix."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary", "labels": ["l"]}},
            "queries": {
                "q": {
                    "interval": 10,
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT :marketplace__name AS l, :item__status AS m",
                    "parameters": {
                        "marketplace": [{"name": "amazon"}, {"name": "ebay"}],
                        "item": [{"status": "active"}, {"status": "inactive"}],
                    },
                },
            },
        }
        config_file = write_config(config)
        with config_file.open() as fd:
            result = load_config(fd, logger)

        assert len(result.queries) == 4

        # check common props for each query
        for query_name, query in result.queries.items():
            assert query.databases == ["db"]
            assert query.metrics == [QueryMetric("m", ["l"])]
            assert query.sql == "SELECT :marketplace__name AS l, :item__status AS m"

        # Q1
        query1 = result.queries["q[params0]"]
        assert query1.name == "q[params0]"
        assert query1.parameters == {
            "marketplace__name": "amazon",
            "item__status": "active",
        }
        # Q2
        query2 = result.queries["q[params1]"]
        assert query2.name == "q[params1]"
        assert query2.parameters == {
            "marketplace__name": "ebay",
            "item__status": "active",
        }
        # Q3
        query3 = result.queries["q[params2]"]
        assert query3.name == "q[params2]"
        assert query3.parameters == {
            "marketplace__name": "amazon",
            "item__status": "inactive",
        }
        # Q4
        query4 = result.queries["q[params3]"]
        assert query4.name == "q[params3]"
        assert query4.parameters == {
            "marketplace__name": "ebay",
            "item__status": "inactive",
        }

    def test_load_queries_section_with_wrong_parameters(self, logger, write_config):
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
            load_config(fd, logger)
        assert (
            str(err.value)
            == 'Parameters for query "q[params0]" don\'t match those from SQL'
        )

    def test_load_queries_section_with_schedule_and_interval(
        self, logger, write_config
    ):
        """An error is raised if query schedule and interval are both present."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary"}},
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT 1",
                    "interval": 10,
                    "schedule": "0 * * * *",
                },
            },
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == 'Invalid schedule for query "q": both interval and schedule specified'
        )

    def test_load_queries_section_invalid_schedule(self, logger, write_config):
        """An error is raised if query schedule has wrong format."""
        config = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary"}},
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT 1",
                    "schedule": "wrong",
                },
            },
        }
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value) == 'Invalid schedule for query "q": invalid schedule format'
        )

    def test_load_queries_section_timeout(self, logger, config_full, write_config):
        """Query configuration can include a timeout."""
        config_full["queries"]["q"]["timeout"] = 2.0
        config_file = write_config(config_full)
        with config_file.open() as fd:
            result = load_config(fd, logger)
        query1 = result.queries["q"]
        assert query1.timeout == 2.0

    @pytest.mark.parametrize(
        "timeout,error_message",
        [
            (
                -1.0,
                "Invalid config at queries/q/timeout: -1.0 is less than or equal to the minimum of 0",
            ),
            (
                0,
                "Invalid config at queries/q/timeout: 0 is less than or equal to the minimum of 0",
            ),
            (
                0.02,
                "Invalid config at queries/q/timeout: 0.02 is not a multiple of 0.1",
            ),
        ],
    )
    def test_load_queries_section_invalid_timeout(
        self, logger, config_full, write_config, timeout, error_message
    ):
        """An error is raised if query timeout is invalid."""
        config_full["queries"]["q"]["timeout"] = timeout
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == error_message

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
                "of the regexes: '^[a-zA-Z_:][a-zA-Z0-9_:]*$'",
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
            (
                CONFIG_INVALID_METRICS_PARAMS_MATRIX_DIFFERENT_KEYS,
                'Invalid parameters definition by path "a" for query "q": '
                "parameters dictionaries must all have the same keys",
            ),
        ],
    )
    def test_configuration_incorrect(self, logger, write_config, config, error_message):
        """An error is raised if configuration is incorrect."""
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == error_message

    def test_configuration_warning_unused(
        self, caplog, logger, config_full, write_config
    ):
        """A warning is logged if unused entries are present in config."""
        config_full["databases"]["db2"] = {"dsn": "sqlite://"}
        config_full["databases"]["db3"] = {"dsn": "sqlite://"}
        config_full["metrics"]["m2"] = {"type": "gauge"}
        config_full["metrics"]["m3"] = {"type": "gauge"}
        config_file = write_config(config_full)
        with config_file.open() as fd:
            load_config(fd, logger)
        assert caplog.messages == [
            'unused entries in "databases" section: db2, db3',
            'unused entries in "metrics" section: m2, m3',
        ]

    def test_load_queries_missing_interval_default_to_none(self, logger, write_config):
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
            config = load_config(fd, logger)
        assert config.queries["q"].interval is None

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
    def test_load_queries_interval(
        self, logger, config_full, write_config, interval, value
    ):
        """The query interval can be specified with suffixes."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.queries["q"].interval == value

    def test_load_queries_interval_not_specified(
        self, logger, config_full, write_config
    ):
        """If the interval is not specified, it's set to None."""
        del config_full["queries"]["q"]["interval"]
        config_file = write_config(config_full)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.queries["q"].interval is None

    @pytest.mark.parametrize("interval", ["1x", "wrong", "1.5m"])
    def test_load_queries_invalid_interval_string(
        self, logger, config_full, write_config, interval
    ):
        """An invalid string query interval raises an error."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == f"Invalid config at queries/q/interval: '{interval}' is not of type 'integer'"
        )

    @pytest.mark.parametrize("interval", [0, -20])
    def test_load_queries_invalid_interval_number(
        self, logger, config_full, write_config, interval
    ):
        """An invalid integer query interval raises an error."""
        config_full["queries"]["q"]["interval"] = interval
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value)
            == f"Invalid config at queries/q/interval: {interval} is less than the minimum of 1"
        )

    def test_load_queries_no_metrics(self, logger, config_full, write_config):
        """An error is raised if no metrics are configured."""
        config_full["queries"]["q"]["metrics"] = []
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert str(err.value) == "Invalid config at queries/q/metrics: [] is too short"

    def test_load_queries_no_databases(self, logger, config_full, write_config):
        """An error is raised if no databases are configured."""
        config_full["queries"]["q"]["databases"] = []
        config_file = write_config(config_full)
        with pytest.raises(ConfigError) as err, config_file.open() as fd:
            load_config(fd, logger)
        assert (
            str(err.value) == "Invalid config at queries/q/databases: [] is too short"
        )

    @pytest.mark.parametrize(
        "expiration,value",
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
    def test_load_metrics_expiration(
        self, logger, config_full, write_config, expiration, value
    ):
        """The metric series expiration time can be specified with suffixes."""
        config_full["metrics"]["m"]["expiration"] = expiration
        config_file = write_config(config_full)
        with config_file.open() as fd:
            config = load_config(fd, logger)
        assert config.metrics["m"].config["expiration"] == value


class TestResolveDSN:
    def test_all_details(self):
        """The DSN can be specified as a dictionary."""
        details = {
            "dialect": "postgresql",
            "user": "user",
            "password": "secret",
            "host": "dbsever",
            "port": 1234,
            "database": "mydb",
            "options": {"foo": "bar", "baz": "bza"},
        }
        assert (
            _resolve_dsn(details, {})
            == "postgresql://user:secret@dbsever:1234/mydb?foo=bar&baz=bza"
        )

    def test_db_as_path(self):
        """If the db name is a path, it's treated accordingly."""
        details = {
            "dialect": "sqlite",
            "database": "/path/to/file",
        }
        assert _resolve_dsn(details, {}) == "sqlite:///path/to/file"

    def test_encode_user_password(self):
        """The user and password are encoded."""
        details = {
            "dialect": "postgresql",
            "user": "us%r",
            "password": "my pass",
            "host": "dbsever",
            "database": "/mydb",
        }
        assert _resolve_dsn(details, {}) == "postgresql://us%25r:my+pass@dbsever/mydb"

    def test_encode_options(self):
        """Option parmaeters are encoded."""
        details = {
            "dialect": "postgresql",
            "database": "/mydb",
            "options": {
                "foo": "a value",
                "bar": "another/value",
            },
        }
        assert (
            _resolve_dsn(details, {})
            == "postgresql:///mydb?foo=a+value&bar=another%2Fvalue"
        )


class TestGetParametersSets:
    def test_list(self):
        params = [
            {
                "param1": 100,
                "param2": "foo",
            },
            {
                "param1": 200,
                "param2": "bar",
            },
        ]
        assert list(_get_parameters_sets(params)) == params

    def test_dict(self):
        params = {
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
        }
        assert list(_get_parameters_sets(params)) == [
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
        ]
