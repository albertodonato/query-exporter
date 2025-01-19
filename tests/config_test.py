from pathlib import Path
import typing as t

import pytest
from pytest_structlog import StructuredLogCapture

from query_exporter.config import (
    ConfigError,
    _get_parameter_sets,
    _resolve_dsn,
    load_config,
)
from query_exporter.db import QueryMetric
from query_exporter.metrics import (
    BUILTIN_METRICS,
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
)

from .conftest import ConfigWriter

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
    def test_load_invalid(self, tmp_path: Path) -> None:
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("foo: !env UNSET")
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert "variable UNSET undefined" in str(err.value)

    def test_load_not_mapping(
        self, tmp_path: Path, write_config: ConfigWriter
    ) -> None:
        config_file = write_config(["a", "b", "c"])
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value) == f"File content is not a mapping: {config_file}"
        )

    def test_load_databases_section(self, write_config: ConfigWriter) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        config = load_config([config_file])
        assert {"db1", "db2"} == set(config.databases)
        database1 = config.databases["db1"]
        database2 = config.databases["db2"]
        assert database1.name == "db1"
        assert database1.dsn == "sqlite:///foo"
        assert database1.keep_connected
        assert database1.autocommit
        assert database2.name == "db2"
        assert database2.dsn == "sqlite:///bar"
        assert not database2.keep_connected
        assert not database2.autocommit

    def test_load_databases_dsn_from_env(
        self,
        log: StructuredLogCapture,
        write_config: ConfigWriter,
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        config = load_config([config_file], env={"FOO": "sqlite://"})
        assert config.databases["db1"].dsn == "sqlite://"
        assert log.has(
            "deprecated DSN source 'env:FOO', use '!env FOO' instead"
        )

    def test_load_databases_missing_dsn(
        self, write_config: ConfigWriter
    ) -> None:
        cfg: dict[str, t.Any] = {
            "databases": {"db1": {}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == "Invalid config at databases/db1: 'dsn' is a required property"
        )

    def test_load_databases_invalid_dsn(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "invalid"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == 'Invalid database DSN: "invalid"'

    def test_load_databases_dsn_details(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        config = load_config([config_file])
        assert config.databases["db1"].dsn == "sqlite:///path/to/file"

    def test_load_databases_dsn_details_only_dialect(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        config = load_config([config_file])
        assert config.databases["db1"].dsn == "sqlite://"

    def test_load_databases_dsn_invalid_env(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "env:NOT-VALID"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == 'Invalid variable name: "NOT-VALID"'

    def test_load_databases_dsn_undefined_env(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "env:FOO"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file], env={})
        assert str(err.value) == 'Undefined variable: "FOO"'

    def test_load_databases_dsn_from_file(
        self,
        tmp_path: Path,
        log: StructuredLogCapture,
        write_config: ConfigWriter,
    ) -> None:
        dsn = "sqlite:///foo"
        dsn_path = tmp_path / "dsn"
        dsn_path.write_text(dsn)
        cfg = {
            "databases": {"db1": {"dsn": f"file:{dsn_path}"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        config = load_config([config_file])
        assert config.databases["db1"].dsn == dsn
        assert log.has(
            f"deprecated DSN source 'file:{dsn_path}', use '!file {dsn_path}' instead"
        )

    def test_load_databases_dsn_from_file_not_found(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "file:/not/found"}},
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Unable to read dsn file : "/not/found": No such file or directory'
        )

    def test_load_databases_no_labels(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {
                "db": {
                    "dsn": "sqlite://",
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        db = result.databases["db"]
        assert db.labels == {}

    def test_load_databases_labels(self, write_config: ConfigWriter) -> None:
        cfg = {
            "databases": {
                "db": {
                    "dsn": "sqlite://",
                    "labels": {"label1": "value1", "label2": "value2"},
                }
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        db = result.databases["db"]
        assert db.labels == {"label1": "value1", "label2": "value2"}

    def test_load_databases_labels_not_all_same(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file], env={})
        assert str(err.value) == "Not all databases define the same labels"

    def test_load_databases_connect_sql(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {
                "db": {
                    "dsn": "sqlite://",
                    "connect-sql": ["SELECT 1", "SELECT 2"],
                },
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        result = load_config([config_file], env={})
        assert result.databases["db"].connect_sql == ["SELECT 1", "SELECT 2"]

    def test_load_metrics_section(self, write_config: ConfigWriter) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        result = load_config([config_file])
        metric1 = result.metrics["metric1"]
        assert metric1.type == "summary"
        assert metric1.description == "metric one"
        assert metric1.labels == ("database", "label1", "label2")
        assert metric1.config == {"expiration": 120}
        metric2 = result.metrics["metric2"]
        assert metric2.type == "histogram"
        assert metric2.description == "metric two"
        assert metric2.labels == ("database",)
        assert metric2.config == {
            "buckets": [10, 100, 1000],
            "expiration": None,
        }
        metric3 = result.metrics["metric3"]
        assert metric3.type == "enum"
        assert metric3.description == "metric three"
        assert metric3.labels == ("database",)
        assert metric3.config == {
            "states": ["on", "off"],
            "expiration": 100,
        }
        # builtin metrics
        assert result.metrics.get(DB_ERRORS_METRIC_NAME) is not None
        assert result.metrics.get(QUERIES_METRIC_NAME) is not None

    def test_load_metrics_overlap_reserved_label(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge", "labels": ["database"]}},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Labels for metric "m" overlap with reserved/database ones: database'
        )

    def test_load_metrics_overlap_database_label(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "sqlite://", "labels": {"l1": "v1"}}},
            "metrics": {"m": {"type": "gauge", "labels": ["l1"]}},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Labels for metric "m" overlap with reserved/database ones: l1'
        )

    @pytest.mark.parametrize("builtin_metric_name", list(BUILTIN_METRICS))
    def test_load_metrics_reserved_name(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        builtin_metric_name: str,
    ) -> None:
        sample_config["metrics"][builtin_metric_name] = {"type": "counter"}
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == f'Label name "{builtin_metric_name} is reserved for builtin metric'
        )

    def test_load_metrics_unsupported_type(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db1": {"dsn": "sqlite://"}},
            "metrics": {
                "metric1": {"type": "info", "description": "info metric"}
            },
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == (
            "Invalid config at metrics/metric1/type: 'info' is not one of "
            "['counter', 'enum', 'gauge', 'histogram', 'summary']"
        )

    def test_load_queries_section(self, write_config: ConfigWriter) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        result = load_config([config_file])
        assert len(result.queries) == 2
        query1 = result.queries["q1"]
        assert query1.name == "q1"
        assert query1.databases == ["db1"]
        assert query1.metrics == [QueryMetric("m1", ["l1", "l2"])]
        assert query1.sql == "SELECT 1"
        assert len(query1.executions) == 1
        query2 = result.queries["q2"]
        assert query2.name == "q2"
        assert query2.databases == ["db2"]
        assert query2.metrics == [QueryMetric("m2", [])]
        assert query2.sql == "SELECT 2"
        assert len(query2.executions) == 1

    def test_load_queries_section_with_parameters(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        result = load_config([config_file])
        assert len(result.queries) == 1
        query = result.queries["q"]
        assert query.name == "q"
        assert query.databases == ["db"]
        assert query.metrics == [QueryMetric("m", ["l"])]
        assert query.sql == "SELECT :param1 AS l, :param2 AS m"
        query_exec1, query_exec2 = query.executions
        assert query_exec1.name == "q[params1]"
        assert query_exec1.parameters == {
            "param1": "label1",
            "param2": 10,
        }
        assert query_exec2.name == "q[params2]"
        assert query_exec2.parameters == {
            "param1": "label2",
            "param2": 20,
        }

    def test_load_queries_section_with_parameters_matrix(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        result = load_config([config_file])
        assert len(result.queries) == 1
        query = result.queries["q"]
        assert query.databases == ["db"]
        assert query.metrics == [QueryMetric("m", ["l"])]
        assert (
            query.sql == "SELECT :marketplace__name AS l, :item__status AS m"
        )

        query_exec1, query_exec2, query_exec3, query_exec4 = query.executions
        assert query_exec1.name == "q[params1]"
        assert query_exec1.parameters == {
            "marketplace__name": "amazon",
            "item__status": "active",
        }
        assert query_exec2.name == "q[params2]"
        assert query_exec2.parameters == {
            "marketplace__name": "ebay",
            "item__status": "active",
        }
        assert query_exec3.name == "q[params3]"
        assert query_exec3.parameters == {
            "marketplace__name": "amazon",
            "item__status": "inactive",
        }
        assert query_exec4.name == "q[params4]"
        assert query_exec4.parameters == {
            "marketplace__name": "ebay",
            "item__status": "inactive",
        }

    def test_load_queries_section_with_wrong_parameters(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Parameters for query "q[params1]" don\'t match those from SQL'
        )

    def test_load_queries_section_with_schedule_and_interval(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Invalid schedule for query "q": both interval and schedule specified'
        )

    def test_load_queries_section_invalid_schedule(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
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
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == 'Invalid schedule for query "q": invalid schedule format'
        )

    def test_load_queries_section_timeout(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        sample_config["queries"]["q"]["timeout"] = 2.0
        config_file = write_config(sample_config)
        result = load_config([config_file])
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
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        timeout: float,
        error_message: str,
    ) -> None:
        sample_config["queries"]["q"]["timeout"] = timeout
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
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
    def test_configuration_incorrect(
        self,
        write_config: ConfigWriter,
        config: dict[str, t.Any],
        error_message: str,
    ) -> None:
        config_file = write_config(config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == error_message

    def test_configuration_warning_unused(
        self,
        log: StructuredLogCapture,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        sample_config["databases"]["db2"] = {"dsn": "sqlite://"}
        sample_config["databases"]["db3"] = {"dsn": "sqlite://"}
        sample_config["metrics"]["m2"] = {"type": "gauge"}
        sample_config["metrics"]["m3"] = {"type": "gauge"}
        config_file = write_config(sample_config)
        load_config([config_file])
        assert log.has(
            "unused config entries",
            section="databases",
            entries=["db2", "db3"],
        )
        assert log.has(
            "unused config entries", section="metrics", entries=["m2", "m3"]
        )

    def test_load_queries_missing_interval_default_to_none(
        self, write_config: ConfigWriter
    ) -> None:
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "summary"}},
            "queries": {
                "q": {"databases": ["db"], "metrics": ["m"], "sql": "SELECT 1"}
            },
        }
        config_file = write_config(cfg)
        config = load_config([config_file])
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
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        interval: str | int | None,
        value: int | None,
    ) -> None:
        sample_config["queries"]["q"]["interval"] = interval
        config_file = write_config(sample_config)
        config = load_config([config_file])
        assert config.queries["q"].interval == value

    def test_load_queries_interval_not_specified(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        del sample_config["queries"]["q"]["interval"]
        config_file = write_config(sample_config)
        config = load_config([config_file])
        assert config.queries["q"].interval is None

    @pytest.mark.parametrize("interval", ["1x", "wrong", "1.5m"])
    def test_load_queries_invalid_interval_string(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        interval: str,
    ) -> None:
        sample_config["queries"]["q"]["interval"] = interval
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == (
            "Invalid config at queries/q/interval: "
            f"'{interval}' does not match '^[0-9]+[smhd]?$'"
        )

    @pytest.mark.parametrize("interval", [0, -20])
    def test_load_queries_invalid_interval_number(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        interval: int,
    ) -> None:
        sample_config["queries"]["q"]["interval"] = interval
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == f"Invalid config at queries/q/interval: {interval} is less than the minimum of 1"
        )

    def test_load_queries_no_metrics(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        sample_config["queries"]["q"]["metrics"] = []
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == "Invalid config at queries/q/metrics: [] should be non-empty"
        )

    def test_load_queries_no_databases(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        sample_config["queries"]["q"]["databases"] = []
        config_file = write_config(sample_config)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert (
            str(err.value)
            == "Invalid config at queries/q/databases: [] should be non-empty"
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
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        expiration: str | int | None,
        value: int | None,
    ) -> None:
        sample_config["metrics"]["m"]["expiration"] = expiration
        config_file = write_config(sample_config)
        config = load_config([config_file])
        assert config.metrics["m"].config["expiration"] == value

    def test_load_multiple_files(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        file_full = write_config(sample_config)
        file1 = write_config({"databases": sample_config["databases"]})
        file2 = write_config({"metrics": sample_config["metrics"]})
        file3 = write_config({"queries": sample_config["queries"]})
        assert load_config([file1, file2, file3]) == load_config([file_full])

    def test_load_multiple_files_combine(
        self,
        write_config: ConfigWriter,
    ) -> None:
        file1 = write_config(
            {
                "databases": {"db1": {"dsn": "sqlite://"}},
                "metrics": {"m1": {"type": "gauge"}},
                "queries": {
                    "q1": {
                        "databases": ["db1"],
                        "metrics": ["m1"],
                        "sql": "SELECT 1 AS m1",
                    }
                },
            }
        )
        file2 = write_config(
            {
                "databases": {"db2": {"dsn": "sqlite://"}},
                "metrics": {"m2": {"type": "gauge"}},
                "queries": {
                    "q2": {
                        "databases": ["db2"],
                        "metrics": ["m2"],
                        "sql": "SELECT 2 AS m2",
                    }
                },
            }
        )
        config = load_config([file1, file2])
        assert set(config.databases) == {"db1", "db2"}
        assert set(config.metrics) == {"m1", "m2"} | BUILTIN_METRICS
        assert set(config.queries) == {"q1", "q2"}

    def test_load_multiple_files_duplicated_database(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        file1 = write_config(sample_config)
        file2 = write_config({"databases": sample_config["databases"]})
        with pytest.raises(ConfigError) as err:
            load_config([file1, file2])
        assert (
            str(err.value)
            == 'Duplicated entries in the "databases" section: db'
        )

    def test_load_multiple_files_duplicated_metric(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        file1 = write_config(sample_config)
        file2 = write_config({"metrics": sample_config["metrics"]})
        with pytest.raises(ConfigError) as err:
            load_config([file1, file2])
        assert (
            str(err.value) == 'Duplicated entries in the "metrics" section: m'
        )

    def test_load_multiple_files_duplicated_query(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        file1 = write_config(sample_config)
        file2 = write_config({"queries": sample_config["queries"]})
        with pytest.raises(ConfigError) as err:
            load_config([file1, file2])
        assert (
            str(err.value) == 'Duplicated entries in the "queries" section: q'
        )


class TestResolveDSN:
    def test_all_details(self) -> None:
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

    def test_db_as_path(self) -> None:
        details = {
            "dialect": "sqlite",
            "database": "/path/to/file",
        }
        assert _resolve_dsn(details, {}) == "sqlite:///path/to/file"

    def test_encode_user_password(self) -> None:
        details = {
            "dialect": "postgresql",
            "user": "us%r",
            "password": "my pass",
            "host": "dbsever",
            "database": "/mydb",
        }
        assert (
            _resolve_dsn(details, {})
            == "postgresql://us%25r:my+pass@dbsever/mydb"
        )

    def test_encode_options(self) -> None:
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


class TestGetParameterSets:
    def test_list(self) -> None:
        params: list[dict[str, t.Any]] = [
            {
                "param1": 100,
                "param2": "foo",
            },
            {
                "param1": 200,
                "param2": "bar",
            },
        ]
        assert list(_get_parameter_sets(params)) == params

    def test_dict(self) -> None:
        params: dict[str, list[dict[str, t.Any]]] = {
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
        assert list(_get_parameter_sets(params)) == [
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
