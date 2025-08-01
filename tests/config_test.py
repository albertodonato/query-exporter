from pathlib import Path
import typing as t

import pytest
from pytest_structlog import StructuredLogCapture

from query_exporter.config import (
    ConfigError,
    load_config,
)
from query_exporter.db import QueryMetric
from query_exporter.metrics import (
    BUILTIN_METRICS,
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
)

from .conftest import ConfigWriter


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

    def test_load_databases_unknown_dialect(
        self,
        write_config: ConfigWriter,
    ) -> None:
        cfg = {
            "databases": {
                "db": {
                    "dsn": "postgresql://foo",
                    "labels": {"label1": "value1", "label2": "value2"},
                },
            },
            "metrics": {},
            "queries": {},
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert str(err.value) == 'Module "psycopg2" not found'

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
            load_config([config_file])
        assert str(err.value) == "Not all databases define the same labels"

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
            "buckets": [10.0, 100.0, 1000.0],
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
        "config,error_message",
        [
            (
                {
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
                },
                'Unknown databases for query "q": db1, db2',
            ),
            (
                {
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
                },
                'Unknown metrics for query "q": m1, m2',
            ),
            (
                {
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
                },
                'Invalid parameters definition for query "q": '
                "parameters dictionaries must all have the same keys",
            ),
            (
                {
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
                },
                'Invalid parameters definition for query "q": '
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

    def test_load_builtin_metrics_query_latency_default_buckets(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        config_file = write_config(sample_config)
        config = load_config([config_file])
        assert config.metrics["query_latency"].config == {}

    def test_load_builtin_metrics_query_latency_custom_buckets(
        self,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
    ) -> None:
        sample_config["builtin-metrics"] = {
            "query_latency": {
                "buckets": [0.1, 0.5, 1.0, 5.0],
            },
        }
        config_file = write_config(sample_config)
        config = load_config([config_file])
        assert config.metrics["query_latency"].config == {
            "buckets": [0.1, 0.5, 1.0, 5.0],
        }
