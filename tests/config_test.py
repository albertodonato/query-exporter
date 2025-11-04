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


class TestLoadConfigWithAlerts:
    """测试包含告警功能的配置加载"""

    def test_load_with_alertmanager_and_alerts(
        self, write_config: ConfigWriter
    ) -> None:
        """测试包含 AlertManager 和告警规则的配置"""
        cfg = {
            "alertmanager": {
                "url": "https://alertmanager.example.com"
            },
            "databases": {
                "doris": {"dsn": "sqlite:///test.db"}
            },
            "metrics": {
                "quotagroup_rjob_megatron_core_trainer_count_over_time_10m": {
                    "type": "gauge",
                    "description": "megatron rjob log counter",
                    "labels": ["cluster", "rjob", "quotagroup", "creator", "container"]  # 列表
                },
                "syslog_bmc_other_transition_critical_5m_count": {
                    "type": "gauge", 
                    "description": "bmc other transition critical log counter",
                    "labels": ["hostname"]  # 列表
                }
            },
            "alerts": {
                "HighErrorRate": {
                    "severity": "P3",
                    "for": "10m", 
                    "summary": "服务故障",
                    "description": "错误率超过阈值",
                    "labels": ["team"],  # 改为列表
                    "annotations": {
                        "dashboard": "https://grafana.example.com/dashboard"
                    }
                }
            },
            "queries": {
                "megatron_log_count": {
                    "databases": ["doris"],
                    "interval": 60,
                    "metrics": ["quotagroup_rjob_megatron_core_trainer_count_over_time_10m"],
                    "sql": "SELECT 1 as value, 'cluster1' as cluster, 'rjob1' as rjob, 'quotagroup1' as quotagroup, 'creator1' as creator, 'container1' as container, 'ai-platform' as team"
                },
                "bm_log_count": {
                    "databases": ["doris"],
                    "interval": 60,
                    "metrics": ["syslog_bmc_other_transition_critical_5m_count"],
                    "sql": "SELECT 1 as value, 'host1' as hostname, 'ai-platform' as team" 
                },
                "alert_test": {
                    "databases": ["doris"],
                    "interval": 60,
                    "metrics": [],
                    "alerts": ["HighErrorRate"],
                    "sql": "SELECT 1 as value, 'ai-platform' as team"
                }
            },
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        # 验证 alertmanager 配置
        assert result.alertmanager is not None
        assert result.alertmanager.url == "https://alertmanager.example.com"
        
        # 验证 alerts 配置
        assert len(result.alerts) == 1
        assert "HighErrorRate" in result.alerts
        alert = result.alerts["HighErrorRate"]
        assert alert.labels == ["team"]  # 现在是列表
        assert alert.severity == "P3"
        assert alert.for_duration == "10m"
        assert alert.summary == "服务故障"
        assert alert.description == "错误率超过阈值"
        assert alert.annotations == {"dashboard": "https://grafana.example.com/dashboard"}
        
        # 验证 queries 配置
        assert "megatron_log_count" in result.queries
        assert "bm_log_count" in result.queries
        assert "alert_test" in result.queries
        
        # 验证查询中的 alerts 字段
        alert_test_query = result.queries["alert_test"]
        assert alert_test_query.alerts == ["HighErrorRate"]
        
    def test_load_with_unknown_alert_in_query(
        self, write_config: ConfigWriter
    ) -> None:
        """测试查询中引用未知告警规则时的错误"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge"}},
            "alerts": {
                "KnownAlert": {
                    "summary": "Known Alert"
                }
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "alerts": ["UnknownAlert"],  # 引用未知告警
                    "sql": "SELECT 1"
                }
            },
        }
        config_file = write_config(cfg)
        with pytest.raises(ConfigError) as err:
            load_config([config_file])
        assert 'Unknown alerts for query "q": UnknownAlert' in str(err.value)

    def test_load_optional_alertmanager(
        self, write_config: ConfigWriter
    ) -> None:
        """测试 AlertManager 配置是可选的"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge"}},
            "alerts": {
                "TestAlert": {
                    "summary": "Test Alert",
                    "severity": "P3"
                }
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT 1"
                }
            },
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        # 应该能正常加载，没有 alertmanager 部分
        assert "db" in result.databases
        assert "m" in result.metrics
        assert "TestAlert" in result.alerts
        assert "q" in result.queries
        # alertmanager 不在配置中是可以的
        assert result.alertmanager is None

    def test_load_multiple_alerts(
        self, write_config: ConfigWriter
    ) -> None:
        """测试多个告警规则配置"""
        cfg = {
            "alertmanager": {"url": "https://alertmanager.example.com"},
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {
                "error_count": {"type": "gauge", "labels": ["service"]},
                "latency": {"type": "gauge", "labels": ["endpoint"]}
            },
            "alerts": {
                "HighErrorRate": {
                    "severity": "P1",
                    "for": "5m",
                    "summary": "High Error Rate",
                    "description": "Error count exceeds 100"
                },
                "HighLatency": {
                    "severity": "P2", 
                    "for": "2m",
                    "summary": "High Latency",
                    "description": "Latency exceeds 1s"
                },
                "CriticalFailure": {
                    "severity": "P0",
                    "for": "1m", 
                    "summary": "Critical Failure",
                    "description": "Service is down"
                }
            },
            "queries": {
                "error_query": {
                    "databases": ["db"],
                    "metrics": ["error_count"],
                    "alerts": ["HighErrorRate", "CriticalFailure"],  # 多个告警
                    "sql": "SELECT 1 as value, 'api' as service"
                },
                "latency_query": {
                    "databases": ["db"],
                    "metrics": ["latency"], 
                    "alerts": ["HighLatency"],
                    "sql": "SELECT 1 as value, '/health' as endpoint"
                }
            },
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        # 验证 alerts 配置
        assert len(result.alerts) == 3
        assert "HighErrorRate" in result.alerts
        assert "HighLatency" in result.alerts
        assert "CriticalFailure" in result.alerts
        
        # 验证查询中的 alerts 字段
        error_query = result.queries["error_query"]
        assert error_query.alerts == ["HighErrorRate", "CriticalFailure"]
        
        latency_query = result.queries["latency_query"]
        assert latency_query.alerts == ["HighLatency"]

    # def test_load_alerts_with_complex_labels_and_annotations(
    #     self, write_config: ConfigWriter
    # ) -> None:
    #     """测试包含复杂标签和注解的告警配置"""
    #     cfg = {
    #         "databases": {"db": {"dsn": "sqlite://"}},
    #         "metrics": {"m": {"type": "gauge", "labels": "[l1, l2]"}},
    #         "alerts": {
    #             "ComplexAlert": {
    #                 "severity": "P1",
    #                 "for": "15m",
    #                 "summary": "Complex Alert Example",
    #                 "description": "This is a complex alert with multiple labels and annotations",
    #                 "labels": {
    #                     "team": "platform",
    #                     "service": "api-gateway",
    #                     "environment": "production",
    #                     "region": "us-east-1"
    #                 },
    #                 "annotations": {
    #                     "runbook": "https://example.com/runbook/complex-alert",
    #                     "dashboard": "https://grafana.example.com/dashboard/api-gateway",
    #                     "slack": "#alerts-platform",
    #                     "priority": "high"
    #                 }
    #             }
    #         },
    #         "queries": {
    #             "q": {
    #                 "databases": ["db"],
    #                 "metrics": ["m"],
    #                 "alerts": ["ComplexAlert"],
    #                 "sql": "SELECT 1"
    #             }
    #         },
    #     }
        
    #     config_file = write_config(cfg)
    #     result = load_config([config_file])
        
    #     alert = result.alerts["ComplexAlert"]
    #     assert alert.labels == {
    #         "team": "platform",
    #         "service": "api-gateway", 
    #         "environment": "production",
    #         "region": "us-east-1"
    #     }
    #     assert alert.annotations == {
    #         "runbook": "https://example.com/runbook/complex-alert",
    #         "dashboard": "https://grafana.example.com/dashboard/api-gateway",
    #         "slack": "#alerts-platform",
    #         "priority": "high"
    #     }

    def test_load_alerts_with_complex_labels_and_annotations(
        self, write_config: ConfigWriter
    ) -> None:
        """测试包含复杂标签和注解的告警配置"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {
                "m": {
                    "type": "gauge", 
                    "labels": ["l1", "l2"]  # 修复：改为列表而不是字符串
                }
            },
            "alerts": {
                "ComplexAlert": {
                    "severity": "P1",
                    "for": "15m",
                    "summary": "Complex Alert Example",
                    "description": "This is a complex alert with multiple labels and annotations",
                    "labels": ["team", "service", "environment", "region"],  # 修复：改为列表而不是字典
                    "annotations": {
                        "runbook": "https://example.com/runbook/complex-alert",
                        "dashboard": "https://grafana.example.com/dashboard/api-gateway",
                        "slack": "#alerts-platform",
                        "priority": "high"
                    }
                }
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "alerts": ["ComplexAlert"],
                    "sql": "SELECT 1 as value, 'platform' as team, 'api-gateway' as service, 'production' as environment, 'us-east-1' as region"
                }
            },
        }
        
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        alert = result.alerts["ComplexAlert"]
        # 现在 labels 应该是列表
        assert alert.labels == ["team", "service", "environment", "region"]
        assert alert.annotations == {
            "runbook": "https://example.com/runbook/complex-alert",
            "dashboard": "https://grafana.example.com/dashboard/api-gateway", 
            "slack": "#alerts-platform",
            "priority": "high"
        }

    def test_load_query_without_alerts_field(
        self, write_config: ConfigWriter
    ) -> None:
        """测试查询中没有 alerts 字段的情况（向后兼容）"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge"}},
            "alerts": {
                "TestAlert": {
                    "summary": "Test Alert"
                }
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    # 没有 alerts 字段
                    "sql": "SELECT 1"
                }
            },
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        # 应该能正常加载，alerts 字段默认为空列表
        query = result.queries["q"]
        assert query.alerts == []

    def test_load_empty_alerts_in_query(
        self, write_config: ConfigWriter
    ) -> None:
        """测试查询中 alerts 字段为空列表的情况"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge"}},
            "alerts": {
                "TestAlert": {
                    "summary": "Test Alert"
                }
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "alerts": [],  # 空列表
                    "sql": "SELECT 1"
                }
            },
        }
        config_file = write_config(cfg)
        result = load_config([config_file])
        
        # 空列表应该被接受
        query = result.queries["q"]
        assert query.alerts == []

    def test_load_multiple_files_with_alerts(
        self, write_config: ConfigWriter
    ) -> None:
        """测试从多个文件加载包含告警的配置"""
        file1 = write_config({
            "alertmanager": {"url": "https://alertmanager.example.com"},
            "databases": {"db1": {"dsn": "sqlite://"}}
        })
        file2 = write_config({
            "metrics": {
                "m1": {"type": "gauge", "labels": ["label1"]},
                "m2": {"type": "counter"}
            }
        })
        file3 = write_config({
            "alerts": {
                "Alert1": {"summary": "Alert 1", "severity": "P1"},
                "Alert2": {"summary": "Alert 2", "severity": "P2"}
            }
        })
        file4 = write_config({
            "queries": {
                "q1": {
                    "databases": ["db1"],
                    "metrics": ["m1"],
                    "alerts": ["Alert1"],
                    "sql": "SELECT 1"
                },
                "q2": {
                    "databases": ["db1"],
                    "metrics": ["m2"],
                    "alerts": ["Alert2"],
                    "sql": "SELECT 2"
                }
            }
        })
        
        config = load_config([file1, file2, file3, file4])
        
        # 验证所有配置都被正确合并
        assert config.alertmanager.url == "https://alertmanager.example.com"
        assert set(config.databases) == {"db1"}
        assert set(config.metrics) == {"m1", "m2"} | BUILTIN_METRICS
        assert set(config.alerts) == {"Alert1", "Alert2"}
        assert set(config.queries) == {"q1", "q2"}
        
        # 验证查询中的告警引用
        assert config.queries["q1"].alerts == ["Alert1"]
        assert config.queries["q2"].alerts == ["Alert2"]

    def test_load_duplicated_alerts_in_multiple_files(
        self, write_config: ConfigWriter
    ) -> None:
        """测试多个文件中重复的告警配置"""
        file1 = write_config({
            "alerts": {
                "DuplicateAlert": {"summary": "Alert from file1"}
            }
        })
        file2 = write_config({
            "alerts": {
                "DuplicateAlert": {"summary": "Alert from file2"}  # 重复的告警名
            }
        })
        
        with pytest.raises(ConfigError) as err:
            load_config([file1, file2])
        assert 'Duplicated entries in the "alerts" section: DuplicateAlert' in str(err.value)

    def test_warn_unused_alerts(
        self,
        log: StructuredLogCapture,
        write_config: ConfigWriter,
    ) -> None:
        """测试未使用的告警规则警告"""
        cfg = {
            "databases": {"db": {"dsn": "sqlite://"}},
            "metrics": {"m": {"type": "gauge"}},
            "alerts": {
                "UsedAlert": {"summary": "This alert is used"},
                "UnusedAlert1": {"summary": "This alert is not used"},
                "UnusedAlert2": {"summary": "This alert is also not used"}
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "alerts": ["UsedAlert"],  # 只使用了一个告警
                    "sql": "SELECT 1"
                }
            },
        }
        config_file = write_config(cfg)
        load_config([config_file])
        
        # 应该警告有两个未使用的告警
        assert log.has(
            "unused config entries",
            section="alerts", 
            entries=["UnusedAlert1", "UnusedAlert2"]
        )
        
# 运行所有配置测试
# python3 -m pytest tests/config_test.py -v
# python3 -m pytest tests/config_test.py::TestLoadConfig -v

# 运行新的告警相关测试
# python3 -m pytest tests/config_test.py::TestLoadConfigWithAlerts -v
# python3 -m pytest tests/config_test.py::TestLoadConfigWithAlerts::test_load_with_alertmanager_and_alerts -v
# python3 -m pytest tests/config_test.py::TestLoadConfigWithAlerts::test_load_optional_alertmanager -v

# 运行特定的测试
# python3 -m pytest tests/config_test.py::TestLoadConfigWithAlerts::test_load_with_alertmanager_and_alerts -v