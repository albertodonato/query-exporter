from pathlib import Path
from textwrap import dedent
import typing as t

import pytest
import yaml

from query_exporter.yaml import load_yaml


class TestLoadYAML:
    def test_load(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                a: b
                c: d
                """
            )
        )
        assert load_yaml(config) == {"a": "b", "c": "d"}

    @pytest.mark.parametrize("env_value", ["foo", 3, False, {"foo": "bar"}])
    def test_load_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, env_value: t.Any
    ) -> None:
        monkeypatch.setenv("FOO", yaml.dump(env_value))
        config = tmp_path / "config.yaml"
        config.write_text("x: !env FOO")
        assert load_yaml(config) == {"x": env_value}

    def test_load_env_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !env FOO")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml(config)
        assert "variable FOO undefined" in str(err.value)

    def test_load_file_relative_path(self, tmp_path: Path) -> None:
        (tmp_path / "foo.txt").write_text("some text")
        config = tmp_path / "config.yaml"
        config.write_text("x: !file foo.txt")
        assert load_yaml(config) == {"x": "some text"}

    def test_load_file_absolute_path(self, tmp_path: Path) -> None:
        text_file = tmp_path / "foo.txt"
        text_file.write_text("some text")
        config = tmp_path / "config.yaml"
        config.write_text(f"x: !file {text_file.absolute()!s}")
        assert load_yaml(config) == {"x": "some text"}

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !file not-here.txt")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml(config)
        assert f"file {tmp_path / 'not-here.txt'} not found" in str(err.value)

    def test_load_include_relative_path(self, tmp_path: Path) -> None:
        (tmp_path / "foo.yaml").write_text("foo: bar")
        config = tmp_path / "config.yaml"
        config.write_text("x: !include foo.yaml")
        assert load_yaml(config) == {"x": {"foo": "bar"}}

    def test_load_include_absolute_path(self, tmp_path: Path) -> None:
        other_file = tmp_path / "foo.yaml"
        other_file.write_text("foo: bar")
        config = tmp_path / "config.yaml"
        config.write_text(f"x: !include {other_file.absolute()!s}")
        assert load_yaml(config) == {"x": {"foo": "bar"}}

    def test_load_include_multiple(self, tmp_path: Path) -> None:
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "bar.yaml").write_text("[a, b, c]")
        (subdir / "foo.yaml").write_text("foo: !include bar.yaml")
        config = tmp_path / "config.yaml"
        config.write_text("x: !include subdir/foo.yaml")
        assert load_yaml(config) == {"x": {"foo": ["a", "b", "c"]}}

    def test_load_include_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !include not-here.yaml")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml(config)
        assert f"file {tmp_path / 'not-here.yaml'} not found" in str(err.value)

class TestLoadYAMLWithAlerts:
    """测试包含告警功能的 YAML 配置加载"""
    
    def test_load_with_alertmanager(self, tmp_path: Path) -> None:
        """测试包含 AlertManager 配置的 YAML"""
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                alertmanager:
                  url: https://alertmanager.example.com
                
                databases:
                  doris:
                    dsn: sqlite:///test.db
                
                metrics:
                  test_metric:
                    type: gauge
                    description: test metric
                    labels: [cluster, service]
                
                alerts:
                  HighErrorRate:
                    severity: P1
                    for: 10m
                    summary: "High error rate detected"
                    description: "Error count exceeds threshold"
                    labels:
                      team: platform
                    annotations:
                      runbook: "https://example.com/runbook"
                
                queries:
                  test_query:
                    databases: [doris]
                    metrics: [test_metric]
                    alerts: [HighErrorRate]
                    sql: SELECT 1 as value, 'cluster1' as cluster, 'service1' as service
                """
            )
        )
        
        result = load_yaml(config)
        assert "alertmanager" in result
        assert result["alertmanager"]["url"] == "https://alertmanager.example.com"
        assert "alerts" in result
        assert "HighErrorRate" in result["alerts"]
        assert result["alerts"]["HighErrorRate"]["severity"] == "P1"
        assert "queries" in result
        assert "test_query" in result["queries"]
        assert result["queries"]["test_query"]["alerts"] == ["HighErrorRate"]

    def test_load_with_env_in_dsn(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """测试在 DSN 中使用 !env 标签"""
        monkeypatch.setenv("DORIS_DATABASE_DSN", "postgresql://user:pass@localhost/db")
        
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                alertmanager:
                  url: https://alertmanager.example.com
                
                databases:
                  doris:
                    dsn: !env DORIS_DATABASE_DSN
                
                metrics:
                  test_metric:
                    type: gauge
                    description: test metric
                
                alerts:
                  TestAlert:
                    summary: "Test Alert"
                    severity: P3
                
                queries:
                  test_query:
                    databases: [doris]
                    metrics: [test_metric]
                    alerts: [TestAlert]
                    sql: SELECT 1
                """
            )
        )
        
        result = load_yaml(config)
        assert result["databases"]["doris"]["dsn"] == "postgresql://user:pass@localhost/db"

    def test_load_with_file_in_sql(self, tmp_path: Path) -> None:
        """测试在 SQL 中使用 !file 标签"""
        # 创建 SQL 文件
        sql_dir = tmp_path / "doris" / "training"
        sql_dir.mkdir(parents=True)
        (sql_dir / "megatron_log_count.sql").write_text(
            "SELECT COUNT(*) as value, 'cluster1' as cluster FROM logs"
        )
        
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                databases:
                  doris:
                    dsn: sqlite:///test.db
                
                metrics:
                  quotagroup_rjob:megatron_core_trainer:count_over_time_10m:
                    type: gauge
                    description: megatron rjob log counter
                    labels: [cluster, rjob, quotagroup, creator, container]
                
                alerts:
                  HighErrorRate:
                    summary: "High Error Rate"
                    severity: P2
                    for: 5m
                
                queries:
                  megatron_log_count:
                    databases: [doris]
                    metrics: [quotagroup_rjob:megatron_core_trainer:count_over_time_10m]
                    alerts: [HighErrorRate]
                    sql: !file doris/training/megatron_log_count.sql
                """
            )
        )
        
        result = load_yaml(config)
        expected_sql = "SELECT COUNT(*) as value, 'cluster1' as cluster FROM logs"
        assert result["queries"]["megatron_log_count"]["sql"] == expected_sql

    def test_load_with_include_in_alerts(self, tmp_path: Path) -> None:
        """测试在 alerts 中使用 !include 标签"""
        # 创建被包含的告警配置文件
        alerts_dir = tmp_path / "alerts"
        alerts_dir.mkdir()
        (alerts_dir / "error_alerts.yaml").write_text(
            dedent(
                """
                HighErrorRate:
                  severity: P1
                  for: 10m
                  summary: "High Error Rate"
                  description: "Error count exceeds 100 in 5 minutes"
                  labels:
                    team: sre
                CriticalErrorRate:
                  severity: P0  
                  for: 2m
                  summary: "Critical Error Rate"
                  description: "Error count exceeds 1000 in 2 minutes"
                """
            )
        )
        
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                alertmanager:
                  url: https://alertmanager.example.com
                
                databases:
                  doris:
                    dsn: sqlite:///test.db
                
                metrics:
                  error_count:
                    type: gauge
                    description: error count
                    labels: [service]
                
                alerts: !include alerts/error_alerts.yaml
                
                queries:
                  error_query:
                    databases: [doris]
                    metrics: [error_count]
                    alerts: [HighErrorRate, CriticalErrorRate]
                    sql: SELECT COUNT(*) as value, 'api' as service FROM errors
                """
            )
        )
        
        result = load_yaml(config)
        assert "HighErrorRate" in result["alerts"]
        assert "CriticalErrorRate" in result["alerts"]
        assert result["alerts"]["HighErrorRate"]["severity"] == "P1"
        assert result["alerts"]["CriticalErrorRate"]["severity"] == "P0"
        assert result["queries"]["error_query"]["alerts"] == ["HighErrorRate", "CriticalErrorRate"]

    def test_load_complex_configuration(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """测试复杂的完整配置，包含所有新功能"""
        # 设置环境变量
        monkeypatch.setenv("DORIS_DATABASE_DSN", "postgresql://user:pass@localhost/prod")
        
        # 创建 SQL 文件
        training_dir = tmp_path / "doris" / "training"
        training_dir.mkdir(parents=True)
        (training_dir / "megatron_log_count.sql").write_text(
            "SELECT COUNT(*) as value, cluster, rjob, quotagroup, creator, container FROM megatron_logs"
        )
        
        bmc_dir = tmp_path / "doris" / "bmc"
        bmc_dir.mkdir(parents=True)
        (bmc_dir / "other_transition_critical_5m.sql").write_text(
            "SELECT COUNT(*) as value, hostname FROM bmc_logs WHERE level = 'CRITICAL'"
        )
        
        alerts_dir = tmp_path / "doris" / "alerts"
        alerts_dir.mkdir(parents=True)
        (alerts_dir / "test_alert.sql").write_text(
            "SELECT 1 as value, 'test' as label"
        )
        
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                alertmanager:
                  url: https://alertmanager.example.com
                
                databases:
                  doris:
                    dsn: !env DORIS_DATABASE_DSN
                
                metrics:
                  quotagroup_rjob:megatron_core_trainer:count_over_time_10m:
                    type: gauge
                    description: megatron rjob log counter
                    labels: [cluster, rjob, quotagroup, creator, container]
                  syslog:bmc:other_transition_critical:5m_count:
                    type: gauge
                    description: "bmc other transition critical log counter"
                    labels: [hostname]
                
                alerts:
                  HighErrorRate:
                    severity: P3
                    for: 10m
                    summary: "服务故障"
                    description: "错误率超过阈值"
                    labels:
                      team: ai-platform
                    annotations:
                      dashboard: "https://grafana.example.com/dashboard"
                
                queries:
                  megatron_log_count:
                    databases: [doris]
                    interval: 60
                    metrics: [quotagroup_rjob:megatron_core_trainer:count_over_time_10m]
                    sql: !file doris/training/megatron_log_count.sql
                  bm_log_count:
                    databases: [doris]
                    interval: 60
                    metrics: [syslog:bmc:other_transition_critical:5m_count]
                    sql: !file doris/bmc/other_transition_critical_5m.sql
                  alert_test:
                    databases: [doris]
                    interval: 60
                    alerts: [HighErrorRate]
                    sql: !file doris/alerts/test_alert.sql
                """
            )
        )
        
        result = load_yaml(config)
        
        # 验证 alertmanager
        assert result["alertmanager"]["url"] == "https://alertmanager.example.com"
        
        # 验证数据库 DSN（环境变量）
        assert result["databases"]["doris"]["dsn"] == "postgresql://user:pass@localhost/prod"
        
        # 验证 metrics
        assert "quotagroup_rjob:megatron_core_trainer:count_over_time_10m" in result["metrics"]
        assert "syslog:bmc:other_transition_critical:5m_count" in result["metrics"]
        
        # 验证 alerts
        assert "HighErrorRate" in result["alerts"]
        assert result["alerts"]["HighErrorRate"]["severity"] == "P3"
        assert result["alerts"]["HighErrorRate"]["summary"] == "服务故障"
        
        # 验证 queries
        assert "megatron_log_count" in result["queries"]
        assert "bm_log_count" in result["queries"]
        assert "alert_test" in result["queries"]
        
        # 验证 SQL 文件内容被正确加载
        assert "SELECT COUNT(*) as value, cluster, rjob, quotagroup, creator, container FROM megatron_logs" in result["queries"]["megatron_log_count"]["sql"]
        assert "SELECT COUNT(*) as value, hostname FROM bmc_logs WHERE level = 'CRITICAL'" in result["queries"]["bm_log_count"]["sql"]
        assert "SELECT 1 as value, 'test' as label" in result["queries"]["alert_test"]["sql"]
        
        # 验证 alerts 字段
        assert result["queries"]["alert_test"]["alerts"] == ["HighErrorRate"]

    def test_load_optional_alertmanager(self, tmp_path: Path) -> None:
        """测试 AlertManager 配置是可选的"""
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                databases:
                  test_db:
                    dsn: sqlite:///test.db
                
                metrics:
                  test_metric:
                    type: counter
                    description: test counter
                
                alerts:
                  TestAlert:
                    summary: "Test Alert"
                    severity: P3
                
                queries:
                  test_query:
                    databases: [test_db]
                    metrics: [test_metric]
                    sql: SELECT 1
                """
            )
        )
        
        result = load_yaml(config)
        # 应该能正常加载，没有 alertmanager 部分
        assert "databases" in result
        assert "metrics" in result
        assert "alerts" in result
        assert "queries" in result
        # alertmanager 不在配置中是可以的
        assert "alertmanager" not in result
        
# 运行所有 YAML 测试
# python3 -m pytest tests/yaml_test.py -v

# 运行新的告警相关测试
# python3 -m pytest tests/yaml_test.py::TestLoadYAMLWithAlerts -v

# 运行特定的测试
# python3 -m pytest tests/yaml_test.py::TestLoadYAMLWithAlerts::test_load_complex_configuration -v