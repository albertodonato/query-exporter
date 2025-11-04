from collections.abc import Iterator
from copy import deepcopy
import typing as t
from unittest import mock

from click.testing import CliRunner, Result
import pytest
from pytest_mock import MockerFixture

from query_exporter.main import QueryExporterScript

from .conftest import ConfigWriter, metric_values


@pytest.fixture
def mock_run_app(mocker: MockerFixture) -> Iterator[mock.MagicMock]:
    yield mocker.patch("prometheus_aioexporter._web.run_app")


@pytest.fixture
def script() -> Iterator[QueryExporterScript]:
    yield QueryExporterScript()


@pytest.fixture
def invoke_cli(
    mock_run_app: mock.MagicMock,
    script: QueryExporterScript,
) -> Iterator[t.Callable[..., Result]]:
    def invoke(*args: str) -> Result:
        return CliRunner().invoke(script.command, args)

    yield invoke


class TestQureyExporterScript:
    def test_run(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        config_file = write_config(sample_config)
        invoke_cli("--config", str(config_file))
        mock_run_app.assert_called_once()

    def test_run_check_only(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        config_file = write_config(sample_config)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 0
        mock_run_app.assert_not_called()

    def test_run_check_only_wrong_config(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        sample_config["extra"] = "stuff"
        config_file = write_config(sample_config)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 1
        mock_run_app.assert_not_called()

    def test_static_metrics_query_interval(
        self,
        mock_run_app: mock.MagicMock,
        script: QueryExporterScript,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        sample_config["queries"]["q2"] = deepcopy(
            sample_config["queries"]["q"]
        )
        sample_config["queries"]["q2"]["interval"] = 20
        config_file = write_config(sample_config)
        result = invoke_cli("--config", str(config_file))
        assert result.exit_code == 0
        metric = script.registry.get_metric("query_interval")
        assert metric_values(metric, by_labels=("query",)) == {
            ("q",): 10.0,
            ("q2",): 20.0,
        }


    # 新增的测试用例
    def test_config_with_alerts(
        self,
        mock_run_app: mock.MagicMock,
        sample_config_with_alerts: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        """测试包含 alerts 的配置能够正常加载"""
        config_file = write_config(sample_config_with_alerts)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 0
        mock_run_app.assert_not_called()

    def test_config_with_invalid_alert_reference(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        """测试引用不存在的 alert 时配置验证失败"""
        config = deepcopy(sample_config)
        config["alerts"] = {
            "existing_alert": {
                "severity": "P3",
                "for": "5m",
                "summary": "测试告警",
                "description": "测试描述",
                "labels": ["label1"]
            }
        }
        config["queries"]["invalid_alert_query"] = {
            "databases": ["db"],
            "interval": 30,
            "alerts": ["non_existing_alert"],  # 引用不存在的 alert
            "sql": "SELECT 'value1' as label1, 100 as value"
        }
        
        config_file = write_config(config)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 1  # 应该失败
        mock_run_app.assert_not_called()

    def test_query_with_both_metrics_and_alerts(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        """测试同时包含 metrics 和 alerts 的 query 配置"""
        config = deepcopy(sample_config)
        config["alerts"] = {
            "test_alert": {
                "severity": "P2",
                "for": "15m",
                "summary": "组合测试告警",
                "description": "同时包含 metrics 和 alerts",
                "labels": ["cluster", "service"]
            }
        }
        config["queries"]["combined_query"] = {
            "databases": ["db"],
            "interval": 60,
            "metrics": ["m"],  # 引用已有的 metric
            "alerts": ["test_alert"],
            "sql": "SELECT 'cluster1' as cluster, 'service1' as service, 42 as value"
        }
        
        config_file = write_config(config)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 0  # 应该成功
        mock_run_app.assert_not_called()

    def test_alert_configuration_validation(
        self,
        mock_run_app: mock.MagicMock,
        sample_config: dict[str, t.Any],
        write_config: ConfigWriter,
        invoke_cli: t.Callable[..., Result],
    ) -> None:
        """测试 alert 配置验证（缺少必需字段）"""
        config = deepcopy(sample_config)
        config["alerts"] = {
            "invalid_alert": {
                # 缺少 severity, for, summary 等必需字段
                "description": "不完整的告警配置"
            }
        }
        
        config_file = write_config(config)
        result = invoke_cli("--config", str(config_file), "--check-only")
        assert result.exit_code == 1  # 应该失败
        mock_run_app.assert_not_called()