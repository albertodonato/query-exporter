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
