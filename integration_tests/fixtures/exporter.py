from collections.abc import Iterator
from pathlib import Path
import typing as t

from prometheus_client.parser import text_string_to_metric_families
import pytest
import requests
import yaml

from .docker import DockerService, ServiceHandler


class Exporter(DockerService):
    """Wrapper to run the exporter process."""

    name = "query-exporter"
    image = "query-exporter:integration-tests"
    port = 9560

    def __init__(
        self,
        container_prefix: str,
        host_ip: str,
        host_port: int,
        config_dir: Path,
    ) -> None:
        super().__init__(container_prefix, host_ip, host_port)
        self.config_dir = config_dir
        self.url = f"http://{self.host_ip}:{self.host_port}"
        self.configure({"databases": {}, "metrics": {}, "queries": {}})

    def docker_config(self) -> dict[str, t.Any]:
        return super().docker_config() | {
            "environment": {
                "QE_LOG_LEVEL": "debug",
            },
            "build": str(Path(".").absolute()),
            "volumes": [f"{self.config_dir}:/config"],
        }

    def check_ready(self) -> bool:
        try:
            return self._get("/").ok
        except (requests.HTTPError, requests.ConnectionError):
            return False

    def configure(self, config: dict[str, t.Any]) -> None:
        """Write exporter configuration."""
        path = self.config_dir / "config.yaml"
        path.write_text(yaml.dump(config), "utf-8")

    def get_metrics(self) -> dict[str, dict[tuple[str, ...], float]]:
        """Return parsed metrics."""
        payload = self._get("/metrics").text

        metrics: dict[str, dict[tuple[str, ...], float]] = {}
        for family in text_string_to_metric_families(payload):
            for sample in family.samples:
                labels = tuple(
                    sample.labels[label] for label in sorted(sample.labels)
                )
                metrics.setdefault(family.name, {})[labels] = sample.value

        return metrics

    def _get(self, path: str) -> requests.Response:
        response = requests.get(self.url + path)
        response.raise_for_status()
        return response


@pytest.fixture(scope="session")
def exporter_service(
    tmp_path_factory: pytest.TempPathFactory,
    unused_tcp_port_factory: t.Callable[[], int],
    docker_compose_project_name: str,
    docker_ip: str,
) -> Iterator[Exporter]:
    """The exporter service."""
    yield Exporter(
        docker_compose_project_name,
        docker_ip,
        unused_tcp_port_factory(),
        tmp_path_factory.mktemp("query-exporter"),
    )


@pytest.fixture
def exporter(
    exporter_service: Exporter, service_handler: ServiceHandler
) -> Iterator[Exporter]:
    """The query-exporter service to access in tests."""
    service_handler.wait(exporter_service)
    yield exporter_service
