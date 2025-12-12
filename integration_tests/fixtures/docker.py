from abc import ABC, abstractmethod
from collections.abc import Iterator
import typing as t

import pytest
from pytest_docker.plugin import DockerComposeExecutor, Services


class DockerService(ABC):
    name: str
    image: str
    port: int = 0

    startup_wait_timeout = 20.0

    def __init__(
        self,
        container_prefix: str,
        host_ip: str,
        host_port: int,
    ) -> None:
        self.container_prefix = container_prefix
        self.host_ip = host_ip
        self.host_port = host_port
        self.post_init()

    def post_init(self) -> None:
        """Add post-init configuration."""

    def docker_config(self) -> dict[str, t.Any]:
        """The docker-compose configuration for the service."""
        return {
            "container_name": f"{self.container_prefix}-{self.name}",
            "ports": [f"{self.host_port}:{self.port}"],
            "image": self.image,
        }

    @abstractmethod
    def check_ready(self) -> bool:
        """Return True if the service is ready."""


@pytest.fixture(scope="session")
def docker_compose(
    docker_compose_command: str,
    docker_compose_file: str,
    docker_compose_project_name: str,
    docker_setup: str | list[str],
    docker_cleanup: str | list[str],
) -> Iterator[DockerComposeExecutor]:
    """Docker compose executor."""
    yield DockerComposeExecutor(
        docker_compose_command,
        docker_compose_file,
        docker_compose_project_name,
    )


class ServiceHandler:
    """Handler starting and stopping a Docker service."""

    def __init__(self, executor: DockerComposeExecutor, services: Services):
        self._executor = executor
        self._services = services

    def start(self, service: DockerService) -> None:
        self._executor.execute(f"start {service.name}")
        self.wait(service)

    def stop(self, service: DockerService) -> None:
        self._executor.execute(f"stop {service.name}")

    def restart(self, service: DockerService) -> None:
        self._executor.execute(f"restart {service.name}")
        self.wait(service)

    def logs(self, service: DockerService) -> str:
        return str(self._executor.execute(f"logs {service.name}").decode())

    def wait(self, service: DockerService) -> None:
        try:
            self._services.wait_until_responsive(
                check=service.check_ready,
                timeout=service.startup_wait_timeout,
                pause=0.5,
            )
        except Exception:
            # on failure, print out logs from the container
            print(self.logs(service))
            raise


@pytest.fixture(scope="session")
def service_handler(
    docker_compose: DockerComposeExecutor,
    docker_services: Services,
) -> Iterator[ServiceHandler]:
    """Handler for managing service start/stop."""
    yield ServiceHandler(docker_compose, docker_services)
