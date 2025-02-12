from collections.abc import Iterator
import typing as t

import pytest
import yaml

from .fixtures.databases import DATABASE_SERVERS, DatabaseServer
from .fixtures.docker import (
    ServiceHandler,
    docker_compose,
    service_handler,
)
from .fixtures.exporter import Exporter, exporter, exporter_service

__all__ = [
    "DatabaseServer",
    "Exporter",
    "ServiceHandler",
    "docker_compose",
    "exporter",
    "exporter_service",
    "service_handler",
]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--databases",
        help="DB engine to run tests on",
        nargs="+",
        choices=list(DATABASE_SERVERS),
        default=list(DATABASE_SERVERS),
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    metafunc.parametrize(
        "db_server_name",
        metafunc.config.getoption("--databases"),
    )


@pytest.fixture(scope="session")
def selected_db_servers(
    request: pytest.FixtureRequest,
    unused_tcp_port_factory: t.Callable[[], int],
    docker_compose_project_name: str,
    docker_ip: str,
) -> Iterator[dict[str, DatabaseServer]]:
    """Map server names to helper class to interact with them."""
    yield {
        name: DATABASE_SERVERS[name](
            docker_compose_project_name,
            docker_ip,
            unused_tcp_port_factory(),
        )
        for name in request.config.getoption("--databases")
    }


@pytest.fixture(scope="session")
def selected_db_servers_services(
    selected_db_servers: dict[str, DatabaseServer],
) -> Iterator[dict[str, dict[str, t.Any]]]:
    """Configuration stanzas for docker-compose services."""
    yield {
        server.name: server.docker_config()
        for server in selected_db_servers.values()
    }


@pytest.fixture(autouse=True)
def skip_if_not_selected_db_server(
    request: pytest.FixtureRequest,
    db_server_name: str,
) -> None:
    """Skip test if narkers exclude the current database server."""
    if marker := request.node.get_closest_marker("database_only"):
        if db_server_name not in marker.args:
            pytest.skip("Database server excluded")
    if marker := request.node.get_closest_marker("database_exclude"):
        if db_server_name in marker.args:
            pytest.skip("Database server excluded")


@pytest.fixture(scope="session")
def docker_compose_file(
    tmp_path_factory: pytest.TempPathFactory,
    selected_db_servers_services: dict[str, dict[str, t.Any]],
    exporter_service: Exporter,
) -> Iterator[str]:
    """Path to docker-compose.yaml config file."""
    config_path = (
        tmp_path_factory.mktemp("docker-compose") / "docker-compose.yml"
    )

    services = selected_db_servers_services
    services[exporter_service.name] = exporter_service.docker_config()

    config = {"services": services}
    with config_path.open("w") as fd:
        yaml.dump(config, fd)
    yield str(config_path)


@pytest.fixture
def db_server(
    selected_db_servers: dict[str, DatabaseServer],
    db_server_name: str,
    service_handler: ServiceHandler,
) -> Iterator[DatabaseServer]:
    server = selected_db_servers[db_server_name]
    service_handler.wait(server)
    yield server
    server.drop_tables()
