from pathlib import Path
import typing as t

import pytest

from query_exporter.yaml import load_yaml

from .conftest import DatabaseServer, Exporter, ServiceHandler

pytestmark = pytest.mark.database_only("postgresql")


EXAMPLE_CONFIGS_DIR = Path("examples")


@pytest.fixture
def pg_stats_config(
    monkeypatch: pytest.MonkeyPatch, db_server: DatabaseServer
) -> t.Iterator[dict[str, t.Any]]:
    monkeypatch.setenv("PG_DATABASE_DSN", db_server.dsn)
    yield load_yaml(EXAMPLE_CONFIGS_DIR / "postgresql-stats" / "config.yaml")


def test_postgresql_stats_metrics(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
    pg_stats_config: dict[str, t.Any],
) -> None:
    exporter.import_config_dir(EXAMPLE_CONFIGS_DIR / "postgresql-stats")
    exporter.write_dotenv({"PG_DATABASE_DSN": db_server.dsn})
    service_handler.restart(exporter)

    # all queries have been executed successfully
    expected_query_results = {
        ("pg", query, "success"): 1.0 for query in pg_stats_config["queries"]
    }
    metrics = exporter.get_metrics()
    assert metrics["queries"] == expected_query_results
