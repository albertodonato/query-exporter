from collections.abc import Iterator

import pytest

from .conftest import DatabaseServer, Exporter, ServiceHandler


@pytest.fixture
def timestamp_query(db_server: DatabaseServer) -> Iterator[str]:
    queries = {
        "mssql": "SELECT DATEDIFF_BIG(SECOND, '1970-01-01 00:00:00', SYSDATETIME()) AS m",
        "mysql": "SELECT UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS m",
        "postgresql": "SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS m",
    }
    query = queries.get(db_server.name)
    assert query, f"Unsupported server {db_server.name}"
    yield query


def test_basic(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
) -> None:
    db_server.make_table("test", ["m"], ["l"])
    db_server.insert_values("test", [(1, "foo"), (2, "bar")])
    exporter.configure(
        {
            "databases": {
                "db": {"dsn": db_server.dsn},
            },
            "metrics": {
                "m": {
                    "type": "gauge",
                    "labels": ["l"],
                },
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT m, l FROM test",
                },
            },
        }
    )
    service_handler.restart(exporter)
    metrics = exporter.get_metrics()
    assert metrics["m"] == {("db", "foo"): 1.0, ("db", "bar"): 2.0}


def test_multiple_metrics(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
) -> None:
    db_server.make_table("test", ["m1", "m2"], ["l1", "l2", "l3"])
    db_server.insert_values(
        "test", [(10, 20, "a", "b", "c"), (100, 200, "x", "y", "z")]
    )
    exporter.configure(
        {
            "databases": {
                "db": {"dsn": db_server.dsn},
            },
            "metrics": {
                "m1": {
                    "type": "gauge",
                    "labels": ["l1", "l2"],
                },
                "m2": {
                    "type": "gauge",
                    "labels": ["l1", "l3"],
                },
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m1", "m2"],
                    "sql": "SELECT m1, m2, l1, l2, l3 FROM test",
                },
            },
        }
    )
    service_handler.restart(exporter)
    metrics = exporter.get_metrics()
    assert metrics["m1"] == {
        ("db", "a", "b"): 10.0,
        ("db", "x", "y"): 100.0,
    }
    assert metrics["m2"] == {
        ("db", "a", "c"): 20.0,
        ("db", "x", "z"): 200.0,
    }


def test_update_metrics(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
) -> None:
    db_server.make_table("test", ["m"], ["l"])
    db_server.insert_values("test", [(1, "foo"), (2, "bar")])
    exporter.configure(
        {
            "databases": {
                "db": {"dsn": db_server.dsn},
            },
            "metrics": {
                "m": {
                    "type": "gauge",
                    "labels": ["l"],
                },
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": "SELECT SUM(m) AS m, l FROM test GROUP BY l",
                },
            },
        }
    )
    service_handler.restart(exporter)
    metrics = exporter.get_metrics()
    assert metrics["m"] == {("db", "foo"): 1.0, ("db", "bar"): 2.0}

    db_server.insert_values("test", [(2, "foo"), (10, "bar")])
    updated_metrics = exporter.get_metrics()
    assert updated_metrics["m"] == {("db", "foo"): 3.0, ("db", "bar"): 12.0}


def test_db_connection_error(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
    timestamp_query: str,
) -> None:
    exporter.configure(
        {
            "databases": {
                "db": {"dsn": db_server.dsn},
            },
            "metrics": {
                "m": {
                    "type": "gauge",
                },
            },
            "queries": {
                "q": {
                    "databases": ["db"],
                    "metrics": ["m"],
                    "sql": timestamp_query,
                },
            },
        }
    )
    service_handler.restart(exporter)
    metrics = exporter.get_metrics()
    metric_value = metrics["m"]["db",]
    assert metrics["queries"] == {
        ("db", "q", "success"): 1.0,
    }
    service_handler.stop(db_server)
    metrics = exporter.get_metrics()
    # the failure is reported as database error
    assert metrics["database_errors"] == {
        ("db",): 1.0,
    }
    assert metrics["queries"] == {
        ("db", "q", "success"): 1.0,
    }
    service_handler.start(db_server)
    metrics = exporter.get_metrics()
    # no change in errors
    assert metrics["database_errors"] == {
        ("db",): 1.0,
    }
    # latest execution is a success
    assert metrics["queries"] == {
        ("db", "q", "success"): 2.0,
    }
    # metric has been updated
    assert metrics["m"]["db",] > metric_value
