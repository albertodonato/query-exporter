from .conftest import DatabaseServer, Exporter, ServiceHandler


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
