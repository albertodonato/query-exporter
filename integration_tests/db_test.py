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
) -> None:
    match db_server.name:
        case "mysql":
            timestamp_expr = "UNIX_TIMESTAMP(CURRENT_TIMESTAMP())"
        case "postgresql":
            timestamp_expr = "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)"
        case _:
            raise Exception(f"unsupported server {db_server.name}")

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
                    "sql": f"SELECT {timestamp_expr} AS m",
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
    # the failure is reported
    assert metrics["queries"] == {
        ("db", "q", "success"): 1.0,
        ("db", "q", "error"): 1.0,
    }
    service_handler.start(db_server)
    metrics = exporter.get_metrics()
    # latest execution is a success
    assert metrics["queries"] == {
        ("db", "q", "success"): 2.0,
        ("db", "q", "error"): 1.0,
    }
    # metric has been updated
    assert metrics["m"]["db",] > metric_value
