from textwrap import dedent
import typing as t

import pytest

from .conftest import DatabaseServer, Exporter, ServiceHandler


@pytest.fixture
def mysql_stored_procedure(db_server: DatabaseServer) -> t.Iterator[str]:
    name = "my_proc"
    sql = dedent(
        f"""
        CREATE PROCEDURE {name}(IN table_name VARCHAR(255))
        BEGIN
          SET @sql = CONCAT('SELECT * FROM ', table_name);
          PREPARE stmt FROM @sql;
          EXECUTE stmt;
          DEALLOCATE PREPARE stmt;
        END
        """
    )
    db_server.execute(sql)
    yield name
    db_server.execute(f"DROP PROCEDURE IF EXISTS {name}")


@pytest.mark.database_only("mysql")
def test_mysql_stored_procedure(
    db_server: DatabaseServer,
    exporter: Exporter,
    service_handler: ServiceHandler,
    mysql_stored_procedure: str,
) -> None:
    table_name = "sample_table"
    db_server.make_table(table_name, ["m"], ["l"])
    db_server.insert_values(table_name, [(10, "foo"), (20, "bar")])
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
                    "sql": f"CALL {mysql_stored_procedure}('{table_name}')",
                },
            },
        }
    )
    service_handler.restart(exporter)

    metrics = exporter.get_metrics()
    assert metrics["m"] == {("db", "foo"): 10.0, ("db", "bar"): 20.0}
