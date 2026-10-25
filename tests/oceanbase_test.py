from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.pool import NullPool

from query_exporter.oceanbase import (
    OCEANBASE_FALLBACK_SERVER_VERSION,
    OceanBaseDialect,
    OceanBaseDialectMySQLdb,
    register_dialects,
)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("5.7.25-OceanBase 4.2.1", (5, 7, 25, 2, 1)),
        ("OceanBase 4.2.1.0", OCEANBASE_FALLBACK_SERVER_VERSION),
        ("OceanBase", OCEANBASE_FALLBACK_SERVER_VERSION),
    ],
)
def test_parse_server_version(version: str, expected: tuple[int, ...]) -> None:
    dialect = OceanBaseDialect()

    assert dialect._parse_server_version(version) == expected
    assert dialect.server_version_info == expected
    assert dialect.is_mariadb is False


def test_get_isolation_level_uses_oceanbase_compatible_variable() -> None:
    cursor = Mock()
    cursor.fetchone.return_value = ("READ-COMMITTED",)
    connection = Mock()
    connection.cursor.return_value = cursor
    dialect = OceanBaseDialect()
    dialect.server_version_info = OCEANBASE_FALLBACK_SERVER_VERSION

    assert dialect.get_isolation_level(connection) == "READ COMMITTED"
    cursor.execute.assert_called_once_with("SELECT @@tx_isolation")
    cursor.close.assert_called_once_with()


def test_register_dialects() -> None:
    register_dialects()

    assert registry.load("oceanbase") is OceanBaseDialect
    assert registry.load("oceanbase.pymysql") is OceanBaseDialect
    assert registry.load("oceanbase.mysqldb") is OceanBaseDialectMySQLdb
    assert registry.load("mysql.oceanbase") is OceanBaseDialect


def test_create_engine_with_native_url() -> None:
    engine = create_engine(
        "oceanbase://root%40test@localhost:2881/query_exporter",
        poolclass=NullPool,
    )

    assert isinstance(engine.dialect, OceanBaseDialect)
    engine.dispose()
