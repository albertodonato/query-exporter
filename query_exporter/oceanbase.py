"""SQLAlchemy dialects for OceanBase MySQL tenants."""

from __future__ import annotations

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mysql import base, mysqldb, pymysql

# OceanBase reports its own release version (for example, ``4.2.x``) from
# VERSION().  SQLAlchemy interprets that as a MySQL version and rejects it
# because it is below the oldest MySQL version supported by the dialect.  A
# conservative MySQL-compatible version keeps SQLAlchemy's feature detection
# usable and selects the legacy isolation variable supported by OceanBase.
OCEANBASE_FALLBACK_SERVER_VERSION = (5, 7, 0)


class _OceanBaseVersionMixin:
    """Make SQLAlchemy's MySQL version parsing tolerant of OceanBase."""

    def _parse_server_version(
        self: base.MySQLDialect, val: str
    ) -> tuple[int, ...]:
        try:
            return base.MySQLDialect._parse_server_version(self, val)
        except NotImplementedError:
            # The base parser may have partially updated these attributes
            # before rejecting an OceanBase version.  Reset them so the
            # dialect is consistently treated as MySQL-compatible.
            self.is_mariadb = False
            self._mariadb_normalized_version_info = (
                OCEANBASE_FALLBACK_SERVER_VERSION
            )
            self.server_version_info = OCEANBASE_FALLBACK_SERVER_VERSION
            return OCEANBASE_FALLBACK_SERVER_VERSION


class OceanBaseDialect(_OceanBaseVersionMixin, pymysql.MySQLDialect_pymysql):
    """OceanBase dialect using the pure-Python PyMySQL driver."""

    name = "oceanbase"
    driver = "pymysql"


class OceanBaseDialectMySQLdb(
    _OceanBaseVersionMixin, mysqldb.MySQLDialect_mysqldb
):
    """OceanBase dialect using the mysqlclient (MySQLdb) driver."""

    name = "oceanbase"
    driver = "mysqldb"


def register_dialects() -> None:
    """Register native and compatibility OceanBase URL schemes."""
    registry.register("oceanbase", __name__, "OceanBaseDialect")
    registry.register("oceanbase.pymysql", __name__, "OceanBaseDialect")
    registry.register(
        "oceanbase.mysqldb", __name__, "OceanBaseDialectMySQLdb"
    )
    # Keep compatibility with the convention used by third-party
    # OceanBase SQLAlchemy dialects.
    registry.register("mysql.oceanbase", __name__, "OceanBaseDialect")
