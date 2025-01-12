from functools import cached_property
import random
import string
import typing as t

import sqlalchemy as sa

from .docker import DockerService


def random_password(length: int = 10) -> str:
    """Generate a random password."""
    return "".join(random.choice(string.hexdigits) for _ in range(length))


class DatabaseServer(DockerService):
    dialect: str
    password: str

    username = "query_exporter"
    database = "query_exporter"

    def post_init(self) -> None:
        self.password = random_password()
        self._metadata = sa.MetaData()

    @property
    def host_dsn(self) -> str:
        """The database connection string for connecting from the host."""
        return f"{self.dialect}://{self.username}:{self.password}@{self.host_ip}:{self.host_port}/{self.database}"

    @property
    def dsn(self) -> str:
        """The database connection string for connecting from the container network."""
        return f"{self.dialect}://{self.username}:{self.password}@{self.name}:{self.port}/{self.database}"

    def check_ready(self) -> bool:
        """Check if the database accepts queries."""
        try:
            self.execute("SELECT 1")
        except sa.exc.OperationalError:
            return False

        return True

    def execute(
        self,
        statement: str,
        params: dict[str, t.Any] | list[dict[str, t.Any]] | None = None,
    ) -> None:
        """Execute a query."""
        with self._engine.connect() as conn:
            with conn.begin():
                conn.execute(sa.text(statement), params)

    def make_table(
        self,
        table_name: str,
        metrics: t.Sequence[str],
        labels: t.Sequence[str] = (),
    ) -> None:
        """Add a table to the database for specified metrics."""
        sa.Table(
            table_name,
            self._metadata,
            *(sa.Column(name, sa.Integer) for name in metrics),
            *(sa.Column(name, sa.Text) for name in labels),
        )
        self._metadata.create_all(self._engine)

    def drop_tables(self) -> None:
        """Drop created tables."""
        self._metadata.drop_all(self._engine)
        self._metadata = sa.MetaData()

    def insert_values(
        self, table_name: str, values: list[tuple[str | int, ...]]
    ) -> None:
        table = self._metadata.tables[table_name]
        columns = [column.name for column in table.columns]
        with self._engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    table.insert(), [dict(zip(columns, v)) for v in values]
                )

    @cached_property
    def _engine(self) -> sa.Engine:
        return sa.create_engine(self.host_dsn)


class PostgreSQL(DatabaseServer):
    name = "postgresql"
    image = "postgres"
    port = 5432

    dialect = "postgresql+psycopg2"

    def docker_config(self) -> dict[str, t.Any]:
        return super().docker_config() | {
            "environment": {
                "POSTGRES_USER": self.username,
                "POSTGRES_PASSWORD": self.password,
                "POSTGRES_DB": self.database,
            },
            "volumes": [
                {
                    "type": "tmpfs",
                    "target": "/var/lib/postgresql/data",
                },
            ],
            "command": "-F",
        }


class MySQL(DatabaseServer):
    name = "mysql"
    image = "mysql"
    port = 3306

    dialect = "mysql+mysqldb"

    def docker_config(self) -> dict[str, t.Any]:
        return super().docker_config() | {
            "environment": {
                "MYSQL_USER": self.username,
                "MYSQL_PASSWORD": self.password,
                "MYSQL_ROOT_PASSWORD": self.password,
                "MYSQL_DATABASE": self.database,
            },
            "volumes": [
                {
                    "type": "tmpfs",
                    "target": "/var/lib/mysql",
                },
            ],
        }


DATABASE_SERVERS = {server.name: server for server in (PostgreSQL, MySQL)}
