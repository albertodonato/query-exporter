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
    username: str
    password: str

    database = "query_exporter"

    def post_init(self) -> None:
        self.password = random_password()
        self._metadata = sa.MetaData()

    @property
    def external_dsn(self) -> str:
        """The database connection string from outside the container network."""
        return f"{self.dialect}://{self.username}:{self.password}@localhost:{self.public_port}"

    @property
    def internal_dsn(self) -> str:
        """The database connection string from inside the container network."""
        return f"{self.dialect}://{self.username}:{self.password}@{self.name}:{self.port}"

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
        return sa.create_engine(self.external_dsn)


class PostgreSQL(DatabaseServer):
    name = "postgresql"
    image = "postgres"
    port = 5432

    dialect = "postgresql+psycopg2"
    username = "postgres"

    def docker_config(self) -> dict[str, t.Any]:
        return super().docker_config() | {
            "environment": {
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
    port = 33060

    dialect = "mysql+mysqldb"
    username = "mysql"

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
