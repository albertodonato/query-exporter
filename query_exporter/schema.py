from enum import StrEnum
from functools import reduce
from itertools import product
import re
from typing import Annotated, Any, Self, cast

from apscheduler.triggers.cron import CronTrigger
from pydantic import (
    AfterValidator,
    AliasGenerator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PrivateAttr,
    model_validator,
)
from sqlalchemy import TextClause, exc, text
from sqlalchemy.engine import URL, make_url


def _validate_unique_items(items: list[Any]) -> list[Any]:
    """Validate that there are no duplicate items in the list."""
    if sorted(items) != sorted(set(items)):
        raise ValueError("Must not contain duplicate items")
    return items


def _validate_sorted(items: list[Any]) -> list[Any]:
    """Validate that items in the list are sorted."""
    if items != sorted(items):
        raise ValueError("Items must be sorted")
    return items


_INTERVAL_RE = re.compile(r"^[0-9]+[smhd]?$")


def _validate_interval(interval: int | str) -> int:
    """Convert a time interval to seconds."""
    multiplier = 1
    if isinstance(interval, str):
        if not _INTERVAL_RE.match(interval):
            raise ValueError("Invalid interval definition")
        # convert to seconds
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 3600 * 24}
        suffix = interval[-1]
        if suffix in multipliers:
            interval = interval[:-1]
            multiplier = multipliers[suffix]

    value = int(interval) * multiplier
    if value <= 0:
        raise ValueError("Must be a positive number")
    return value


def _validate_schedule(schedule: str) -> str:
    """Check that the schedule is a valid Cron expression."""
    CronTrigger.from_crontab(schedule)
    return schedule


Buckets = Annotated[
    list[float],
    Field(min_length=1),
    AfterValidator(_validate_unique_items),
    AfterValidator(_validate_sorted),
]
Label = Annotated[
    str,
    Field(pattern="^[a-zA-Z_][a-zA-Z0-9_]*$"),
]
TimeInterval = Annotated[
    int,
    BeforeValidator(_validate_interval),
]
TimeSchedule = Annotated[
    str,
    BeforeValidator(_validate_schedule),
]
Timeout = Annotated[
    float,
    Field(
        gt=0,
        multiple_of=0.1,
    ),
]
Port = Annotated[
    int,
    Field(
        ge=1,
        le=65535,
    ),
]


class MetricType(StrEnum):
    """Supported metric types."""

    COUNTER = "counter"
    ENUM = "enum"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class Model(BaseModel):
    """Base schema model."""

    model_config = ConfigDict(
        # support dashed field names
        alias_generator=AliasGenerator(alias=lambda s: s.replace("_", "-")),
        # don't allow extra fields
        extra="forbid",
    )


class DSNDetails(Model):
    """DSN configuration details."""

    dialect: str
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: Port | None = None
    database: str | None = None
    options: dict[str, str | list[str]] = Field(default_factory=dict)


def _validate_dsn(dsn: str | dict[str, str | list[str]]) -> str:
    """Build the DSN string from its definition."""
    url: URL
    try:
        match dsn:
            case str():
                url = make_url(dsn)
            case DSNDetails():
                url = URL.create(
                    dsn.dialect,
                    username=dsn.user,
                    password=dsn.password,
                    host=dsn.host,
                    port=dsn.port,
                    database=dsn.database,
                    query=dsn.options,
                )
    except exc.ArgumentError:
        raise ValueError("Invalid DSN format")
    return url.render_as_string(hide_password=False)


DSN = Annotated[str | DSNDetails, AfterValidator(_validate_dsn)]


class BuiltinMetric(Model):
    """Configuration for a builtin metric"""

    buckets: Buckets

    def config(self) -> dict[str, Any]:
        """The metric configuration."""
        return self.model_dump()


class BuiltinMetrics(BaseModel):
    """Configuration for builtin metrics"""

    model_config = ConfigDict(extra="forbid")

    query_latency: BuiltinMetric | None = None

    def as_dict(self) -> dict[str, BuiltinMetric]:
        """Return builtin metrics as a dictionary keyed by metric name."""
        d = {}
        for name in self.__class__.model_fields:
            if value := getattr(self, name):
                d[name] = value
        return d


class ConnectionPool(Model):
    """Database connection pool configuration."""

    size: int = Field(ge=0, le=100, default=1)
    max_overflow: int = Field(ge=0, le=100, default=0)

    @model_validator(mode="after")
    def validate_all(self) -> Self:
        if self.max_overflow > 0 and self.size == 0:
            raise ValueError("Overflow can't be set with no connection pool")
        return self


class Database(Model):
    """Database connection configuration."""

    dsn: DSN
    connection_pool: ConnectionPool = Field(default_factory=ConnectionPool)
    connect_sql: list[str] = Field(min_length=1, default_factory=list)
    labels: dict[Label, str] = Field(min_length=1, default_factory=dict)


class Metric(Model):
    """A metric definition."""

    type: MetricType
    description: str = ""
    labels: list[Label] = Field(min_length=1, default_factory=list)
    buckets: Buckets | None = None
    states: (
        Annotated[
            list[str],
            Field(min_length=1),
            AfterValidator(_validate_unique_items),
        ]
        | None
    ) = None
    expiration: TimeInterval | None = None
    increment: bool = False

    @model_validator(mode="after")
    def validate_all(self) -> Self:
        if self.states and not self.type == MetricType.ENUM:
            raise ValueError("States can only be set for enum metrics")
        if self.increment and not self.type == MetricType.COUNTER:
            raise ValueError("Increment can only be set for counter metrics")
        return self

    @property
    def config(self) -> dict[str, Any]:
        """The metric configuration."""
        return self.model_dump(
            exclude={"type", "description", "labels"},
            exclude_none=True,
            exclude_defaults=True,
        )


QueryParametersList = list[dict[str, Any]]


def _validate_query_parameters(
    parameters: QueryParametersList | dict[str, QueryParametersList],
) -> QueryParametersList:
    """Return each set of parameters with their values."""
    if isinstance(parameters, list):
        params_list = parameters
    else:
        # first, flatten dict like
        #
        # {
        #     'a': [{'arg1': 1, 'arg2': 1}],
        #     'b': [{'arg1': 1, 'arg2': 1}],
        # }
        #
        # into a sequence like
        #
        # (
        #     [{'a__arg1': 1, 'a__arg2': 2}],
        #     [{'b__arg1': 1, 'b__arg2': 2}],
        # )
        flattened_params = (
            [
                {f"{top_key}__{key}": value for key, value in arg_set.items()}
                for arg_set in arg_sets
            ]
            for top_key, arg_sets in parameters.items()
        )
        # make a list of merged dictionaries from each combination of the two
        # sets
        params_list = list(
            reduce(lambda p1, p2: {**p1, **p2}, params)
            for params in product(*flattened_params)
        )

    param_sets = {tuple(sorted(param_set)) for param_set in params_list}
    if len(param_sets) > 1:
        raise ValueError("Not all parameter sets define the same names")
    return params_list


QueryParameters = Annotated[
    Annotated[QueryParametersList, Field(min_length=1)]
    | Annotated[dict[str, QueryParametersList], Field(min_length=1)],
    AfterValidator(_validate_query_parameters),
]


class Query(Model):
    """A database query definition."""

    databases: Annotated[
        list[str], Field(min_length=1), AfterValidator(_validate_unique_items)
    ]
    metrics: Annotated[
        list[str], Field(min_length=1), AfterValidator(_validate_unique_items)
    ]
    sql: str
    interval: TimeInterval | None = None
    parameters: QueryParameters | None = None
    schedule: TimeSchedule | None = None
    timeout: Timeout | None = None

    _statement: TextClause = PrivateAttr()

    @model_validator(mode="after")
    def validate_periodic(self) -> Self:
        if self.interval and self.schedule:
            raise ValueError("Can't set both interval and schedule")
        return self

    @model_validator(mode="after")
    def validate_parameters(self) -> Self:
        self._statement = text(self.sql)
        stmt_params = set(self._statement.compile().params)
        params = set(
            cast(QueryParametersList, self.parameters)[0]
            if self.parameters
            else ()
        )
        if params != stmt_params:
            raise ValueError("Query parameters don't match parameter set")
        return self


class ExporterConfig(Model):
    """Exporter configuration."""

    builtin_metrics: BuiltinMetrics | None = None
    databases: dict[str, Database]
    metrics: dict[Label, Metric]
    queries: dict[str, Query]
