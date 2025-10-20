from enum import StrEnum
from functools import reduce
from itertools import product
import re
import typing as t

from pydantic import (
    AfterValidator,
    AliasGenerator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)
from sqlalchemy import exc
from sqlalchemy.engine import URL, make_url


def _validate_unique_items(items: list[t.Any]) -> list[t.Any]:
    """Validate that there are no duplicate items in the list."""
    assert sorted(items) == sorted(set(items)), (
        "must not contain duplicate items"
    )
    return items


def _validate_sorted(items: list[t.Any]) -> list[t.Any]:
    """Validate that items in the list are sorted."""
    assert items == sorted(items), "items must be sorted"
    return items


_INTERVAL_RE = re.compile(r"^[0-9]+[smhd]?$")


def _validate_interval(interval: int | str) -> int:
    """Convert a time interval to seconds.

    Return None if no interval is specified.

    """
    multiplier = 1
    if isinstance(interval, str):
        assert _INTERVAL_RE.match(interval), "invalid interval definition"
        # convert to seconds
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 3600 * 24}
        suffix = interval[-1]
        if suffix in multipliers:
            interval = interval[:-1]
            multiplier = multipliers[suffix]

    value = int(interval) * multiplier
    assert value > 0, "must be a positive number"
    return value


Buckets = t.Annotated[
    list[float],
    Field(min_length=1),
    AfterValidator(_validate_unique_items),
    AfterValidator(_validate_sorted),
]
Label = t.Annotated[
    str,
    Field(pattern="^[a-zA-Z_][a-zA-Z0-9_]*$"),
]
TimeInterval = t.Annotated[
    int,
    BeforeValidator(_validate_interval),
]
Timeout = t.Annotated[
    float,
    Field(
        gt=0,
        multiple_of=0.1,
    ),
]
Port = t.Annotated[
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


DSN = t.Annotated[str | DSNDetails, AfterValidator(_validate_dsn)]


class BuiltinMetric(Model):
    """Configuration for a builtin metric"""

    buckets: Buckets

    def config(self) -> dict[str, t.Any]:
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


class Parallel(Model):
    """Parallel execution configuration."""

    enabled: bool = False
    pool_size: t.Annotated[int, Field(ge=1)] = 5
    max_overflow: t.Annotated[int, Field(ge=0)] = 10


class Database(Model):
    """Database connection configuration."""

    dsn: DSN
    autocommit: bool = True
    keep_connected: bool = True
    connect_sql: list[str] = Field(min_length=1, default_factory=list)
    labels: dict[Label, str] = Field(min_length=1, default_factory=dict)


class Metric(Model):
    """A metric definition."""

    type: MetricType
    description: str = ""
    labels: list[Label] = Field(min_length=1, default_factory=list)
    buckets: Buckets | None = None
    states: (
        t.Annotated[
            list[str],
            Field(min_length=1),
            AfterValidator(_validate_unique_items),
        ]
        | None
    ) = None
    expiration: TimeInterval | None = None
    increment: bool = False

    @model_validator(mode="after")
    def validate_all(self) -> t.Self:
        if self.states and not self.type == MetricType.ENUM:
            raise ValueError("states can only be set for enum metrics")
        if self.increment and not self.type == MetricType.COUNTER:
            raise ValueError("increment can only be set for counter metrics")
        return self

    @property
    def config(self) -> dict[str, t.Any]:
        """The metric configuration."""
        return self.model_dump(
            exclude={"type", "description", "labels"},
            exclude_none=True,
            exclude_defaults=True,
        )


def _validate_query_paramters(
    parameters: list[dict[str, t.Any]] | dict[str, list[dict[str, t.Any]]],
) -> list[dict[str, t.Any]]:
    """Return an sequence of set of paramters with their values."""
    if isinstance(parameters, list):
        return parameters

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
    # return a list of merged dictionaries from each combination of the two
    # sets
    return list(
        reduce(lambda p1, p2: {**p1, **p2}, params)
        for params in product(*flattened_params)
    )


QueryParameters = t.Annotated[
    t.Annotated[list[dict[str, t.Any]], Field(min_length=1)]
    | t.Annotated[dict[str, list[dict[str, t.Any]]], Field(min_length=1)],
    AfterValidator(_validate_query_paramters),
]


class Query(Model):
    """A database query definition."""

    databases: t.Annotated[
        list[str], Field(min_length=1), AfterValidator(_validate_unique_items)
    ]
    metrics: t.Annotated[
        list[str], Field(min_length=1), AfterValidator(_validate_unique_items)
    ]
    sql: str
    interval: TimeInterval | None = None
    parameters: QueryParameters | None = None
    schedule: str | None = None
    timeout: Timeout | None = None


class Configuration(Model):
    """Exporter configuration."""

    builtin_metrics: BuiltinMetrics | None = None
    parallel: Parallel | None = None
    databases: dict[str, Database]
    metrics: dict[Label, Metric]
    queries: dict[str, Query]
