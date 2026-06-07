from typing import Any

from prometheus_aioexporter import DEFAULT_METRIC_TYPES
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Enum,
    Gauge,
    Histogram,
    Info,
    Summary,
)
from prometheus_client.exposition import generate_latest
from prometheus_client.metrics import MetricWrapperBase
import pytest

from query_exporter.builtin_metrics import BuiltinMetrics
from query_exporter.metrics import (
    TimestampedMixin,
    timestamped_metric,
    timestamped_metric_types,
)

METRIC_UPDATES = [
    pytest.param(Counter, {}, "inc", 3, id="counter"),
    pytest.param(Gauge, {}, "set", 3, id="gauge"),
    pytest.param(Histogram, {}, "observe", 3, id="histogram"),
    pytest.param(Summary, {}, "observe", 3, id="summary"),
    pytest.param(Enum, {"states": ["on", "off"]}, "state", "on", id="enum"),
    pytest.param(Info, {}, "info", {"version": "1.0"}, id="info"),
]


def make_metric(
    cls: type[MetricWrapperBase], options: dict[str, Any]
) -> TimestampedMixin:
    metric = timestamped_metric(cls)(
        "test",
        "test metric",
        labelnames=["l"],
        registry=CollectorRegistry(),
        **options,
    )
    assert isinstance(metric, TimestampedMixin)
    return metric


class TimestampedGauge(TimestampedMixin, Gauge):
    pass


def make_gauge() -> TimestampedGauge:
    return TimestampedGauge(
        "test", "test metric", labelnames=["l"], registry=CollectorRegistry()
    )


class TestBuiltinMetrics:
    def test_names(self) -> None:
        assert BuiltinMetrics.names() == frozenset(
            {
                "database_errors",
                "queries",
                "query_interval",
                "query_latency",
                "query_timestamp",
            }
        )

    def test_get_configs_return_all_builtin_metrics(self) -> None:
        configs = BuiltinMetrics.get_configs(frozenset(), {})
        assert set(configs) == BuiltinMetrics.names()

    def test_get_configs_add_extra_labels(self) -> None:
        configs = BuiltinMetrics.get_configs(frozenset({"database"}), {})
        for name, config in configs.items():
            if name == "query_interval":
                assert "database" not in config.labels
            else:
                assert "database" in config.labels

    def test_get_configs_query_interval_not_affected_by_extra_labels(
        self,
    ) -> None:
        configs_no_extra = BuiltinMetrics.get_configs(frozenset(), {})
        configs_with_extra = BuiltinMetrics.get_configs(
            frozenset({"database"}), {}
        )
        assert (
            configs_no_extra["query_interval"].labels
            == configs_with_extra["query_interval"].labels
        )

    def test_get_configs_builtin_metrics_config_overrides_applied(
        self,
    ) -> None:
        configs = BuiltinMetrics.get_configs(
            frozenset(), {"queries": {"increment": False}}
        )
        assert configs["queries"].config["increment"] is False


class TestTimestampedMetric:
    def test_timestamped_metric(self) -> None:
        cls = timestamped_metric(Gauge)
        assert cls.__name__ == "TimestampedGauge"
        assert issubclass(cls, TimestampedMixin)
        assert issubclass(cls, Gauge)

    def test_timestamped_metric_types(self) -> None:
        metric_types = timestamped_metric_types()
        assert [mt.name for mt in metric_types] == [
            mt.name for mt in DEFAULT_METRIC_TYPES
        ]
        for metric_type, default_type in zip(
            metric_types, DEFAULT_METRIC_TYPES, strict=True
        ):
            assert issubclass(metric_type.cls, TimestampedMixin)
            assert issubclass(metric_type.cls, default_type.cls)
            assert metric_type.options == default_type.options

    @pytest.mark.parametrize("cls,options,method,value", METRIC_UPDATES)
    def test_samples_with_timestamp(
        self,
        cls: type[MetricWrapperBase],
        options: dict[str, Any],
        method: str,
        value: object,
    ) -> None:
        metric = make_metric(cls, options)
        child = metric.labels(l="foo")
        child.set_timestamp(1234.5)
        getattr(child, method)(value)
        [family] = metric.collect()
        assert family.samples
        assert all(sample.timestamp == 1234.5 for sample in family.samples)
        assert all(sample.labels["l"] == "foo" for sample in family.samples)

    @pytest.mark.parametrize("cls,options,method,value", METRIC_UPDATES)
    def test_samples_without_timestamp(
        self,
        cls: type[MetricWrapperBase],
        options: dict[str, Any],
        method: str,
        value: object,
    ) -> None:
        metric = make_metric(cls, options)
        getattr(metric.labels(l="foo"), method)(value)
        [family] = metric.collect()
        assert family.samples
        assert all(sample.timestamp is None for sample in family.samples)

    def test_timestamp_per_label_set(self) -> None:
        metric = make_gauge()
        foo = metric.labels(l="foo")
        foo.set_timestamp(10.0)
        foo.set(1)
        metric.labels(l="bar").set(2)
        [family] = metric.collect()
        assert {
            sample.labels["l"]: sample.timestamp for sample in family.samples
        } == {"foo": 10.0, "bar": None}

    def test_timestamp_reset(self) -> None:
        metric = make_gauge()
        child = metric.labels(l="foo")
        child.set_timestamp(10.0)
        child.set(1)
        child.set_timestamp(None)
        child.set(2)
        [family] = metric.collect()
        [sample] = family.samples
        assert sample.timestamp is None

    def test_exposition_milliseconds(self) -> None:
        metric = make_gauge()
        child = metric.labels(l="foo")
        child.set_timestamp(10.123456789)
        child.set(1)
        registry = CollectorRegistry()
        registry.register(metric)
        assert b'test{l="foo"} 1.0 10123\n' in generate_latest(registry)
