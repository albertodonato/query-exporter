from collections.abc import Iterable

from prometheus_aioexporter import DEFAULT_METRIC_TYPES, MetricType
from prometheus_client.metrics import MetricWrapperBase
from prometheus_client.samples import Sample


class TimestampedMixin(MetricWrapperBase):
    _timestamp: float | None = None

    def set_timestamp(self, timestamp: float | None) -> None:
        self._timestamp = timestamp

    def _child_samples(self) -> Iterable[Sample]:
        samples = super()._child_samples()
        if self._timestamp is None:
            return samples
        return [
            sample._replace(timestamp=self._timestamp) for sample in samples
        ]


def timestamped_metric(
    cls: type[MetricWrapperBase],
) -> type[MetricWrapperBase]:
    return type(f"Timestamped{cls.__name__}", (TimestampedMixin, cls), {})


def timestamped_metric_types(
    metric_types: Iterable[MetricType] = DEFAULT_METRIC_TYPES,
) -> list[MetricType]:
    return [
        MetricType(mt.name, timestamped_metric(mt.cls), mt.options)
        for mt in metric_types
    ]
