"""Export Prometheus metrics generated from SQL queries."""

from distutils.version import LooseVersion

from pkg_resources import get_distribution

__all__ = ["PACKAGE", "__version__"]


PACKAGE = get_distribution("query_exporter")

__version__ = LooseVersion(PACKAGE.version)
