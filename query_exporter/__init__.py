"""Export Prometheus metrics generated from SQL queries."""

from distutils.version import LooseVersion

import pkg_resources

__all__ = ["__version__"]

__version__ = LooseVersion(pkg_resources.require("query_exporter")[0].version)
