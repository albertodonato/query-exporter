query-exporter - Export Prometheus metrics from SQL queries
===========================================================

|Latest Version| |Build Status| |Coverage Status| |Snap Status|

``query-exporter`` is a Prometheus_ exporter which allows collecting metrics
from database queries, at specified time intervals.

It uses SQLAlchemy_ to connect to different database engines, including
PostgreSQL, MySQL, Oracle and Microsoft SQL Server.

Each query can be run on multiple databases, and update multiple metrics.

The application is called with a configuration file that looks like this:

.. code:: yaml

    databases:
      db1:
        dsn: sqlite://
      db2:
        dsn: sqlite://

    metrics:
      metric1:
        type: gauge
        description: A sample gauge
      metric2:
        type: summary
        description: A sample summary
      metric3:
        type: histogram
        description: A sample histogram
        buckets: [10, 20, 50, 100, 1000]
      metric4:
        type: enum
        description: A sample enum
        states: [foo, bar, baz]

    queries:
      query1:
        interval: 5
        databases: [db1]
        metrics: [metric1]
        sql: SELECT random() / 1000000000000000
      query2:
        interval: 20
        databases: [db1, db2]
        metrics: [metric2, metric3]
        sql: |
          SELECT abs(random() / 1000000000000000),
                 abs(random() / 10000000000000000)
      query3:
        interval: 10
        databases: [db2]
        metrics: [metric4]
        sql: |
          SELECT value FROM (
            SELECT 'foo' AS value UNION
            SELECT 'bar'
            UNION SELECT 'baz')
          ORDER BY random()
          LIMIT 1


The ``dsn`` connection string has the following format::

    dialect[+driver]://[username:password][@host:port]/database

(see `SQLAlchemy documentation`_ for details on the available options).

The ``metrics`` list in the query configuration must match values returned by
the query defined in ``sql``.

The ``interval`` value is interpreted as seconds if no suffix is specified;
valid suffix are ``s``, ``m``, ``h``, ``d``. Only integer values can be
specified. If no value is specified (or specified as ``null``), the query is
executed at every HTTP request.

Queries will usually return a single row, but multiple rows are supported, and
each row will cause an update of the related metrics.  This is relevant for any
kind of metric except gauges, which will be effectively updated to the value
from the last row.

For the configuration above, exported metrics look like this::

    # HELP metric1 A sample gauge
    # TYPE metric1 gauge
    metric1{database="db1"} 1549.0
    # HELP metric2 A sample summary
    # TYPE metric2 summary
    metric2_count{database="db2"} 1.0
    metric2_sum{database="db2"} 5229.0
    metric2_count{database="db1"} 1.0
    metric2_sum{database="db1"} 4513.0
    # TYPE metric2_created gauge
    metric2_created{database="db2"} 1.5456472955657206e+09
    metric2_created{database="db1"} 1.5456472955663064e+09
    # HELP metric3 A sample histogram
    # TYPE metric3 histogram
    metric3_bucket{database="db2",le="10.0"} 0.0
    metric3_bucket{database="db2",le="20.0"} 0.0
    metric3_bucket{database="db2",le="50.0"} 0.0
    metric3_bucket{database="db2",le="100.0"} 0.0
    metric3_bucket{database="db2",le="1000.0"} 1.0
    metric3_bucket{database="db2",le="+Inf"} 1.0
    metric3_count{database="db2"} 1.0
    metric3_sum{database="db2"} 714.0
    metric3_bucket{database="db1",le="10.0"} 0.0
    metric3_bucket{database="db1",le="20.0"} 0.0
    metric3_bucket{database="db1",le="50.0"} 0.0
    metric3_bucket{database="db1",le="100.0"} 0.0
    metric3_bucket{database="db1",le="1000.0"} 1.0
    metric3_bucket{database="db1",le="+Inf"} 1.0
    metric3_count{database="db1"} 1.0
    metric3_sum{database="db1"} 602.0
    # TYPE metric3_created gauge
    metric3_created{database="db2"} 1.545647295565831e+09
    metric3_created{database="db1"} 1.5456472955663848e+09
    # HELP metric4 A sample enum
    # TYPE metric4 gauge
    metric4{database="db2",metric4="foo"} 0.0
    metric4{database="db2",metric4="bar"} 1.0
    metric4{database="db2",metric4="baz"} 0.0

Metrics are automatically tagged with the ``database`` label so that
indipendent series are generated for each database.


Database engines
----------------

SQLAlchemy doesn't depend on specific Python database modules at
installation. This means additional modules might need to be installed for
engines in use, as follows::

    pip install SQLAlchemy[postgresql] SQLAlchemy[mysql] ...

based on which databased is in use.

See `supported databases`_ for details.


.. _Prometheus: https://prometheus.io/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _`SQLAlchemy documentation`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _`supported databases`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#supported-databases

.. |Latest Version| image:: https://img.shields.io/pypi/v/query-exporter.svg
   :target: https://pypi.python.org/pypi/query-exporter
.. |Build Status| image:: https://img.shields.io/travis/albertodonato/query-exporter.svg
   :target: https://travis-ci.org/albertodonato/query-exporter
.. |Coverage Status| image:: https://img.shields.io/codecov/c/github/albertodonato/query-exporter/master.svg
   :target: https://codecov.io/gh/albertodonato/query-exporter
.. |Snap Status| image:: https://build.snapcraft.io/badge/albertodonato/query-exporter.svg
   :target: https://build.snapcraft.io/user/albertodonato/query-exporter
