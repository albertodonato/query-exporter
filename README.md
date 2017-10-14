# query-exporter - Export Prometheus metrics from SQL queries

[![Latest Version](https://img.shields.io/pypi/v/query-exporter.svg)](https://pypi.python.org/pypi/query-exporter)
[![Build Status](https://travis-ci.org/albertodonato/query-exporter.svg?branch=master)](https://travis-ci.org/albertodonato/query-exporter)
[![Coverage Status](https://codecov.io/gh/albertodonato/query-exporter/branch/master/graph/badge.svg)](https://codecov.io/gh/albertodonato/query-exporter)

`query-exporter` is a [Prometheus](https://prometheus.io/) exporter which
allows collecting metrics from database queries, at specified time intervals.

It uses [SQLAlchemy](https://www.sqlalchemy.org/) to connect to different
database engines, including PostgreSQL, MySQL, Oracle and Microsoft SQL Server.

Each query can be run on multiple databases, and update multiple metrics.

The application is called with a configuration file that looks like this:

```yaml
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
```

The `dsn` connection string has the following format:

```
dialect[+driver]://[username:password][@host:port]/database
```

(see
[SQLAlchemy documentation](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls) for
details on the available options).

The `metrics` list in the query configuration must match values returned by the
query defined in `sql`.

The `interval` value is interpreted as seconds if no suffix is specified; valid
suffix are `s`, `m`, `h`, `d`. Only integer values can be specified.

Queries will usually return a single row, but multiple rows are supported, and
each row will cause an update of the related metrics.  This is relevant for any
kind of metric except gauges, which will be effectively updated to the value
from the last row.

For the configuration above, exported metrics look like this:

```
# HELP metric1 A sample gauge
# TYPE metric1 gauge
metric1{database="db1"} 1549.0
# HELP metric2 A sample summary
# TYPE metric2 summary
metric2_count{database="db2"} 6.0
metric2_sum{database="db2"} 25329.0
metric2_count{database="db1"} 6.0
metric2_sum{database="db1"} 30170.0
# HELP metric3 A sample histogram
# TYPE metric3 histogram
metric3_bucket{database="db2",le="10.0"} 0.0
metric3_bucket{database="db2",le="20.0"} 1.0
metric3_bucket{database="db2",le="50.0"} 2.0
metric3_bucket{database="db2",le="100.0"} 2.0
metric3_bucket{database="db2",le="1000.0"} 6.0
metric3_bucket{database="db2",le="+Inf"} 6.0
metric3_count{database="db2"} 6.0
metric3_sum{database="db2"} 2542.0
metric3_bucket{database="db1",le="10.0"} 1.0
metric3_bucket{database="db1",le="20.0"} 1.0
metric3_bucket{database="db1",le="50.0"} 1.0
metric3_bucket{database="db1",le="100.0"} 2.0
metric3_bucket{database="db1",le="1000.0"} 6.0
metric3_bucket{database="db1",le="+Inf"} 6.0
metric3_count{database="db1"} 6.0
metric3_sum{database="db1"} 2901.0
```

Metrics are automatically tagged with the `database` label so that indipendent
series are generated for each database.


## Database engines

SQLAlchemy doesn't depend on specific Python database modules at installation.
This means additional modules might need to be installed for engines in use
(e.g. `psycopg2` for PostgreSQL or `MySQL-python` for MySQL).

See
[supported databases](http://docs.sqlalchemy.org/en/latest/core/engines.html#supported-databases) for details.
