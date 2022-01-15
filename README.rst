|query-exporter logo|

Export Prometheus metrics from SQL queries
==========================================

|Latest Version| |Build Status| |Coverage Status| |Snap Package| |Docker Pulls|

``query-exporter`` is a Prometheus_ exporter which allows collecting metrics
from database queries, at specified time intervals.

It uses SQLAlchemy_ to connect to different database engines, including
PostgreSQL, MySQL, Oracle and Microsoft SQL Server.

Each query can be run on multiple databases, and update multiple metrics.

The application is simply run as::

  query-exporter config.yaml

where the passed configuration file contains the definitions of the databases
to connect and queries to perform to update metrics.


Configuration file format
-------------------------

A sample configuration file for the application looks like this:

.. code:: yaml

    databases:
      db1:
        dsn: sqlite://
        connect-sql:
          - PRAGMA application_id = 123
          - PRAGMA auto_vacuum = 1
        labels:
          region: us1
          app: app1
      db2:
        dsn: sqlite://
        keep-connected: false
        labels:
          region: us2
          app: app1

    metrics:
      metric1:
        type: gauge
        description: A sample gauge
      metric2:
        type: summary
        description: A sample summary
        labels: [l1, l2]
        expiration: 24h
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
        sql: SELECT random() / 1000000000000000 AS metric1
      query2:
        interval: 20
        timeout: 0.5
        databases: [db1, db2]
        metrics: [metric2, metric3]
        sql: |
          SELECT abs(random() / 1000000000000000) AS metric2,
                 abs(random() / 10000000000000000) AS metric3,
                 "value1" AS l1,
                 "value2" AS l2
      query3:
        schedule: "*/5 * * * *"
        databases: [db2]
        metrics: [metric4]
        sql: |
          SELECT value FROM (
            SELECT "foo" AS metric4 UNION
            SELECT "bar" AS metric3 UNION
            SELECT "baz" AS metric4
          )
          ORDER BY random()
          LIMIT 1

``databases`` section
~~~~~~~~~~~~~~~~~~~~~

This section contains defintions for databases to connect to. Key names are
arbitrary and only used to reference databases in the ``queries`` section.

Each database defintions can have the following keys:

``dsn``:
  database connection details.

  It can be provided as a string in the following format::

    dialect[+driver]://[username:password][@host:port]/database[?option=value&...]

  (see `SQLAlchemy documentation`_ for details on available engines and
  options), or as key/value pairs:

  .. code:: yaml

      dialect: <dialect>[+driver]
      user: <username>
      password: <password>
      host: <host>
      port: <port>
      database: <database>
      options:
        <key1>: <value1>
        <key2>: <value2>

  All entries are optional, except ``dialect``.

  Note that in the string form, username, password and options need to be
  URL-encoded, whereas this is done automatically for the key/value form.

  See `database-specific options`_ page for some extra details on database
  configuration options.

  It's also possible to get the connection string indirectly from other sources:

  - from an environment variable (e.g. ``$CONNECTION_STRING``) by setting ``dsn`` to::

      env:CONNECTION_STRING

  - from a file, containing only the DSN value, by setting ``dsn`` to::

      file:/path/to/file

  These forms only support specifying the actual DNS in the string form.

``connect-sql``:
  An optional list of queries to run right after database connection. This can
  be used to set up connection-wise parameters and configurations.

``keep-connected``:
  whether to keep the connection open for the database between queries, or
  disconnect after each one. If not specified, defaults to ``true``.  Setting
  this option to ``false`` might be useful if queries on a database are run
  with very long interval, to avoid holding idle connections.

``autocommit``:
  whether to set autocommit for the database connection. If not specified,
  defaults to ``true``.  This should only be changed to ``false`` if specific
  queries require it.

``labels``:
  an optional mapping of label names and values to tag metrics collected from each database.
  When labels are used, all databases must define the same set of labels.

``metrics`` section
~~~~~~~~~~~~~~~~~~~

This section contains Prometheus_ metrics definitions. Keys are used as metric
names, and must therefore be valid metric identifiers.

Each metric definition can have the following keys:

``type``:
  the type of the metric, must be specified. The following metric types are
  supported:

  - ``counter``: value is incremented with each result from queries
  - ``enum``: value is set with each result from queries
  - ``gauge``: value is set with each result from queries
  - ``histogram``: each result from queries is added to observations
  - ``summary``: each result from queries is added to observations

``description``:
  an optional description of the metric.

``labels``:
  an optional list of label names to apply to the metric.

  If specified, queries updating the metric must return rows that include
  values for each label in addition to the metric value.  Column names must
  match metric and labels names.

``buckets``:
  for ``histogram`` metrics, a list of buckets for the metrics.

  If not specified, default buckets are applied.

``states``:
  for ``enum`` metrics, a list of string values for possible states.

  Queries for updating the enum must return valid states.

``expiration``:
  the amount of time after which a series for the metric is cleared if no new
  value is collected.

  Last report times are tracked independently for each set of label values for
  the metric.

  This can be useful for metric series that only last for a certain amount of
  time, to avoid an ever-increasing collection of series.

  The value is interpreted as seconds if no suffix is specified; valid suffixes
  are ``s``, ``m``, ``h``, ``d``. Only integer values are accepted.

``queries`` section
~~~~~~~~~~~~~~~~~~~

This section contains definitions for queries to perform. Key names are
arbitrary and only used to identify queries in logs.

Each query definition can have the following keys:

``databases``:
  the list of databases to run the query on.

  Names must match those defined in the ``databases`` section.

  Metrics are automatically tagged with the ``database`` label so that
  indipendent series are generated for each database that a query is run on.

``interval``:
  the time interval at which the query is run.

  The value is interpreted as seconds if no suffix is specified; valid suffixes
  are ``s``, ``m``, ``h``, ``d``. Only integer values are accepted.

  If a value is specified for ``interval``, a ``schedule`` can't be specified.

  If no value is specified (or specified as ``null``), the query is only
  executed upon HTTP requests.

``metrics``:
  the list of metrics that the query updates.

  Names must match those defined in the ``metrics`` section.

``parameters``:
  an optional list or dictionary of parameters sets to run the query with.

  If specified as a list, the query will be run once for every set of
  parameters specified in this list, for every interval.

  Each parameter set must be a dictionary where keys must match parameters
  names from the query SQL (e.g. ``:param``).

  As an example:

  .. code:: yaml

      query:
        databases: [db]
        metrics: [metric]
        sql: |
          SELECT COUNT(*) AS metric FROM table
          WHERE id > :param1 AND id < :param2
        parameters:
          - param1: 10
            param2: 20
          - param1: 30
            param2: 40

  If specified as a dictionary, it's used as a multidimensional matrix of
  parameters lists to run the query with.
  The query will be run once for each permutation of parameters.

  If a query is specified with parameters as matrix in its ``sql``, it will be run once
  for every permutation in matrix of parameters, for every interval.

  Variable format in sql query: ``:{top_level_key}__{inner_key}``

  .. code:: yaml

      query:
        databases: [db]
        metrics: [apps_count]
        sql: |
          SELECT COUNT(1) AS apps_count FROM apps_list
          WHERE os = :os__name AND arch = :os__arch AND lang = :lang__name
        parameters:
            os:
              - name: MacOS
                arch: arm64
              - name: Linux
                arch: amd64
              - name: Windows
                arch: amd64
            lang:
              - name: Python3
              - name: Java
              - name: Typescript

  This example will generate 9 queries with all permutations of ``os`` and
  ``lang`` paramters.

``schedule``:
  a schedule for executing queries at specific times.

  This is expressed as a Cron-like format string (e.g. ``*/5 * * * *`` to run
  every five minutes).

  If a value is specified for ``schedule``, an ``interval`` can't be specified.

  If no value is specified (or specified as ``null``), the query is only
  executed upon HTTP requests.

``sql``:
  the SQL text of the query.

  The query must return columns with names that match those of the metrics
  defined in ``metrics``, plus those of labels (if any) for all these metrics.

  .. code:: yaml

      query:
        databases: [db]
        metrics: [metric1, metric2]
        sql: SELECT 10.0 AS metric1, 20.0 AS metric2

  will update ``metric1`` to ``10.0`` and ``metric2`` to ``20.0``.

  **Note**:
   since ``:`` is used for parameter markers (see ``parameters`` above),
   literal single ``:`` at the beginning of a word must be escaped with
   backslash (e.g. ``SELECT '\:bar' FROM table``).  There's no need to escape
   when the colon occurs inside a word (e.g. ``SELECT 'foo:bar' FROM table``).

``timeout``:
  a value in seconds after which the query is timed out.

  If specified, it must be a multiple of 0.1.


Metrics endpoint
----------------

The exporter listens on port ``9560`` providing the standard ``/metrics``
endpoint.

By default, the port is bound on ``localhost``. Note that if the name resolves
both IPv4 and IPv6 addressses, the exporter will bind on both.

For the configuration above, the endpoint would return something like this::

  # HELP database_errors_total Number of database errors
  # TYPE database_errors_total counter
  # HELP queries_total Number of database queries
  # TYPE queries_total counter
  queries_total{app="app1",database="db1",query="query1",region="us1",status="success"} 50.0
  queries_total{app="app1",database="db2",query="query2",region="us2",status="success"} 13.0
  queries_total{app="app1",database="db1",query="query2",region="us1",status="success"} 13.0
  queries_total{app="app1",database="db2",query="query3",region="us2",status="error"} 1.0
  # HELP queries_created Number of database queries
  # TYPE queries_created gauge
  queries_created{app="app1",database="db1",query="query1",region="us1",status="success"} 1.5945442444463024e+09
  queries_created{app="app1",database="db2",query="query2",region="us2",status="success"} 1.5945442444471517e+09
  queries_created{app="app1",database="db1",query="query2",region="us1",status="success"} 1.5945442444477117e+09
  queries_created{app="app1",database="db2",query="query3",region="us2",status="error"} 1.5945444000140696e+09
  # HELP query_latency Query execution latency
  # TYPE query_latency histogram
  query_latency_bucket{app="app1",database="db1",le="0.005",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.01",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.025",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.05",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.075",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.1",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.25",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.5",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="0.75",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="1.0",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="2.5",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="5.0",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="7.5",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="10.0",query="query1",region="us1"} 50.0
  query_latency_bucket{app="app1",database="db1",le="+Inf",query="query1",region="us1"} 50.0
  query_latency_count{app="app1",database="db1",query="query1",region="us1"} 50.0
  query_latency_sum{app="app1",database="db1",query="query1",region="us1"} 0.004666365042794496
  query_latency_bucket{app="app1",database="db2",le="0.005",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.01",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.025",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.05",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.075",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.1",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.25",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.5",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="0.75",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="1.0",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="2.5",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="5.0",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="7.5",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="10.0",query="query2",region="us2"} 13.0
  query_latency_bucket{app="app1",database="db2",le="+Inf",query="query2",region="us2"} 13.0
  query_latency_count{app="app1",database="db2",query="query2",region="us2"} 13.0
  query_latency_sum{app="app1",database="db2",query="query2",region="us2"} 0.012369773990940303
  query_latency_bucket{app="app1",database="db1",le="0.005",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.01",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.025",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.05",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.075",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.1",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.25",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.5",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="0.75",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="1.0",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="2.5",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="5.0",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="7.5",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="10.0",query="query2",region="us1"} 13.0
  query_latency_bucket{app="app1",database="db1",le="+Inf",query="query2",region="us1"} 13.0
  query_latency_count{app="app1",database="db1",query="query2",region="us1"} 13.0
  query_latency_sum{app="app1",database="db1",query="query2",region="us1"} 0.004745393933262676
  # HELP query_latency_created Query execution latency
  # TYPE query_latency_created gauge
  query_latency_created{app="app1",database="db1",query="query1",region="us1"} 1.594544244446163e+09
  query_latency_created{app="app1",database="db2",query="query2",region="us2"} 1.5945442444470239e+09
  query_latency_created{app="app1",database="db1",query="query2",region="us1"} 1.594544244447551e+09
  # HELP metric1 A sample gauge
  # TYPE metric1 gauge
  metric1{app="app1",database="db1",region="us1"} -3561.0
  # HELP metric2 A sample summary
  # TYPE metric2 summary
  metric2_count{app="app1",database="db2",l1="value1",l2="value2",region="us2"} 13.0
  metric2_sum{app="app1",database="db2",l1="value1",l2="value2",region="us2"} 58504.0
  metric2_count{app="app1",database="db1",l1="value1",l2="value2",region="us1"} 13.0
  metric2_sum{app="app1",database="db1",l1="value1",l2="value2",region="us1"} 75262.0
  # HELP metric2_created A sample summary
  # TYPE metric2_created gauge
  metric2_created{app="app1",database="db2",l1="value1",l2="value2",region="us2"} 1.594544244446819e+09
  metric2_created{app="app1",database="db1",l1="value1",l2="value2",region="us1"} 1.594544244447339e+09
  # HELP metric3 A sample histogram
  # TYPE metric3 histogram
  metric3_bucket{app="app1",database="db2",le="10.0",region="us2"} 1.0
  metric3_bucket{app="app1",database="db2",le="20.0",region="us2"} 1.0
  metric3_bucket{app="app1",database="db2",le="50.0",region="us2"} 2.0
  metric3_bucket{app="app1",database="db2",le="100.0",region="us2"} 3.0
  metric3_bucket{app="app1",database="db2",le="1000.0",region="us2"} 13.0
  metric3_bucket{app="app1",database="db2",le="+Inf",region="us2"} 13.0
  metric3_count{app="app1",database="db2",region="us2"} 13.0
  metric3_sum{app="app1",database="db2",region="us2"} 5016.0
  metric3_bucket{app="app1",database="db1",le="10.0",region="us1"} 0.0
  metric3_bucket{app="app1",database="db1",le="20.0",region="us1"} 0.0
  metric3_bucket{app="app1",database="db1",le="50.0",region="us1"} 0.0
  metric3_bucket{app="app1",database="db1",le="100.0",region="us1"} 0.0
  metric3_bucket{app="app1",database="db1",le="1000.0",region="us1"} 13.0
  metric3_bucket{app="app1",database="db1",le="+Inf",region="us1"} 13.0
  metric3_count{app="app1",database="db1",region="us1"} 13.0
  metric3_sum{app="app1",database="db1",region="us1"} 5358.0
  # HELP metric3_created A sample histogram
  # TYPE metric3_created gauge
  metric3_created{app="app1",database="db2",region="us2"} 1.5945442444469101e+09
  metric3_created{app="app1",database="db1",region="us1"} 1.5945442444474254e+09
  # HELP metric4 A sample enum
  # TYPE metric4 gauge
  metric4{app="app1",database="db2",metric4="foo",region="us2"} 0.0
  metric4{app="app1",database="db2",metric4="bar",region="us2"} 0.0
  metric4{app="app1",database="db2",metric4="baz",region="us2"} 1.0


Builtin metrics
---------------

The exporter provides a few builtin metrics which can be useful to track query execution:

``database_errors{database="db"}``:
  a counter used to report number of errors, per database.

``queries{database="db",query="q",status="[success|error|timeout]"}``:
  a counter with number of executed queries, per database, query and status.

``query_latency{database="db",query="q"}``:
  a histogram with query latencies, per database and query.


In addition, metrics for resources usage for the exporter procecss can be
included by passing ``--process-stats`` in the command line.


Debugging / Logs
----------------

You can enable extended logging using the ``-L`` commandline switch. Possible
log levels are ``CRITICAL``, ``ERROR``, ``WARNING``, ``INFO``, ``DEBUG``.


Database engines
----------------

SQLAlchemy_ doesn't depend on specific Python database modules at
installation. This means additional modules might need to be installed for
engines in use. These can be installed as follows::

  pip install SQLAlchemy[postgresql] SQLAlchemy[mysql] ...

based on which database engines are needed.

See `supported databases`_ for details.


Install from Snap
-----------------

|Get it from the Snap Store|

``query-exporter`` can be installed from `Snap Store`_ on systems where Snaps
are supported, via::

  sudo snap install query-exporter

The snap provides both the ``query-exporter`` command and a deamon instance of
the command, managed via a Systemd service.

To configure the daemon:

- create or edit ``/var/snap/query-exporter/current/config.yaml`` with the
  configuration
- run ``sudo snap restart query-exporter``

The snap has support for connecting the following databases:

- PostgreSQL (``postgresql://``)
- MySQL (``mysql://``)
- SQLite (``sqlite://``)
- Microsoft SQL Server (``mssql://``)
- IBM DB2 (``db2://``) on supported architectures (x86_64, ppc64le and
  s390x)


Run in Docker
-------------

``query-exporter`` can be run inside Docker_ containers, and is availble from
the `Docker Hub`_::

  docker run -p 9560:9560/tcp -v "$CONFIG_FILE:/config.yaml" --rm -it adonato/query-exporter:latest

where ``$CONFIG_FILE`` is the absolute path of the configuration file to
use. Note that the image expects the file to be available as ``/config.yaml``
in the container.

The image has support for connecting the following databases:

- PostgreSQL (``postgresql://``)
- MySQL (``mysql://``)
- SQLite (``sqlite://``)
- Microsoft SQL Server (``mssql://``)
- IBM DB2 (``db2://``)
- Oracle (``oracle://``)


.. _Prometheus: https://prometheus.io/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _`SQLAlchemy documentation`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _`supported databases`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#supported-databases
.. _`Snap Store`: https://snapcraft.io
.. _Docker: http://docker.com/
.. _`Docker Hub`: https://hub.docker.com/r/adonato/query-exporter
.. _`database-specific options`: databases.rst

.. |query-exporter logo| image:: https://raw.githubusercontent.com/albertodonato/query-exporter/main/logo.svg
   :alt: query-exporter logo
.. |Latest Version| image:: https://img.shields.io/pypi/v/query-exporter.svg
   :alt: Latest Version
   :target: https://pypi.python.org/pypi/query-exporter
.. |Build Status| image:: https://github.com/albertodonato/query-exporter/workflows/CI/badge.svg
   :alt: Build Status
   :target: https://github.com/albertodonato/query-exporter/actions?query=workflow%3ACI
.. |Coverage Status| image:: https://img.shields.io/codecov/c/github/albertodonato/query-exporter/main.svg
   :alt: Coverage Status
   :target: https://codecov.io/gh/albertodonato/query-exporter
.. |Snap Package| image:: https://snapcraft.io/query-exporter/badge.svg
   :alt: Snap Package
   :target: https://snapcraft.io/query-exporter
.. |Get it from the Snap Store| image:: https://snapcraft.io/static/images/badges/en/snap-store-black.svg
   :alt: Get it from the Snap Store
   :target: https://snapcraft.io/query-exporter
.. |Docker Pulls| image:: https://img.shields.io/docker/pulls/adonato/query-exporter
   :alt: Docker Pulls
   :target: https://hub.docker.com/r/adonato/query-exporter
