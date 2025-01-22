|query-exporter logo|

Export Prometheus metrics from SQL queries
==========================================

|Latest Version| |Build Status| |PyPI Downloads| |Docker Pulls| |Snap Package|

``query-exporter`` is a Prometheus_ exporter which allows collecting metrics
from database queries, at specified time intervals.

It uses SQLAlchemy_ to connect to different database engines, including
PostgreSQL, MySQL, Oracle and Microsoft SQL Server.

Each query can be run on multiple databases, and update multiple metrics.

The application is simply run as::

  query-exporter

which will look for a ``config.yaml`` configuration file in the current
directory, containing the definitions of the databases to connect and queries
to perform to update metrics.  The configuration file can be overridden by
passing the ``--config`` option (or setting the ``QE_CONFIG`` environment
variable).  The option can be provided multiple times to pass partial
configuration files, the resulting configuration will be the merge of the
content of each top-level section (``databases``, ``metrics``, ``queries``).

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


See the `configuration file format`_ documentation for complete details on
availble configuration options.


Exporter options
----------------

The exporter provides the following options, that can be set via command-line
switches, environment variables or through the ``.env`` file:

.. table::
   :widths: auto

  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | Command-line option     | Environment variable   | Default         | Description                                                       |
  +=========================+========================+=================+===================================================================+
  | ``-H``, ``--host``      | ``QE_HOST``            | ``localhost``   | Host addresses to bind. Multiple values can be provided.          |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  |  ``-p``, ``--port``     | ``QE_PORT``            | ``9560``        | Port to run the webserver on.                                     |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--metrics-path``      | ``QE_METRICS_PATH``    | ``/metrics``    | Path under which metrics are exposed.                             |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``-L``, ``--log-level`` | ``QE_LOG_LEVEL``       | ``info``        | Minimum level for log messages level.                             |
  |                         |                        |                 | One of ``critical``, ``error``, ``warning``, ``info``, ``debug``. |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--log-format``        | ``QE_LOG_FORMAT``      | ``plain``       | Log output format. One of ``plain``, ``json``.                    |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--process-stats``     | ``QE_PROCESS_STATS``   | ``false``       | Include process stats in metrics.                                 |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--ssl-private-key``   | ``QE_SSL_PRIVATE_KEY`` |                 | Full path to the SSL private key.                                 |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--ssl-public-key``    | ``QE_SSL_PUBLIC_KEY``  |                 | Full path to the SSL public key.                                  |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--ssl-ca``            | ``QE_SSL_CA``          |                 | Full path to the SSL certificate authority (CA).                  |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--check-only``        | ``QE_CHECK_ONLY``      | ``false``       | Only check configuration, don't run the exporter.                 |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  | ``--config``            | ``QE_CONFIG``          | ``config.yaml`` | Configuration files. Multiple values can be provided.             |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+
  |                         | ``QE_DOTENV``          | ``$PWD/.env``   | Path for the dotenv file where environment variables can be       |
  |                         |                        |                 | provided.                                                         |
  +-------------------------+------------------------+-----------------+-------------------------------------------------------------------+


Metrics endpoint
----------------

The exporter listens on port ``9560`` providing the standard ``/metrics``
endpoint.

By default, the port is bound on ``localhost``. Note that if the name resolves
both IPv4 and IPv6 addressses, the exporter will bind on both.


Builtin metrics
---------------

The exporter provides a few builtin metrics which can be useful to track query execution:

``database_errors{database="db"}``:
  a counter used to report number of errors, per database.

``queries{database="db",query="q",status="[success|error|timeout]"}``:
  a counter with number of executed queries, per database, query and status.

``query_interval{query="q"}``:
  a gauge reporting the configured execution interval in seconds, if set, per query.

``query_latency{database="db",query="q"}``:
  a histogram with query latencies, per database and query.

``query_timestamp{database="db",query="q"}``:
  a gauge with query last execution timestamps, per database and query.

In addition, metrics for resources usage for the exporter process can be
included by passing ``--process-stats`` in the command line.


Database engines
----------------

SQLAlchemy_ doesn't depend on specific Python database modules at
installation. This means additional modules might need to be installed for
engines in use. These can be installed as follows::

  pip install SQLAlchemy[postgresql] SQLAlchemy[mysql] ...

based on which database engines are needed.

See `supported databases`_ for details.


Run in Docker
=============

``query-exporter`` can be run inside Docker_ containers, and is available from
the `Docker Hub`_::

  docker run --rm -it -p 9560:9560/tcp -v "$CONFIG_DIR:/config" adonato/query-exporter:latest

where ``$CONFIG_DIR`` is the absolute path of a directory containing a
``config.yaml`` file, the configuration file to use. Alternatively, a volume
name can be specified.

If a ``.env`` file is present in the specified volume for ``/config``, its
content is loaded and applied to the environment for the exporter. The location
of the dotenv file can be customized by setting the ``QE_DOTENV`` environment
variable.

The image has support for connecting the following databases:

- PostgreSQL (``postgresql://``)
- MySQL (``mysql://``)
- SQLite (``sqlite://``)
- Microsoft SQL Server (``mssql://``)
- IBM DB2 (``db2://``)
- Oracle (``oracle://``)
- ClickHouse (``clickhouse+native://``)

A `Helm chart`_ to run the container in Kubernetes is also available.

Automated builds from the ``main`` branch are available on the `GitHub container registry`_ via::

  docker pull ghcr.io/albertodonato/query-exporter:sha256-28058bd8c5acc97d57c1ad95f1a7395d9d43c30687459cd4adacc3e19d009996


ODBC driver version
-------------------

A different ODBC driver version to use can be specified during image building,
by passing ``--build-arg ODBC_bVERSION_NUMBER``, e.g.::

  docker build . --build-arg ODBC_DRIVER_VERSION=17


Install from Snap
=================

|Get it from the Snap Store|

``query-exporter`` can be installed from `Snap Store`_ on systems where Snaps
are supported, via::

  sudo snap install query-exporter

The snap provides both the ``query-exporter`` command and a daemon instance of
the command, managed via a Systemd service.

To configure the daemon:

- create or edit ``/var/snap/query-exporter/current/config.yaml`` with the
  configuration
- optionally, create a ``/var/snap/query-exporter/current/.env`` file with
  environment variables definitions for additional config options
- run ``sudo snap restart query-exporter``

The snap has support for connecting the following databases:

- PostgreSQL (``postgresql://``)
- MySQL (``mysql://``)
- SQLite (``sqlite://``)
- Microsoft SQL Server (``mssql://``)
- IBM DB2 (``db2://``) on supported architectures (x86_64, ppc64le and
  s390x)


Contributing
============

The project welcomes contributions of any form. Please refer to the
`contribution guide`_ for details on how to contribute.

For general purpose questions, you can use `Discussions`_ on GitHub.


.. _Prometheus: https://prometheus.io/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _`supported databases`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#supported-databases
.. _`Snap Store`: https://snapcraft.io
.. _Docker: http://docker.com/
.. _`Docker Hub`: https://hub.docker.com/r/adonato/query-exporter
.. _`configuration file format`: docs/configuration.rst
.. _`contribution guide`: docs/contributing.rst
.. _`Helm chart`: https://github.com/makezbs/helm-charts/tree/main/charts/query-exporter
.. _`GitHub container registry`: https://github.com/albertodonato/query-exporter/pkgs/container/query-exporter
.. _`Discussions`: https://github.com/albertodonato/query-exporter/discussions

.. |query-exporter logo| image:: https://raw.githubusercontent.com/albertodonato/query-exporter/main/logo.svg
   :alt: query-exporter logo
.. |Latest Version| image:: https://img.shields.io/pypi/v/query-exporter.svg
   :alt: Latest Version
   :target: https://pypi.python.org/pypi/query-exporter
.. |Build Status| image:: https://github.com/albertodonato/query-exporter/workflows/CI/badge.svg
   :alt: Build Status
   :target: https://github.com/albertodonato/query-exporter/actions?query=workflow%3ACI
.. |Snap Package| image:: https://snapcraft.io/query-exporter/badge.svg
   :alt: Snap Package
   :target: https://snapcraft.io/query-exporter
.. |Get it from the Snap Store| image:: https://snapcraft.io/static/images/badges/en/snap-store-black.svg
   :alt: Get it from the Snap Store
   :target: https://snapcraft.io/query-exporter
.. |Docker Pulls| image:: https://img.shields.io/docker/pulls/adonato/query-exporter
   :alt: Docker Pulls
   :target: https://hub.docker.com/r/adonato/query-exporter
.. |PyPI Downloads| image:: https://static.pepy.tech/badge/query-exporter/month
   :alt: PyPI Downloads
   :target: https://pepy.tech/projects/query-exporter
