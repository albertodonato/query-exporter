v3.2.0 - 2025-01-25
===================

- Run queries in explicit transactions, deprecate ``autocommit`` flag (#232)
- Add builtin ``query_interval`` metric. (#225)

**NOTE**:
  The ``autocommit`` flag for ``database`` entries is now deprecated and not
  used anymore. Queries are always run in a separate transaction with explicit
  commit/rollback. Queries in the ``connect-sql`` config are all run in a
  single transaction.


v3.1.0 - 2025-01-08
===================

- Support passing multiple configuration files (#108).
- Support YAML tags for ``env``, ``file``, and ``include`` (#188).

**NOTE**:
  The ``env:`` and ``file:`` prefixes for DSNs string form is now deprecated in
  favor of the corresponding tags, and will be dropped in the next major release.


v3.0.0 - 2024-12-30
===================

- Convert all logging to structured (#199).
- Look for ``config.yaml`` configuration file by default, configuration file
  can be optionally specified with ``--config``.
- Support passing configuration options via environment variables.
- Support loading ``.env`` file with environment variables for configuration.
- [docker] Run exporter from the ``/config`` directory, supporting having
  ``.env`` file there.
- [snap] Run exporter from the ``$SNAP_DATA`` directory, supporting having
  ``.env`` file there.

**NOTE**:
  This release introduces a few breaking changes from the 2.x series,
  specificially:

  - The ``--log-level`` option now takes lowercase names for levels.
  - The configuration file is no longer a required option, since
    ``config.yaml`` in the current directory is looked up automatically. If a
    different file is specified, it should be done as an optional parameter
    with ``--config``.
  - Metrics of type ``counter`` are now set by default to values returned by
    queries. To preserve the old default behavior of incrementing it by the
    returned value, ``increment`` should be set to ``true`` in the metric
    configuration.


v2.11.1 - 2024-11-19
====================

- Update ``prometheus-aioexporter`` dependency range.


v2.11.0 - 2024-11-19
====================

- Switch to SQLAlchemy 2.
- Require Python 3.11.
- [docker] Use Python 3.13 on Debian Bookwork as base image.
- [snap] Rebase on core24.


v2.10.0 - 2024-01-28
====================

- Fix columns names in InvalidResultColumnNames being reported the wrong way
  round (#185).
- Add a metric to track query execution timestamp (#178).
- [docker] Define a volume containing the config file (#158).
- [docker] Add support for ODBC version 17, support alternative versions.
- Switch to ruff for formatting.


v2.9.2 - 2023-10-28
===================

- Fix main script (#171).
- Typos fixes in documentation.


v2.9.1 - 2023-10-28
===================

- Update dependency to ``prometheus-aioexporter`` 2.0.
- [snap] Add support for ClickHouse.
- [docker] Add support for ClickHouse.


v2.9.0 - 2023-08-18
===================

- Add ``increment`` flag for counter metrics (#124).
- Rework project setup.
- [docker] Add ``pymssql`` package (#133).
- [docker] Fix setup for Microsoft repository (#159).


v2.8.3 - 2022-07-16
===================

- Test with Python 3.10 in GitHub actions
- Fix tests


v2.8.2 - 2022-07-16
===================

- Require Python 3.10.
- [snap] Change base to core22.
- [docker] Use Python 3.10.
- [docker] Base on Debian 11.


v2.8.1 - 2022-02-18
===================

- Require ``sqlalchemy_aio`` 0.17.0, drop workaround for previous versions
  (#105).


v2.8.0 - 2022-01-18
===================

- Add support for parameters matrix in queries.
- Allow freetext name for databases in config (#99).


v2.7.1 - 2021-12-01
===================

- Require Python3.8
- Move creation of async locks and queues after loop setup (#90, #96).
- Update Python project config.
- Document action performed for each query type (#86).


v2.7.0 - 2021-03-28
===================

- Support optional ``expiration`` parameter for metrics to clear stale series
  (#26).
- Correctly close the database connection when ``keep-connected: false`` is
  used (#81).
- Don't connect to all databases at startup, only when the first query is run.


v2.6.2 - 2021-03-13
===================

- Workaround missing attributes from AsyncioEngine (#62 and #76).
- Log app version at startup.


v2.6.1 - 2020-12-20
===================

- Fix schedule times iterator for scheduled queries (#76).
- [docker] - Fix build.


v2.6.0 - 2020-12-06
===================

- Add support reading database DSN from file.
- Add support for specifying database connection details as separate elements.
- [docker] Fix MSSQL support in.


v2.5.1 - 2020-11-26
===================

- Add tracebacks for database errors in debug log.


v2.5.0 - 2020-11-19
===================

- Add ``query`` name label for builtin metrics.
- Add optional ``timeout`` option for queries.
- [snap] Add bash completion.
- [snap] Switch to core20 base.
- [docker] Include support for Oracle database.


v2.4.0 - 2020-06-20
===================

- Add a ``query_latency`` metric to track query execution times. This is
  labeled by database and query name (#46).


v2.3.0 - 2020-06-04
===================

- Add support for query schedule (#29).
- [docker] Pass the config file in the command line.


v2.2.1 - 2020-05-08
===================

- [snap] Enable IBM DB2 support only on supported architectures (x86_64,
  ppc64le and s390x).
- [docker] Fix python library paths.
- Add support for disabling query autocommit in database configuration.


v2.2.0 - 2020-03-16
===================

- Validate that metric names don't collide with builtin ones.
- Perform database connect/disconnect under lock (#28).
- Add support for queries to run at connection (#31).
- [snap,docker] Support IBM DB2 (#14).


v2.1.0 - 2020-02-29
===================

- When validating config, warn about database and metrics that are not used in
  any query.
- Support extra per-database labels for metrics. All databases must define the
  same set of labels (#27).


v2.0.2 - 2020-02-15
===================

- Don't disable queries failing because of ``OperationalError`` as it might not
  be a fatal error (#25).


v2.0.1 - 2020-02-07
===================

- Fix validation for entries in the ``queries`` section for config file.
- [snap,docker] Add MSSQL support.


v2.0.0 - 2020-02-02
===================

- Support only named parameters (e.g.: ``:param``) in queries (#21, #24).
- Add JSON-schema validation for config file (#23).
- Validate at startup if database engines from DSNs are supported and
  corresponding modules are available.
- Check that names for queries ``parameters`` match the ones in queries SQL.
- Add ``--check-only`` command line option to just validate configuration.
- Drop support for matching query columns positionally, only support name
  match. This is to avoid confusing behavior with positional match, and make
  queries more explicit.

**NOTE**:
 some of the changes above for query definitions are backwards incompatible,
 thus queries might need updating. Specifically:

 - Only named parameters with the ``:param`` style are now supported, queries
   using positional parameters or other styles of named parameters need to be
   updated.
 - Literal ``:`` at the beginning of a word need to be escaped (with backslash)
   to avoid confusion with parameter markers. Colons that appear inside words
   don't need to be escaped.
 - Column names for query results must now always match metric and label names
   involved in the query. Position-based match for queries without labels is no
   longer supported. Queries can be updated adding ``AS
   <metric_name|label_name>`` expressions.


v1.9.3 - 2019-12-29
===================

- Convert ``Decimal`` query results to float (#19).


v1.9.2 - 2019-12-24
===================

- Fix failure when multiple query columns have the same name (#18).
- [docker] Add Dockerfile (#17).


v1.9.1 - 2019-11-26
===================

- Track doomed queries on a per-database basis (#16).
- Add ``--version`` option.


v1.9.0 - 2019-11-03
===================

- Support passing sets of parameters for queries.


v1.8.1 - 2019-07-14
===================

- Enable autocommit on connection (#10).


v1.8.0 - 2019-05-25
===================

- Support custom labels in metrics, setting values from queries result (#7).
- Suport matching metrics by query result column name instead of order.
- Disable queries that will certainly always fail (e.g. because of invalid.
  returned column names/number) (#6).
- Support disconnecting from after each query (#8).
- Rework tests to use actually SQLite in-memory databases instead of fakes.


v1.7.0 - 2019-04-07
===================

- Add a ``queries`` and ``database_errors`` metrics labeled by database (#1).
- Support database DSNs defined as ``env:<VARNAME>`` to supply the dns from the
  environment (#5).


v1.6.0 - 2019-03-26
===================

- Change default port to 9560 (to make it unique).


v1.5.0 - 2018-12-28
===================

- Drop support for Python 3.5.
- Add support for ``enum`` metrics.
- [snap] Add initial snap support.
- Rework project setup and use pytest.


v1.4.0 - 2018-06-08
===================

- Support for python3.7.
- Use asynctest for asynchronous tests.
- Updated toolrack dependency.


v1.3.0 - 2018-02-20
===================

- Support aperiodic queries, which are run at every request for the metrics
  endpoint.


v1.2.2 - 2017-10-25
===================

- Fix tests for latest prometheus_aioexporter.


v1.2.1 - 2017-10-25
===================

- Documentation cleanups (and conversion to reST).


v1.2.0 - 2017-06-30
===================

- Switch to SQLAlchemy. Multiple database engines are now supported.
- Needed database libraries must now be installed separately, as there is no
  explicit dependency in SQLAlchemy.


v1.1.0 - 2017-05-21
===================

- Use connection pools for queries.


v1.0.0 - 2017-05-13
===================

- Replace aiopg with asyncpg. The database dsn string is now specified as a
  ``postgres://`` URI.


v0.1.2 - 2017-05-07
===================

- Replace Makefile with tox.


v0.1.1 - 2017-03-07
===================

- Fix setup.py issues.


v0.1.0 - 2017-03-07
===================

- First release.
