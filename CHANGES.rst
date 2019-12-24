v1.9.2 - 2019-12-24
===================

- Fix failure when multiple query columns have the same name (#18)
- Add Dockerfile (#17)


v1.9.1 - 2019-11-26
===================

- Track doomed queries on a per-database basis (#16)
- Add --version option


v1.9.0 - 2019-11-03
===================

- Support passing sets of parameters for queries.


v1.8.1 - 2019-07-14
===================

- Enable autocommit on connection (#10)


v1.8.0 - 2019-05-25
===================

- Support custom labels in metrics, setting values from queries result (#7)
- Suport matching metrics by query result column name instead of order
- Disable queries that will certainly always fail (e.g. because of invalid
  returned column names/number) (#6)
- Support disconnecting from after each query (#8)
- Rework tests to use actualy SQLite in-memory databases instead of fakes


v1.7.0 - 2019-04-07
===================

- Add a ``queries`` and ``database_errors`` metrics lebeled by database (Fixes #1)
- Support database DSNs defined as ``env:<VARNAME>`` to supply the dns from the
  environment (Fixes: #5)


v1.6.0 - 2019-03-26
===================

- Change default port to 9560 (to make it unique)


v1.5.0 - 2018-12-28
===================

- Drop support for Python 3.5.
- Add support for ``enum`` metrics.
- Add initial snap support.
- Rework project setup and use pytest.


v1.4.0 - 2018-06-08
===================

- Support for python3.7.
- Use asynctest for asyncronous tests.
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
