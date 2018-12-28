v1.5.0 - 2018-12-28
===================

- Drop support for Python 3.5.
- Add support for "enum" metrics.
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
   postgres:// URI.


v0.1.2 - 2017-05-07
===================

- Replace Makefile with tox.


v0.1.1 - 2017-03-07
===================

- Fix setup.py issues.


v0.1.0 - 2017-03-07
===================

- First release.
