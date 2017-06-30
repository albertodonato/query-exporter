# v1.2.0 - 2017-06-30

- Switch to SQLAlchemy. Multiple database engines are now supported.
- Needed database libraries must now be installed separately, as there is no
  explicit dependency in SQLAlchemy.


# v1.1.0 - 2017-05-21

- Use connection pools for queries.


# v1.0.0 - 2017-05-13

- Replace aiopg with asyncpg. The database dsn string is now specified as a
  postgres:// URI.


# v0.1.2 - 2017-05-07

- Replace Makefile with tox.


# v0.1.1 - 2017-03-07

- Fix setup.py issues.


# v0.1.0 - 2017-03-07

- First release.
