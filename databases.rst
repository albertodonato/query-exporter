Database-specific options
=========================

This section collects a few details and gotchas related to specific database
engines.


Microsoft SQL Server [``mssql://``]
===================================


Multiple queries on a single connection
---------------------------------------

When defining multiple queries on the same connection with MSSQL, the Multiple
Active Result Sets (MARS_) feature should be enabled for query results to be
reported correctly.

This can be done via the ``MARS_Connection`` parameter on the database DSN::

  mssql://username:password@host:port/db?MARS_Connection=yes


Connection closed error during query execution
----------------------------------------------

In some cases an error about ``"The cursor's connection has been closed"`` is
returned when fetching query results.

If this happens, setting ``autocommit: false`` in database configuration should
solve it.


.. _MARS: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/enabling-multiple-active-result-sets
