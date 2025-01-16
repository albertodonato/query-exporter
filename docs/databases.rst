Database-specific options
=========================

This section collects a few details and gotchas related to specific database
engines.


Microsoft SQL Server [``mssql://``]
===================================

Sample DSN::

  mssql://<username>:<password>@<host>:<port>/<db>?driver=ODBC+Driver+17+for+SQL+Server


Multiple queries on a single connection
---------------------------------------

When defining multiple queries on the same connection with MSSQL, the Multiple
Active Result Sets (MARS_) feature should be enabled for query results to be
reported correctly.

This can be done via the ``MARS_Connection`` parameter on the database DSN::

  mssql://<username>:<password>@<host>:<port>/<db>?MARS_Connection=yes


Oracle [``oracle://``]
======================

Sample DSN::

  oracle://<username>:<password>@(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = <hostname>)(PORT = <port>))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = <service>)))



.. _MARS: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/enabling-multiple-active-result-sets
