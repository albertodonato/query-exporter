Configuration file format
=========================

Configuration is provided as a YAML file, composed by a few sections, as
described in the following sections.

The following tags are supported in the configuration file:

``!include <filename>``:
  include the content of another YAML file.  This allows modularizing
  configuration.

  If the specified path is not absolute, it's considered relative to the
  including file.

``!file <filename>``:
  include the text content of a file as a string.

  If the specified path is not absolute, it's considered relative to the
  including file.

``!env <variable>``:
  expand to the value of the specified environment variable.

  Note that the value of the variable is interpreted as YAML (and thus JSON),
  allowing for specifying values other than strings (e.g. integers/floats).

  The specified variable must be set.


``databases`` section
---------------------

This section contains definitions for databases to connect to. Key names are
arbitrary and only used to reference databases in the ``queries`` section.
  
Each database definitions can have the following keys:

``dsn``:
  database connection details.

  It can be provided as a string in the following format::

    dialect[+driver]://[username:password][@host:port]/database[?option=value&...]

  or as a map with the following keys:

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

  See `SQLAlchemy documentation`_ for details on available engines and options
  and the `database-specific options`_ page for some extra details on database
  configuration options.

  **Note**: in the string form, username, password and options need to be
  URL-encoded, whereas this is done automatically for the key/value form.

  **Note**: use of the ``env:`` and ``file:`` prefixes in the string form is
  deprecated, and will be dropped in the 4.0 release. Use ``!env`` and
  ``!file`` YAML tags instead.

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

  **Note**: this option is deprecated and be removed in the next major
   release. Explicit transactions are now always used to run each query.

``labels``:
  an optional mapping of label names and values to tag metrics collected from each database.
  When labels are used, all databases must define the same set of labels.


``metrics`` section
-------------------

This section contains Prometheus metrics definitions. Keys are used as metric
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

``increment``:
  for ``counter`` metrics, whether to increment the value by the query result,
  or set the value to it.

  By default, counters are set to the value returned by the query. If this is
  set to ``true``, instead, the metric value will be incremented by the result
  of the query.


``queries`` section
-------------------

This section contains definitions for queries to perform. Key names are
arbitrary and only used to identify queries in logs.

Each query definition can have the following keys:

``databases``:
  the list of databases to run the query on.

  Names must match those defined in the ``databases`` section.

  Metrics are automatically tagged with the ``database`` label so that
  independent series are generated for each database that a query is run on.

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
              - name: TypeScript

  This example will generate 9 queries with all permutations of ``os`` and
  ``lang`` parameters.

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


.. _`database-specific options`: databases.rst
.. _`SQLAlchemy documentation`:
   http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
