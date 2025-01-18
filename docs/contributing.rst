Thanks for considering contributing to query-exporter!

Issues
======

When submitting an issue, please include a detailed description of the issue,
what is happening, and in which conditions.

If possible, attach a log of the exporter with debug enabled (``--log-level
debug``), as well as the (sanitized as needed) content of the configuration
file.

Always include the exporter version (``query-exporter --version``), as well as
the installation method.


Enhancements
============

When proposing enhancements, please describe in detail the use cases or
problems that the enhancement would solve.

If possible, include examples of the new behavior with the change.


Pull requests
=============

Creating pull requests is very easy, and requires just a minimal development
setup is requested to verify the changes.

When creating a pull request for a non-trivial bug or enhancement, please
consider creating an issue first, so that discussion can happen more easily,
and reference it in the pull request.

Prerequisites
-------------

The development environment requires having ``tox`` installed. Please refer to
to the `Tox wiki`_ for installation instructions.

Please make sure that you run the following steps on your changes before
creating the pull request.

Tests
-----

Changes must have full test coverage.  The full suite can be run via::

  tox run -e coverage

which will include the coverage report.

To just run the tests, possibly limiting to a subset of them, run::

  tox run -e py -- <pytest args>

Type checking
-------------

The project uses ``mypy`` for type checking. Please make sure types are added correctly to new/changed code.
To verify, run::

  tox run -e check

Linting and formatting
----------------------

Formatting can be applied automatically with::

  tox run -e format

Linting is checked on pull requests, and can be verified with::

  tox run -e lint


.. _`Tox wiki`: https://tox.wiki/en/latest/index.html
