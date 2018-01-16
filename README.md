DB Statistics Collector
=======================

# Summary:

DB Statistics Collector (dbstat) is a PostgreSQL extension that collects table metrics such as exact row count, and the timing of insert/update/delete statements.

This is a learning exercise in the use of libpq and C-based extensions.

## Requirements:

* PostgreSQL 9.6+
* gcc
* development packages for postgres

## Installation

1. Checkout the dbstat source from github.
2. Ensure that pg_config is in the user's path
3. As root, run 'make install' with dbstat.control in your cwd.
4. Run 'make' to compile dbstat
5. Login to the target database server.
6. Run 'CREATE EXTENSION dbstat;'
7. Start dbstat with the connection parameters to the target DB server/instance.

Note: You may need to symlink the libpq-fe.h file to /usr/includes. This file is typically located in:
/usr/pgsql-version/includes/

## Changelog

### Version 0.2
* Multithreaded listeners
* SIGHUP handler reloads and reissues listeners
* Supports NoSQL / KV store backends for statistics

### Version 0.1
Initial version
