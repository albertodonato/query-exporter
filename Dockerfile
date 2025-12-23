FROM --platform=$BUILDPLATFORM adonato/query-exporter:min-latest

ENV BUILD_DEPS=" \
    build-essential \
    pkg-config \
    default-libmysqlclient-dev \
    libpq-dev"
RUN apt-get update && apt-get full-upgrade -y
RUN apt-get install -y --no-install-recommends $BUILD_DEPS

RUN /virtualenv/bin/pip install \
    clickhouse-sqlalchemy \
    "ibm-db-sa; platform_machine == 'x86_64'" \
    mysqlclient \
    oracledb \
    psycopg2 \
    pymssql \
    teradatasqlalchemy

RUN apt-get install -y --no-install-recommends \
    libmariadb-dev-compat \
    libpq5 \
    libxml2
RUN apt-get purge -y $BUILD_DEPS && apt-get autoremove --purge -y
