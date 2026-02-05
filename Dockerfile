FROM --platform=$BUILDPLATFORM python:3.14-slim AS base

RUN apt-get update && apt-get full-upgrade -y

COPY . /src
RUN python3 -m venv /virtualenv
RUN /virtualenv/bin/pip install \
    -r /src/requirements.txt \
    /src
RUN rm -rf /src

ENV PATH="/virtualenv/bin:$PATH"
ENV VIRTUAL_ENV="/virtualenv"

# IPv6 support is not enabled by default, only bind IPv4
ENV QE_HOST="0.0.0.0"

EXPOSE 9560/tcp
VOLUME /config
WORKDIR /config
ENTRYPOINT ["query-exporter"]


FROM --platform=$BUILDPLATFORM base AS full

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
