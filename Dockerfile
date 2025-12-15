FROM --platform=$BUILDPLATFORM python:3.13-slim-bookworm AS build-image

RUN apt-get update
RUN apt-get full-upgrade -y
RUN apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    default-libmysqlclient-dev \
    libpq-dev

COPY . /srcdir
RUN python3 -m venv /virtualenv
ENV PATH="/virtualenv/bin:$PATH"
RUN pip install \
    -r /srcdir/requirements.txt \
    /srcdir \
    clickhouse-sqlalchemy \
    "ibm-db-sa; platform_machine == 'x86_64'" \
    mysqlclient \
    oracledb \
    psycopg2 \
    pymssql \
    teradatasqlalchemy

FROM --platform=$BUILDPLATFORM python:3.13-slim-bookworm

RUN apt-get update
RUN apt-get full-upgrade -y
RUN apt-get install -y --no-install-recommends \
    libmariadb-dev-compat \
    libpq5 \
    libxml2
RUN rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man && apt-get clean

COPY --from=build-image /virtualenv /virtualenv
COPY --from=build-image /opt /opt

ENV PATH="/virtualenv/bin:$PATH"
ENV VIRTUAL_ENV="/virtualenv"

# IPv6 support is not enabled by default, only bind IPv4
ENV QE_HOST="0.0.0.0"

EXPOSE 9560/tcp
VOLUME /config
WORKDIR /config
ENTRYPOINT ["query-exporter"]
