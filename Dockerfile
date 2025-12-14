FROM --platform=$BUILDPLATFORM python:3.13-alpine AS build-image

RUN apk add --no-cache --virtual .build-deps \
    build-base \
    pkgconfig \
    mariadb-dev \
    postgresql-dev \
    libxml2-dev

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

FROM --platform=$BUILDPLATFORM python:3.13-alpine

RUN apk add --no-cache \
    mariadb-connector-c \
    postgresql-libs \
    libxml2

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
