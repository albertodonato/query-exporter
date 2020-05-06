FROM python:3.8-slim AS build-image

RUN apt update
RUN apt install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    libxml2 \
    unixodbc-dev

ADD . /srcdir
RUN python3 -m venv /virtualenv
ENV PATH="/virtualenv/bin:$PATH"
RUN pip install \
    /srcdir \
    ibm-db-sa \
    mysqlclient \
    pyodbc \
    psycopg2


FROM python:3.8-slim

RUN apt update
RUN apt install -y --no-install-recommends \
    libmariadb-dev-compat \
    libodbc1 \
    libpq5
COPY --from=build-image /virtualenv /virtualenv
ENV PATH="/virtualenv/bin:$PATH"

EXPOSE 9560/tcp
# IPv6 support is not enabled by default, only bind IPv4
ENTRYPOINT ["query-exporter", "-H", "0.0.0.0"]
