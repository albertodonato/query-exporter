FROM --platform=$BUILDPLATFORM python:3.13-slim-bookworm AS build-image

RUN apt-get update
RUN apt-get full-upgrade -y
RUN apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    default-libmysqlclient-dev \
    freetds-dev \
    libkrb5-dev \
    libpq-dev \
    pkg-config \
    unixodbc-dev \
    unzip

COPY . /srcdir
RUN python3 -m venv /virtualenv
ENV PATH="/virtualenv/bin:$PATH"
RUN pip install \
    -r /srcdir/requirements.txt \
    /srcdir \
    cx-Oracle \
    clickhouse-sqlalchemy \
    "ibm-db-sa; platform_machine == 'x86_64' or platform_machine == 'ppc64le'" \
    mysqlclient \
    psycopg2 \
    pymssql \
    pyodbc \
    teradatasqlalchemy

RUN curl \
    https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linux$(arch | sed -e 's/x86_64/x64/g; s/aarch64/-arm64/g').zip \
    -o instantclient.zip
RUN unzip instantclient.zip
RUN mkdir -p /opt/oracle/instantclient
RUN mv instantclient*/* /opt/oracle/instantclient


FROM --platform=$BUILDPLATFORM python:3.13-slim-bookworm
ARG ODBC_DRIVER_VERSION=18
ENV ODBC_DRIVER=msodbcsql${ODBC_DRIVER_VERSION}

RUN apt-get update && \
    apt-get full-upgrade -y && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    libaio1 \
    libmariadb-dev-compat \
    libodbc1 \
    libpq5 \
    libxml2 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg && \
    (. /etc/os-release; echo "deb https://packages.microsoft.com/debian/$VERSION_ID/prod $VERSION_CODENAME main") > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends $ODBC_DRIVER && \
    rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man && \
    apt-get clean

COPY --from=build-image /virtualenv /virtualenv
COPY --from=build-image /opt /opt

ENV PATH="/virtualenv/bin:$PATH"
ENV VIRTUAL_ENV="/virtualenv"
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient"
# IPv6 support is not enabled by default, only bind IPv4
ENV QE_HOST="0.0.0.0"

EXPOSE 9560/tcp
VOLUME /config
WORKDIR /config
ENTRYPOINT ["query-exporter"]
