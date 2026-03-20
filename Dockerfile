FROM python:3.14-slim AS base

RUN apt-get update && apt-get full-upgrade -y

COPY --from=docker.io/astral/uv:latest /uv /bin/

COPY . /src

ENV PATH="/virtualenv/bin:$PATH" \
    VIRTUAL_ENV="/virtualenv" \
    UV_PROJECT_ENVIRONMENT="/virtualenv" \
    UV_WORKING_DIR="/src" \
    UV_COMPILE_BYTECODE="1" \
    UV_PYTHON_DOWNLOADS="never"

RUN uv sync --locked --no-default-groups

# IPv6 support is not enabled by default, only bind IPv4
ENV QE_HOST="0.0.0.0"

EXPOSE 9560/tcp
VOLUME /config
WORKDIR /config
ENTRYPOINT ["query-exporter"]


FROM base AS full

ENV BUILD_DEPS=" \
    build-essential \
    pkg-config \
    default-libmysqlclient-dev \
    libpq-dev"
RUN apt-get update && apt-get full-upgrade -y
RUN apt-get install -y --no-install-recommends $BUILD_DEPS

RUN uv sync --locked --group=docker-dbs

RUN apt-get install -y --no-install-recommends \
    libmariadb-dev-compat \
    libpq5 \
    libxml2
RUN apt-get purge -y $BUILD_DEPS && apt-get autoremove --purge -y
