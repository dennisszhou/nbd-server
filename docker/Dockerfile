FROM rust:1-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        e2fsprogs \
        kmod \
        make \
        nbd-client \
        netcat-openbsd \
        nodejs \
        npm \
        procps \
        sqlite3 \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
