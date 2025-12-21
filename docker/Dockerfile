FROM ubuntu:25.10

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    pkg-config \
    libboost-all-dev \
    libhiredis-dev \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
CMD ["/bin/bash"]

