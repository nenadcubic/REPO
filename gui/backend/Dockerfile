FROM python:3.12-slim AS er_builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    pkg-config \
    libboost-all-dev \
    libhiredis-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY include /src/include
COPY src /src/src
COPY cli /src/cli

RUN cmake -S /src -B /build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /build -j \
    && mkdir -p /out \
    && install -m 0755 /build/cli/er_cli /out/er_cli

FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libboost-all-dev \
    libhiredis1.1.0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY gui/backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY gui/backend/app /app/app
COPY gui/backend/openapi.yaml /app/openapi.yaml
COPY gui/backend/scripts /app/scripts
COPY gui/backend/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh /app/scripts/selftest.sh \
    && mkdir -p /app/presets /app/logs

COPY --from=er_builder /out/er_cli /usr/local/bin/er_cli

EXPOSE 8000
CMD ["/app/entrypoint.sh"]
