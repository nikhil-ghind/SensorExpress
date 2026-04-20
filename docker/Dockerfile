FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# ── System dependencies ────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    libmosquitto-dev \
    mosquitto-clients \
    libyaml-cpp-dev \
    libspdlog-dev \
    libfmt-dev \
    libcurl4-openssl-dev \
    libgtest-dev \
    libnlohmann-json3-dev \
    python3 \
    python3-pip \
    ca-certificates \
  && pip3 install --no-cache-dir paho-mqtt \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# ── httplib single-header ─────────────────────────────────────────────────
RUN mkdir -p /usr/local/include && \
    git clone --depth 1 https://github.com/yhirose/cpp-httplib.git /tmp/httplib && \
    cp /tmp/httplib/httplib.h /usr/local/include/httplib.h && \
    rm -rf /tmp/httplib

# ── Build GTest (Ubuntu 22.04 ships sources, not compiled) ────────────────
RUN cmake -S /usr/src/gtest -B /tmp/gtest_build \
        -DCMAKE_BUILD_TYPE=Release && \
    cmake --build /tmp/gtest_build -j"$(nproc)" && \
    cp /tmp/gtest_build/lib/*.a /usr/local/lib/ && \
    rm -rf /tmp/gtest_build

# ── Copy source and build ─────────────────────────────────────────────────
WORKDIR /app
COPY . .

RUN cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXE_LINKER_FLAGS="-static-libgcc" && \
    cmake --build build -j"$(nproc)"

# ── Runtime ───────────────────────────────────────────────────────────────
RUN mkdir -p /app/logs

EXPOSE 9090

ENTRYPOINT ["./build/sensor_express"]
CMD ["config/pipeline.yaml"]
