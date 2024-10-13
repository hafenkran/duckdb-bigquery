# Stage 1: Build Step
FROM ubuntu:24.04 AS builder

# Set environment variables
ENV VCPKG_ROOT=/vcpkg
ENV VCPKG_DEFAULT_TRIPLET=x64-linux
ENV VCPKG_MANIFEST_MODE=1
ENV VCPKG_FEATURE_FLAGS=manifests
ENV VCPKG_TOOLCHAIN_PATH=/vcpkg/scripts/buildsystems/vcpkg.cmake
ENV CMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake
ENV USE_MERGED_VCPKG_MANIFEST=1
ENV GEN=ninja

# Install necessary packages
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    cmake \
    libcurl4-openssl-dev \
    git \
    curl \
    zip \
    unzip \
    tar \
    ninja-build \
    flex \
    bison \
    wget \
    pkg-config \
    python3 \
    && git clone https://github.com/microsoft/vcpkg.git /vcpkg \
    && /vcpkg/bootstrap-vcpkg.sh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /duckdb

COPY . /duckdb

RUN make release

# Stage 2: Final Docker Image
FROM ubuntu:24.04

# Install necessary runtime packages
RUN apt-get update && \
    apt-get install -y \
    libstdc++6 \
    libcurl4 \
    apt-transport-https ca-certificates gnupg curl \
    lsb-release \
    gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    rm -rf /var/lib/apt/lists/*

# Copy DuckDB binaries from the build stage
COPY --from=builder /duckdb/build/release/duckdb /usr/local/bin/duckdb
COPY --from=builder /duckdb/build/release/extension/ /usr/local/lib/duckdb/extension/

# Set the entrypoint
ENTRYPOINT ["duckdb"]
