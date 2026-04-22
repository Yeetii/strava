FROM mcr.microsoft.com/azure-functions/dotnet-isolated:4-dotnet-isolated8.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsqlite3-dev \
    zlib1g-dev \
    git \
    && git clone https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j$(nproc) \
    && make install \
    && rm -rf /tmp/tippecanoe /var/lib/apt/lists/* /tmp/* /var/cache/apt/*

# tippecanoe will be available at /usr/local/bin/tippecanoe
