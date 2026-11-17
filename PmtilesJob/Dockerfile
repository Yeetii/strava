# PmtilesJob image for the self-hosted stack.
#
# This image contains only the race-tile build: the Hangfire scheduler mode
# (`schedule-race-tiles`, which registers a weekly recurring job) and the
# one-shot `build-race-tiles-from-organizers` command. Admin-area, filter, Cosmos and
# export commands are deliberately not the container's job; run those from a
# developer machine if needed.
#
# There is intentionally no default CMD; the Compose service supplies
# `schedule-race-tiles` as its command. For a manual one-shot build use:
#   docker compose run --rm pmtiles build-race-tiles-from-organizers

# ---------------------------------------------------------------------------
# 1. Build the .NET app from the whole repository context. The project also
#    compiles against Shared/, so copying individual folders is fragile.
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish PmtilesJob/PmtilesJob.csproj -c Release -o /app/publish

# ---------------------------------------------------------------------------
# 2. Compile tippecanoe from source. `make install` leaves both
#    /usr/local/bin/tippecanoe and /usr/local/bin/tile-join in this stage.
#    This tracks tippecanoe HEAD; to pin, add
#    `git -C /tmp/tippecanoe checkout <release-tag>` before `cd`/`make`.
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS tippecanoe
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        libsqlite3-dev \
        zlib1g-dev \
    && git clone https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /tmp/tippecanoe /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# 3. Runtime. The ASP.NET Core base is required because PmtilesJob references
#    Microsoft.AspNetCore.App for its Hangfire scheduler. Only tippecanoe's
#    small shared-library dependencies are installed here (not its build deps).
#    `procps` provides pgrep for the Compose worker healthcheck.
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsqlite3-0 \
        zlib1g \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tippecanoe /usr/local/bin/tippecanoe /usr/local/bin/tippecanoe
COPY --from=tippecanoe /usr/local/bin/tile-join /usr/local/bin/tile-join

WORKDIR /app
COPY --from=build /app/publish .
ENV DOTNET_RUNNING_IN_CONTAINER=true
ENTRYPOINT ["dotnet", "PmtilesJob.dll"]
