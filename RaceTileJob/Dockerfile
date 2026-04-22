FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj ./Shared/Shared.csproj
COPY RaceTileJob/RaceTileJob.csproj ./RaceTileJob.csproj
COPY Shared/Constants ./Shared/Constants
COPY Shared/Models ./Shared/Models
COPY Shared/Services ./Shared/Services
COPY Shared/Geo ./Shared/Geo
COPY Shared/Serialization ./Shared/Serialization
COPY Shared/Properties ./Shared/Properties
COPY RaceTileJob/Program.cs ./Program.cs
COPY RaceTileJob/RaceTileBuildService.cs ./RaceTileBuildService.cs

RUN dotnet publish RaceTileJob.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:8.0

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

WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "RaceTileJob.dll"]
