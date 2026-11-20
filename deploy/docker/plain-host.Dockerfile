# syntax=docker/dockerfile:1.7
#
# Plain .NET 10 host image for the self-hosted stack. Produces the same
# published output for all four roles; WORKER_ROLE at runtime selects
# api / schedule / heavy. No Functions runtime packages remain.

ARG DOTNET_VERSION=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build

ARG PROJECT=Stigar.Api
WORKDIR /src

COPY Directory.Build.props ./
COPY Stigar.Core/Stigar.Core.csproj Stigar.Core/
COPY Stigar.Api/Stigar.Api.csproj Stigar.Api/
COPY Stigar.Workers/Stigar.Workers.csproj Stigar.Workers/
RUN dotnet restore "${PROJECT}/${PROJECT}.csproj"

COPY Stigar.Core/ Stigar.Core/
COPY Stigar.Api/ Stigar.Api/
COPY Stigar.Workers/ Stigar.Workers/

RUN dotnet publish "${PROJECT}/${PROJECT}.csproj" \
    --configuration Release \
    --no-restore \
    --output /app/publish

# Shared runtime layer for both the source-built (local) and prebuilt (CI) images.
FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS runtime-base

# procps provides pgrep for the worker-role healthcheck in docker-compose.yml.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

ENV DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_ENVIRONMENT=Production \
    ASPNETCORE_HTTP_PORTS=80

WORKDIR /app
EXPOSE 80
ENTRYPOINT ["sh", "-c", "exec dotnet \"${APP_DLL}\""]

# CI target: the build context is one output directory from
# deploy/scripts/publish-images.sh, which already contains the published app.
FROM runtime-base AS prebuilt
ARG APP_DLL
ENV APP_DLL=${APP_DLL}
COPY . /app

# Default target (local `docker compose ... --build`): compile from source.
FROM runtime-base AS final

ARG PROJECT=Stigar.Api
ENV APP_DLL=${PROJECT}.dll

COPY --from=build /app/publish /app

# StoreCuratedPeaksGroups reads ./peaks.json.
COPY Stigar.Api/peaks.json /app/peaks.json
