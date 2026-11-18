# syntax=docker/dockerfile:1.7
#
# Plain .NET 10 host image for the self-hosted stack. Produces the same
# published output for all four roles; WORKER_ROLE at runtime selects
# api / schedule / heavy. No Functions runtime packages remain.

ARG DOTNET_VERSION=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build

ARG PROJECT=API
WORKDIR /src

COPY Directory.Build.props ./
COPY Shared/Shared.csproj Shared/
COPY Shared.Hosting/Shared.Hosting.csproj Shared.Hosting/
COPY API/API.csproj API/
COPY Backend/Backend.csproj Backend/
RUN dotnet restore "${PROJECT}/${PROJECT}.csproj"

COPY Shared/ Shared/
COPY Shared.Hosting/ Shared.Hosting/
COPY API/ API/
COPY Backend/ Backend/

RUN dotnet publish "${PROJECT}/${PROJECT}.csproj" \
    --configuration Release \
    --no-restore \
    --output /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS final

ARG PROJECT=API
# procps provides pgrep for the worker-role healthcheck in docker-compose.yml.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

ENV DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_ENVIRONMENT=Production \
    ASPNETCORE_HTTP_PORTS=80 \
    APP_DLL=${PROJECT}.dll

COPY --from=build /app/publish /app

# StoreCuratedPeaksGroups reads ./peaks.json.
COPY Backend/peaks.json /app/peaks.json

WORKDIR /app
EXPOSE 80
ENTRYPOINT ["sh", "-c", "exec dotnet \"$APP_DLL\""]
