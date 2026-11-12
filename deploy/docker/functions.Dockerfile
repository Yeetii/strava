# syntax=docker/dockerfile:1.7
#
# Builds one Azure Functions isolated-worker app (API or Backend) into the
# official Functions runtime image. The same source that runs in Azure is used
# here; no backend code is forked for self-hosting.

ARG DOTNET_VERSION=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build

ARG PROJECT=API
WORKDIR /src

# Restore against the project files first so source-only changes reuse the
# NuGet layer.
COPY Directory.Build.props ./
COPY Shared/Shared.csproj Shared/
COPY API/API.csproj API/
COPY Backend/Backend.csproj Backend/
RUN dotnet restore "${PROJECT}/${PROJECT}.csproj"

COPY Shared/ Shared/
COPY API/ API/
COPY Backend/ Backend/

RUN dotnet publish "${PROJECT}/${PROJECT}.csproj" \
    --configuration Release \
    --no-restore \
    --output /app/publish

FROM mcr.microsoft.com/azure-functions/dotnet-isolated:4-dotnet-isolated10.0 AS final

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    FUNCTIONS_WORKER_RUNTIME=dotnet-isolated \
    FUNCTIONS_EXTENSION_VERSION=~4 \
    WEBSITES_ENABLE_APP_SERVICE_STORAGE=false \
    DOTNET_RUNNING_IN_CONTAINER=true

COPY --from=build /app/publish /home/site/wwwroot

# StoreCuratedPeaksGroups reads ./peaks.json. It is intentionally excluded from
# the normal publish output, so copy it explicitly for the self-hosted image.
COPY Backend/peaks.json /home/site/wwwroot/peaks.json

WORKDIR /home/site/wwwroot
EXPOSE 80
