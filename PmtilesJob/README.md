# PmtilesJob

PmtilesJob is a small .NET console app that builds PMTiles for race trail geometry when the source data is marked dirty. The project now also contains a reusable `PmtilesUtilityService` for generic PMTiles operations such as Tippecanoe builds and `tile-join` filtering.

> To force a rebuild regardless of the dirty marker, run:
>
> ```bash
> dotnet run --project PmtilesJob.csproj -- --Force
> ```
>
> To run the outdoor filter entrypoint:
>
> ```bash
> dotnet run --project PmtilesJob.csproj -- filter-outdoor --input /path/world.pmtiles --output /path/outdoors.pmtiles
> ```
>
> To run the admin-boundaries filter entrypoint:
>
> ```bash
> dotnet run --project PmtilesJob.csproj -- filter-admin-boundaries --input /path/world.pmtiles --output /path/admin-boundaries.pmtiles
> ```
>
> To build filled admin areas from Cosmos-stored admin boundary polygons:
>
> ```bash
> dotnet run --project PmtilesJob.csproj -- build-admin-areas --output /path/admin-areas.pmtiles
> ```
>
> The default `build-admin-areas` output combines countries and regions (`adminLevel` `2` and `4`) into the same PMTiles, with separate vector layers named `countries` and `regions`.
> To choose specific levels explicitly:
>
> ```bash
> dotnet run --project PmtilesJob.csproj -- build-admin-areas --admin-levels 2,4 --output /path/admin-areas.pmtiles
> ```
>
## Requirements

- .NET 8 SDK
- Access to a Cosmos DB instance containing the `races` container
- Access to an Azure Blob Storage account for tile staging and production when running the default race tile build
- `tippecanoe` binary installed, or set `TippecanoeBinaryPath` to a custom path
- `tile-join` binary installed, or set `TileJoinBinaryPath` to a custom path

## Configuration

The app reads configuration from `appsettings.json`, then `local.settings.json`, followed by environment variables and command line arguments.

- `appsettings.json` is checked in and contains non-secret defaults.
- `local.settings.json` is for local secrets only.

Required settings:

- `CosmosDBConnection` - Cosmos DB connection string
- `BlobStorageConnection` - Azure Blob Storage connection string

The `filter-outdoor` and `filter-admin-boundaries` commands do not require Cosmos DB or Blob Storage configuration.
The `build-admin-areas` command requires Cosmos DB configuration but does not require Blob Storage.

Optional settings (configured in `appsettings.json`):

- `RaceTilesBlobContainerName` - blob container name to use (default: `race-tiles`)
- `TippecanoeBinaryPath` - path to the `tippecanoe` executable
- `TileJoinBinaryPath` - path to the `tile-join` executable
- `ForceRaceTileBuild` - when `true`, build even if no dirty marker exists (only the first run)

## Docker

The project includes a `Dockerfile` that builds the app and installs `tippecanoe`.

Build the image:

```bash
docker build -t pmtilesjob:latest -f PmtilesJob/Dockerfile .
```

Run the container:

```bash
docker run --rm \
  -e CosmosDBConnection="<your-cosmos-connection>" \
  -e BlobStorageConnection="<your-blob-connection>" \
  pmtilesjob:latest
```

## Notes

- The app will build PMTiles when the dirty marker blob exists, or when `ForceRaceTileBuild` is enabled for the first run.
- The dirty marker is created by the backend race change feed when race features change.
- `PmtilesUtilityService.FilterOutdoorMapAsync(input, output, cancellationToken)` keeps `natural`, `landuse`, `water`, `waterway`, `places`, `roads`, and `boundaries` when filtering a larger Protomaps PMTiles input for outdoor usage.
- The outdoor filter currently prunes `places` labels down to larger administrative and settlement labels such as `country`, `region`, `city`, `town`, `village`, `hamlet`, and `locality`, while dropping tiny local urban labels such as neighbourhoods and suburbs.
- `PmtilesUtilityService.FilterAdminBoundariesMapAsync(input, output, cancellationToken)` keeps only the `boundaries` layer and filters it down to country and region borders corresponding to admin levels 2 and 4.
- `AdminAreaPmtilesBuildService.BuildAdminAreasAsync(output, adminLevels, cancellationToken)` exports full admin-boundary polygons from Cosmos and builds a PMTiles archive with semantic layers such as `countries` and `regions`.
- Supported commands are the default race-tile build flow, `filter-outdoor --input <path> --output <path>`, `filter-admin-boundaries --input <path> --output <path>`, and `build-admin-areas --admin-level <n> --output <path>`.
