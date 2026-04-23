# RaceTileJob

RaceTileJob is a small .NET console app that builds PMTiles for race trail geometry when the source data is marked dirty.

> To force a rebuild regardless of the dirty marker, run:
>
> ```bash
> dotnet run --project RaceTileJob.csproj -- --ForceRaceTileBuild true
> ```
>
## Requirements

- .NET 8 SDK
- Access to a Cosmos DB instance containing the `races` container
- Access to an Azure Blob Storage account for tile staging and production
- `tippecanoe` binary installed, or set `TippecanoeBinaryPath` to a custom path

## Configuration

The app reads configuration from `appsettings.json`, then `local.settings.json`, followed by environment variables and command line arguments.

- `appsettings.json` is checked in and contains non-secret defaults.
- `local.settings.json` is for local secrets only.

Required settings:

- `CosmosDBConnection` - Cosmos DB connection string
- `BlobStorageConnection` - Azure Blob Storage connection string

Optional settings (configured in `appsettings.json`):

- `RaceTilesBlobContainerName` - blob container name to use (default: `race-tiles`)
- `TippecanoeBinaryPath` - path to the `tippecanoe` executable
- `ForceRaceTileBuild` - when `true`, build even if no dirty marker exists (only the first run)

## Docker

The project includes a `Dockerfile` that builds the app and installs `tippecanoe`.

Build the image:

```bash
docker build -t racetilejob:latest -f RaceTileJob/Dockerfile .
```

Run the container:

```bash
docker run --rm \
  -e CosmosDBConnection="<your-cosmos-connection>" \
  -e BlobStorageConnection="<your-blob-connection>" \
  racetilejob:latest
```

## Notes

- The app will build PMTiles when the dirty marker blob exists, or when `ForceRaceTileBuild` is enabled for the first run.
- The dirty marker is created by the backend race change feed when race features change.
