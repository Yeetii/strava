# PmtilesJob

PmtilesJob builds the race-trail PMTiles archive for the self-hosted stack. It
also contains `PmtilesUtilityService`, a reusable helper for generic PMTiles
operations such as Tippecanoe builds and `tile-join` filtering.

## Race build: scheduler or one-shot

Both modes call the same build service; only the trigger differs.

| Mode | Command | Use |
| --- | --- | --- |
| Scheduler | `dotnet run --project PmtilesJob/PmtilesJob.csproj -- schedule-race-tiles` | Production. Registers a weekly Hangfire recurring job on Redis and stays running. Compose starts the container with this command. |
| One-shot | `dotnet run --project PmtilesJob/PmtilesJob.csproj -- build-race-tiles-from-organizers` | Manual rebuilds and verification. Builds once, then exits. |

With Compose, the one-shot form runs against the shared race-data volume:

```bash
docker compose run --rm pmtiles build-race-tiles-from-organizers
```

One-shot options:

- `--write-transparency` — also write assembled race JSON to disk under
  `{organizerKey}/races/` (default off; also settable via `WriteTransparency`).
- `--output <path>` — override `RaceTiles__OutputPath` for this run.
- `--organizer-id <key>` — debug only: assemble a single organizer.

## Requirements

- .NET 10 SDK (the project targets `net10.0`).
- `tippecanoe` and `tile-join` on `PATH`, or `TippecanoeBinaryPath` /
  `TileJoinBinaryPath` pointing at them. **Both binaries are compiled into the
  container image** at `/usr/local/bin`; a local checkout needs them installed
  separately.
- A running Redis instance for the scheduler and the geocoding cache
  (`RedisConnectionString`). The one-shot build uses Redis for geocoding too.
- Race data on local disk (organizer documents and route GPX), seeded from Blob
  once as described below. After seeding, the race build needs **no Azure
  connection strings** — no Blob, Cosmos or Tables.

## Configuration

Configuration comes from `appsettings.json`, then `local.settings.json`, then
environment variables and command-line arguments. `appsettings.json` is checked
in with non-secret defaults; `local.settings.json` is for local secrets only.

Self-hosted settings (the Compose `pmtiles` service sets these for you):

| Setting | Purpose | Container value |
| --- | --- | --- |
| `OrganizerStore__Provider` | Organizer source | `FileSystem` |
| `OrganizerStore__RaceDataDir` | Root of the disk store | `/data/races` |
| `RaceTiles__OutputPath` | Built archive path | `/data/races/tiles/trails.pmtiles` |
| `RaceTiles__WeeklyCron` | Scheduler cadence | `0 5 * * 1` |
| `WriteTransparency` | Write assembled race JSON | `false` |
| `RedisConnectionString` | Hangfire + geocoding cache | from `.env` |

Optional defaults in `appsettings.json`:

- `TippecanoeBinaryPath` — path to the `tippecanoe` executable.
- `TileJoinBinaryPath` — path to the `tile-join` executable.
- `ForceRaceTileBuild` — when `true`, build even if nothing is marked dirty.

## Seed the local store from Blob (one-off)

Run once during rollout, from the repository root. This is a `Tools` command and
is deliberately **not** part of the pmtiles image:

```bash
dotnet run --project Tools/CosmosBulkDelete.csproj -- export-organizers-to-disk --data-dir /data/races
```

It needs the Blob connection string temporarily to read existing organizers and
routes. Once the directory is populated, the race build runs entirely from disk.

## Utility commands (developer machine only)

The container image contains **only** the race build. These commands remain
available from a local checkout:

```bash
dotnet run --project PmtilesJob/PmtilesJob.csproj -- filter-outdoor --input /path/world.pmtiles --output /path/outdoors.pmtiles
dotnet run --project PmtilesJob/PmtilesJob.csproj -- filter-admin-boundaries --input /path/world.pmtiles --output /path/admin-boundaries.pmtiles
dotnet run --project PmtilesJob/PmtilesJob.csproj -- build-admin-areas --output /path/admin-areas.pmtiles
dotnet run --project PmtilesJob/PmtilesJob.csproj -- build-admin-areas --admin-levels 2,4 --output /path/admin-areas.pmtiles
```

- `build-admin-areas` defaults to countries and regions (`adminLevel` `2` and
  `4`) in separate layers named `countries` and `regions`; `--admin-levels`
  overrides them. It needs Cosmos DB configuration but not Blob Storage.
- The filter commands need neither Cosmos DB nor Blob Storage.
- `PmtilesUtilityService.FilterOutdoorMapAsync(input, output, cancellationToken)`
  keeps `natural`, `landuse`, `water`, `waterway`, `places`, `roads` and
  `boundaries`. It prunes `places` to larger administrative and settlement
  labels (country, region, city, town, village, hamlet, locality), dropping
  neighbourhoods and suburbs.
- `PmtilesUtilityService.FilterAdminBoundariesMapAsync(input, output, cancellationToken)`
  keeps only the `boundaries` layer, filtered to country and region borders at
  admin levels 2 and 4.
- `AdminAreaPmtilesBuildService.BuildAdminAreasAsync(output, adminLevels, cancellationToken)`
  exports full admin-boundary polygons from Cosmos into semantic layers.

## Docker

The image is built from the repository root so the full source context is
available:

```bash
docker build -t pmtilesjob:latest -f PmtilesJob/Dockerfile .
```

In the self-hosted stack, `deploy/docker-compose.yml` builds/pushes it as
`peakshunters-pmtiles` and mounts the shared `race-data` volume. Start the
scheduler with `docker compose up -d pmtiles`, or run one build with the
`docker compose run --rm pmtiles build-race-tiles-from-organizers` command above.
