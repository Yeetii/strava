# Race Scraper — File-System Migration Plan

## Goal

Move the race discovery + scraping + tile-build pipeline off Azure (Cosmos DB, Service Bus, Blob Storage, Azure Functions) to a self-contained CLI workflow runnable on local hardware or a Raspberry Pi.

---

## Current Flow

```
TimerTrigger (Functions)
  → Discovery writes RaceOrganizerDocument to Cosmos
    → Service Bus enqueues scrape message
      → ScrapeRaceWorker reads Cosmos doc, runs scrapers, patches Cosmos doc
        → PmtilesJob queries Cosmos, runs RaceAssembler, uploads to Azure Blob
```

## Target Flow

```
dotnet run -- discover --source all --output-dir ./races-data
dotnet run -- scrape   --input-dir ./races-data --fresh-only
dotnet run -- build-race-tiles --input-dir ./races-data --output ./trails.pmtiles
```

Cron these on the host. No queue, no cloud database, no Functions runtime.

---

## Directory Layout

```
races-data/
  nighttrailrun.se/
    organizer.json        ← RaceOrganizerDocument (discovery + scraper output + hashes)
    routes/
      bfs-0.gpx
      utmb-0.gpx
  betrail.run~event~x/
    organizer.json
    routes/
      bfs-0.gpx
```

`organizer.json` is the same `RaceOrganizerDocument` model already in use — serialised to disk instead of Cosmos. `ScraperOutput` (including `ScraperHashes`) is embedded inline. GPX files are stored in `routes/` alongside the JSON.

---

## What Stays Unchanged

- All scraper logic: `BfsScraper`, `UtmbScraper`, `ItraScraper`, `RaceHtmlScraper`, etc.
- `RaceAssembler` — takes a `RaceOrganizerDocument`, returns features, doesn't care about storage
- `ScrapeJob` and all discovery parsing (`RaceScrapeDiscovery.*`)
- All models: `ScraperOutput`, `SourceDiscovery`, `ScrapedRouteOutput`, `RaceOrganizerDocument`
- `PmtilesUtilityService` — already local
- Hash-based change detection in `ScrapeRaceWorker` — hashes go into `organizer.json` instead of a Cosmos patch

---

## Changes Required

### 1. `FileSystemOrganizerStore` (new, replaces `RaceOrganizerClient`)

Implement the same operations `RaceOrganizerClient` currently provides:

| Current method | File-system equivalent |
|---|---|
| `GetByIdMaybe(key, pk)` | `File.ReadAllText(path/organizer.json)` + deserialise |
| `WriteDiscoveriesAsync(source, items)` | Read → merge discovery → write back |
| `WriteScraperOutputAsync(key, scraperKey, output)` | Read → set `Scrapers[scraperKey]` → write back |
| `PatchScraperPropertiesAsync(...)` | Same as above (no value in partial patching on local disk) |

No patch operations needed — read-modify-write is fast enough on local disk.

### 2. Discovery workers → CLI `discover` command

Each discovery source currently runs as an Azure Functions `TimerTrigger`. Replace with a `discover` subcommand in `PmtilesJob` (or a new `ScraperJob` CLI project):

```
dotnet run -- discover --source betrail --output-dir ./races-data
dotnet run -- discover --source duv     --output-dir ./races-data
dotnet run -- discover --source all     --output-dir ./races-data
```

Sources: `betrail`, `duv`, `itra`, `loppkartan`, `lopplistanRaces`, `runagain`, `skyrunning`, `tracedetrail`, `trailrunningsweden`, `utmb`

Each source calls the same `DiscoverAndWriteAsync` logic, just wired to `FileSystemOrganizerStore` instead of `RaceOrganizerClient + ServiceBusClient`.

### 3. `ScrapeRaceWorker` → CLI `scrape` command

```
dotnet run -- scrape --input-dir ./races-data
dotnet run -- scrape --input-dir ./races-data --fresh-only   # skip recently scraped
dotnet run -- scrape --input-dir ./races-data --key nighttrailrun.se  # single organizer
```

Walk `races-data/*/organizer.json`, apply the same freshness check (`IsFreshEnoughForAutomaticScrape`), run the existing scraper pipeline, write results back to `organizer.json`. No Service Bus — sequential is fine (scraping is I/O-bound, current concurrency is already 1–2).

For Mistral scraping (`ScrapeRaceMistralWorker`): replace `MistralStudioApiUrl` with a local Ollama endpoint (`http://localhost:11434/v1/`).

### 4. `RaceFromOrganizersPmtilesBuildService` — swap Cosmos iterator for directory walk

In `AssembleAndExportToGeoJsonAsync`, replace:

```csharp
// Current: Cosmos query iterator
var query = new QueryDefinition("SELECT * FROM c");
var iterator = cosmosContainer.GetItemQueryIterator<RaceOrganizerDocument>(query);
while (iterator.HasMoreResults) { ... }
```

With:

```csharp
// New: directory walk
foreach (var dir in Directory.EnumerateDirectories(dataPath))
{
    var jsonPath = Path.Combine(dir, "organizer.json");
    if (!File.Exists(jsonPath)) continue;
    var doc = JsonSerializer.Deserialize<RaceOrganizerDocument>(
        await File.ReadAllTextAsync(jsonPath, cancellationToken));
    // same assembly logic
}
```

This is ~10 lines changed. Cosmos and BlobServiceClient DI registrations become optional for this command.

### 5. PMTiles output — remove blob upload

`BuildAsync` currently uploads to Azure Blob Storage after building. Replace with a local file path output. For serving:
- Static file server (nginx) serving the `.pmtiles` file directly — the frontend PMTiles JS library fetches byte ranges locally.
- Or [Martin tile server](https://github.com/maplibre/martin) for a full tile API.

---

## Data Migration (One-Time)

Export existing Cosmos `raceOrganizers` container to the directory layout:

```csharp
// One-off Tools/ script or dotnet-script
var iterator = container.GetItemQueryIterator<RaceOrganizerDocument>("SELECT * FROM c");
while (iterator.HasMoreResults)
{
    foreach (var doc in await iterator.ReadNextAsync())
    {
        var dir = Path.Combine(outputDir, doc.Id);
        Directory.CreateDirectory(dir);
        await File.WriteAllTextAsync(
            Path.Combine(dir, "organizer.json"),
            JsonSerializer.Serialize(doc, options));
    }
}
```

This seeds the local store with all existing discovery + scraper data so the first local scrape run only processes stale organizers.

---

## What Is Lost

| Feature | Impact |
|---|---|
| Parallel scraping via Service Bus | Low — scraping is I/O-bound, sequential is fine for nightly cron |
| Auto-enqueue scrape on new discovery | Scrape must be a separate explicit step; not a problem for scheduled runs |
| Cosmos change-feed triggers | Not used by the scraper path |

---

## Recommended Implementation Order

1. **Export Cosmos → file system** (one-off migration script in `Tools/`)
2. **Implement `FileSystemOrganizerStore`** with the same interface as `RaceOrganizerClient`
3. **Update `RaceFromOrganizersPmtilesBuildService`** to accept a directory path, remove Cosmos + Blob dependencies — verify `build-race-tiles` produces identical output locally
4. **Wire discovery sources to `discover` CLI command** in `PmtilesJob` or a new `ScraperJob` project
5. **Wire `ScrapeRaceWorker` logic to `scrape` CLI command**
6. **Set up cron** on host hardware:
   ```
   0 2 * * 1   discover --source all
   0 3 * * 1   scrape   --fresh-only
   0 5 * * 1   build-race-tiles
   ```
