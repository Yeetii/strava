# Organizer Documents Out of Cosmos — Migration Plan

## Problem

`RaceOrganizerDocument.Scrapers[key].Routes[].Coordinates` stores full GPX coordinate arrays
inline in Cosmos. A single route can be thousands of `[lng, lat]` pairs. This dominates document
size, driving up RU cost for every read and write of an organizer document — including the cheap
discovery-only writes that don't touch routes at all.

Additionally, tile assembly (`RaceFromOrganizersPmtilesBuildService`) does a full `SELECT * FROM c`
on the `raceOrganizers` container every build run, which is the most expensive Cosmos operation.

---

## Solution

**Replace Cosmos entirely for the organizer pipeline with Azure Blob Storage.**

`RaceOrganizerDocument` JSON — coordinates included — is stored directly as a blob per organizer.
The `RaceOrganizerClient` Cosmos operations are replaced with blob read/write.
No model changes. No assembly changes.

---

## Data Flow: Before vs After

### Before
```
Discovery → PatchItemAsync → Cosmos (large doc with coordinate arrays)
Scraper   → PatchItemAsync → Cosmos
Tile build → SELECT * FROM c (full scan, high RU) → RaceAssembler → PMTiles
```

### After
```
Discovery → Download blob → merge → Upload blob (small JSON, or large — same cost)
Scraper   → Download blob → run scrapers → Upload blob (coordinates stay inline)
Tile build → ListBlobsAsync + parallel DownloadContentAsync → RaceAssembler → PMTiles
```

---

## Blob Layout

```
Container: race-organizers

nighttrailrun.se/organizer.json
utmb.world/organizer.json
betrail.run~event~x/organizer.json
tracedetrail.fr~event~x/organizer.json
```

One blob per organizer. The blob path key is the organizer key (same as Cosmos document `id`).
`RaceOrganizerDocument` is serialised as-is — no model changes required.

---

## Cost

At Azure Blob Storage hot tier LRS pricing, assuming ~20,000 organizer blobs totalling ~1–2 GB:

| Item | Annual cost |
|---|---|
| Storage (2 GB) | ~$0.50 |
| Tile build GETs (20k × 52 weeks) | ~$0.45 |
| Scrape writes (~500 PUTs/week) | ~$0.14 |
| Egress (same Azure region) | Free |
| Egress (to local machine for tile build) | ~$8 |
| **Total (same region)** | **~$1/year** |
| **Total (local tile build)** | **~$9/year** |

**vs. current Cosmos cost for tile build alone:**
- 200KB avg doc × 20,000 docs = ~4M RU per build × 52 = 208M RU/year
- At $0.28/million RU (serverless) = **~$58/year just for tile build reads**, plus all scrape RUs

---

## Code Changes

### 1. `IBlobOrganizerStore` interface (new, in `Shared/Services/`)

Mirrors the same operations as `RaceOrganizerClient`:

```csharp
public interface IBlobOrganizerStore
{
    Task<RaceOrganizerDocument?> GetByIdAsync(string organizerKey, CancellationToken ct = default);

    Task WriteAsync(RaceOrganizerDocument doc, CancellationToken ct = default);

    /// <summary>
    /// Read-modify-write with ETag-based optimistic concurrency.
    /// Retries on conflict (low risk — one scrape job per organizer at a time).
    /// </summary>
    Task MergeDiscoveryAsync(string organizerKey, string canonicalUrl, string source,
        List<SourceDiscovery> discoveries, CancellationToken ct = default);

    Task MergeScraperOutputAsync(string organizerKey, string scraperKey,
        ScraperOutput output, ScraperOutputHashes hashes, CancellationToken ct = default);

    Task PatchLastScrapedAsync(string organizerKey, string lastScrapedUtc,
        Dictionary<string, ScraperOutputHashes>? scraperHashes, CancellationToken ct = default);

    Task PatchLastAssembledAsync(string organizerKey, int? maxSlotIndex,
        Dictionary<string, RaceSlotHashes>? assemblyHashes, CancellationToken ct = default);

    /// <summary>List all organizer keys (for tile build and scrape scheduling).</summary>
    IAsyncEnumerable<string> ListKeysAsync(CancellationToken ct = default);

    /// <summary>Stream all documents (for tile build). Downloads in parallel.</summary>
    IAsyncEnumerable<RaceOrganizerDocument> StreamAllAsync(int maxConcurrency = 32, CancellationToken ct = default);

    /// <summary>Return organizer keys not scraped since cutoff (replaces Cosmos SQL query).</summary>
    IAsyncEnumerable<string> GetKeysDueForScrapeAsync(DateTime cutoffUtc, CancellationToken ct = default);
}
```

Implementation: `AzureBlobOrganizerStore` backed by `BlobContainerClient`.

All merge operations use **ETag-based optimistic concurrency** (read with ETag → write with `IfMatch` condition → retry on 412). Contention is negligible since one scrape job runs per organizer at a time.

### 2. `RaceFromOrganizersPmtilesBuildService` — swap Cosmos iterator for blob stream

In `AssembleAndExportToGeoJsonAsync`, replace the Cosmos query iterator:

```csharp
// Before
var iterator = cosmosContainer.GetItemQueryIterator<RaceOrganizerDocument>("SELECT * FROM c");
while (iterator.HasMoreResults)
{
    var page = await iterator.ReadNextAsync(cancellationToken);
    foreach (var doc in page) { /* assemble */ }
}

// After
await foreach (var doc in _blobStore.StreamAllAsync(maxConcurrency: 32, cancellationToken))
{
    /* same assembly logic, no other changes */
}
```

`StreamAllAsync` lists blobs with `GetBlobsAsync()` then fetches 32 documents concurrently.
`CosmosClient` and its DI registration are removed from `PmtilesJob` for this command.

### 3. `ScrapeRaceWorker` — replace `RaceOrganizerClient` with `IBlobOrganizerStore`

- `GetByIdMaybe` → `GetByIdAsync`
- `WriteScraperOutputAsync` → `MergeScraperOutputAsync`
- `PatchScraperPropertiesAsync` → `MergeScraperOutputAsync` (same call; blob read-modify-write replaces partial patch)
- `PatchLastScrapedAsync` → `PatchLastScrapedAsync` (same semantics)

`ScrapeRaceWorker` business logic and hash comparison are unchanged.

### 4. Discovery workers — replace `RaceDiscoveryService.DiscoverAndWriteAsync`

`DiscoverAndWriteAsync` calls `RaceOrganizerClient.WriteDiscoveriesAsync`, which maps to `MergeDiscoveryAsync` on the blob store. The discovery workers themselves are unchanged.

### 5. `QueueAllScrapeJobs` — replace Cosmos SQL query

Currently: `GetIdsDueForAutomaticScrapeAsync` runs `SELECT VALUE c.id FROM c WHERE c.lastScrapedUtc < @cutoff`.

With blob: `GetKeysDueForScrapeAsync` lists all blobs, downloads just the `lastScrapedUtc` field (or uses blob metadata to avoid a full download), and filters client-side.

Options:
- **Blob metadata** — store `lastScrapedUtc` as a blob metadata tag (set on every scrape write) and use `FindBlobsByTagsAsync` for the filtered query. Avoids downloading any document bodies.
- **Index blob** — maintain a single `_index.json` blob with `{organizerKey: lastScrapedUtc}` entries, updated on each scrape write.

Blob metadata tags are the cleanest — no separate index to keep consistent.

---

## What Is Unchanged

- `RaceOrganizerDocument` model — no field changes
- `ScrapedRouteOutput` model — coordinates stay inline (no separate GPX blobs needed)
- `RaceAssembler` — takes a `RaceOrganizerDocument`, returns features, doesn't care about storage
- All scraper logic, hash computation, freshness checks
- `ScrapeRaceWorker` business logic
- All discovery parsing (`RaceScrapeDiscovery.*`, `ScrapeJob`)

---

## Migration: Cosmos → Blob (One-Off)

Export all `raceOrganizers` documents to blobs using a `Tools/` command:

```csharp
var iterator = container.GetItemQueryIterator<RaceOrganizerDocument>("SELECT * FROM c");
while (iterator.HasMoreResults)
{
    foreach (var doc in await iterator.ReadNextAsync())
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(doc, options);
        var blob = containerClient.GetBlobClient($"{doc.Id}/organizer.json");
        await blob.UploadAsync(BinaryData.FromBytes(json), overwrite: true);
    }
}
```

After migration, the `raceOrganizers` Cosmos container can be deleted.

---

## Limitations vs Cosmos

| Feature | Cosmos | Blob |
|---|---|---|
| Atomic partial patch | Yes (`PatchOperation.Set`) | Read-modify-write with ETag |
| Server-side filtering | Yes (`WHERE lastScrapedUtc < @x`) | Client-side or blob metadata tags |
| Change feed | Yes | Azure Storage Events (Event Grid) |
| Concurrency | Optimistic (ETag / `_etag`) | Optimistic (ETag / `IfMatch`) |

None of these are blockers at this scale. Scrape concurrency per organizer is 1, so ETag conflicts are rare. Filtering by `lastScrapedUtc` via blob metadata tags is a single `FindBlobsByTagsAsync` call.

---

## Implementation Order

1. **Export Cosmos → blob** with one-off `Tools/` migration command
2. **Implement `AzureBlobOrganizerStore`** with ETag-based read-modify-write
3. **Update `RaceFromOrganizersPmtilesBuildService`** — swap Cosmos iterator for `StreamAllAsync`, verify tile output is identical
4. **Update `ScrapeRaceWorker`** to use `IBlobOrganizerStore`
5. **Update `QueueAllScrapeJobs`** — use blob metadata tags for staleness filter
6. **Update discovery workers** — wire `MergeDiscoveryAsync`
7. **Remove `CosmosClient` DI registration** from Backend and PmtilesJob for the organizer pipeline; keep it only for containers that remain in Cosmos (`users`, `sessions`, `activities`, etc.)
