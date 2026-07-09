# Organizer Blob Read Optimization Plan

## Problem

PMTiles generation currently reads organizer data from blob storage in a way that still pulls a large amount of heavy document data.

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`
- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`

The current organizer layout is:

```
race-organizers/{organizerKey}/organizer.json
```

Each `organizer.json` may contain large embedded route coordinate arrays inside scraper outputs. That makes the full document expensive to fetch even when tile generation only needs to know whether the organizer changed.

---

## Current Bottlenecks

### 1. Full document downloads for every organizer

`RaceFromOrganizersPmtilesBuildService` streams all organizers and assembles every one on every build.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`

### 2. "Metadata-only" reads still download full blobs

`StreamMetadataWithoutGeometriesAsync(...)` deserializes a lighter shape, but it still gets there by downloading the full `organizer.json` blob first.

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`

### 3. Blob listing is chatty

The current stream path:

1. lists top-level prefixes
2. lists each organizer prefix again
3. then downloads the blob

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`

### 4. Existing local cache only helps on warm hosts

`BlobOrganizerStore` already has an ETag-based local cache, but it only helps if the same host sees the same organizer blobs again.

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`

This does not solve the baseline problem that PMTiles generation still treats every organizer as a full blob read candidate.

---

## Goal

Reduce organizer-related blob I/O during PMTiles generation by:

- avoiding full organizer downloads when an organizer has not changed
- reducing blob listing chatter
- making lightweight organizer metadata actually lightweight over the network

---

## Existing Reusable Fields

The model already contains state that can support incremental decisions:

- `ScraperHashes`
- `LastAssembledUtc`
- `LastMaxSlotIndex`
- `AssemblyHashes`

Relevant code:

- `Shared/Models/RaceOrganizerDocument.cs`
- `Shared/Services/OrganizerBlobMetadata.cs`

There is also already a projection-oriented API:

- `BlobOrganizerStore.StreamMetadataWithoutGeometriesAsync(...)`

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`

That API proves there is already a need for lighter organizer reads, but it is not yet reducing network transfer because it still downloads the full source blob first.

These fields should become the basis for deciding whether a full organizer read is required.

---

## Proposed Solution

Split organizer storage into two logical layers:

1. a lightweight organizer summary/index blob
2. the full `organizer.json` source blob

Suggested layout:

```
race-organizers/{organizerKey}/organizer.json
race-organizers/{organizerKey}/summary.json
```

`summary.json` should contain enough data to decide whether PMTiles assembly must touch the full organizer blob.

---

## `summary.json` Shape

Suggested contents:

```json
{
  "id": "nighttrailrun.se",
  "url": "https://nighttrailrun.se",
  "lastScrapedUtc": "2026-07-09T12:34:56.0000000Z",
  "lastAssembledUtc": "2026-07-09T13:00:00.0000000Z",
  "lastMaxSlotIndex": 4,
  "scraperHashes": {
    "bfs": {
      "propertiesHash": "...",
      "routesHash": "..."
    }
  },
  "assemblyHashes": {
    "nighttrailrun.se-0": {
      "assemblyVersion": 1,
      "propertiesHash": "...",
      "geometryHash": "..."
    }
  },
  "discovery": {
    "utmb": [
      {
        "name": "Night Trail Run",
        "date": "2026-08-01",
        "location": "Sater, Dalarna",
        "country": "SE",
        "latitude": 60.35,
        "longitude": 15.75
      }
    ]
  },
  "scrapers": {
    "bfs": {
      "websiteUrl": "https://nighttrailrun.se/race",
      "extractedName": "Night Trail Run",
      "scrapedAtUtc": "2026-07-09T12:30:00.0000000Z",
      "routes": [
        {
          "name": "50K",
          "distance": "50 km"
        }
      ]
    }
  }
}
```

Important rule:

- no route coordinate arrays in `summary.json`

---

## PMTiles Build Flow With Summary Blobs

### Before

```
List organizers
  → Download every organizer.json
    → Assemble every organizer
      → Build PMTiles
```

### After

```
List summaries
  → Compare scraper/assembly state
    → Download organizer.json only for changed organizers
      → Assemble changed organizers
        → Reuse previous assembly output for unchanged organizers
```

This does not by itself define the final reuse format for unchanged assemblies, but it removes the need to fetch large source docs for obviously unchanged organizers.

---

## Incremental Decision Rule

The minimum useful first step is organizer-level skipping.

Suggested rule:

- if `scraperHashes` and relevant discovery inputs are unchanged since `lastAssembledUtc`, skip full organizer download and skip re-assembly
- otherwise download `organizer.json` and assemble

This is lower risk than per-feature incremental replacement and still removes a lot of unnecessary reads.

---

## How `summary.json` Stays Updated

Whenever organizer data changes:

1. write or update `organizer.json`
2. derive and write `summary.json`

This should happen in the same application flow that already writes organizer blobs.

Primary writer paths:

- discovery writes
- scraper writes
- any future assembly state write-backs

The summary is derived data, not a second source of truth.

---

## Alternative: Blob Metadata Only

Another option is to push some fields into blob metadata or tags instead of `summary.json`.

Examples:

- `lastScrapedUtc`
- `lastAssembledUtc`
- a combined organizer content hash

This helps for coarse filtering, but it is too limited for richer incremental decisions because:

- blob metadata is small and awkward for structured data
- `scraperHashes` and `assemblyHashes` are nested objects
- discovery-derived coordinate fallback state may matter

Recommendation: use a real summary blob, not metadata-only, for PMTiles-oriented optimization.

---

## Blob Listing Optimization

Separate from summary blobs, `BlobOrganizerStore.StreamOrganizerBlobsAsync(...)` should be simplified.

Recommended change:

- replace the current list-prefix then list-each-prefix pattern with a flatter blob scan that groups keys in memory

This reduces extra storage list requests and will help all organizer streaming paths.

---

## Local Cache Role

Keep the existing ETag-based local cache in `BlobOrganizerStore`.

It remains valuable for:

- repeated local development runs
- reruns on the same build host
- fallback resilience when the same blobs are touched repeatedly

But it should be treated as a warm-cache optimization, not the main solution to heavy organizer reads.

---

## Rollout Plan

1. Add `summary.json` model and writer support in `BlobOrganizerStore`
2. Update discovery and scraper write flows to keep `summary.json` in sync
3. Add a summary streaming API, for example `StreamSummariesAsync(...)`
4. Update PMTiles generation to pre-scan summaries before deciding which full organizer blobs to download
5. Simplify blob listing logic in `StreamOrganizerBlobsAsync(...)`
6. Measure downloaded bytes before and after

---

## Expected Outcome

This should:

- substantially reduce full organizer blob downloads during PMTiles generation
- reduce the impact of large embedded route/course files
- preserve `organizer.json` as the canonical source document
- create a clean foundation for later incremental assembly work
