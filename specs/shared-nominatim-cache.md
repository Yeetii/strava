# Shared Nominatim Cache Plan

## Problem

`NominatimLocationGeocodingService` is used by both `Backend` and `PmtilesJob`, but its cache is only local to the current host and process family.

Current behavior:

- requests are globally throttled to 1 request/second with a static `SemaphoreSlim`
- cache entries live in an in-memory static dictionary
- cache contents are snapshotted to `.geocoding-cache/__geocoding/nominatim-cache.json`

Relevant code:

- `Shared/Services/LocationGeocodingService.cs`
- `Backend/Program.cs`
- `PmtilesJob/Program.cs`

This helps repeated lookups on one machine, but it does not help when:

- `Backend` and `PmtilesJob` run on different instances
- a build runs on a fresh machine
- multiple services repeatedly geocode the same race locations over time

The result is avoidable Nominatim traffic and slower PMTiles generation when fallback geocoding is needed.

---

## Goal

Add a shared persisted geocoding cache that:

- is used by all services through `ILocationGeocodingService`
- avoids duplicate Nominatim requests across hosts and processes
- keeps the operational model simple
- does not add aggressive freshness logic

---

## Non-Goals

- building a separate geocoding microservice
- adding automatic cache refresh jobs
- guaranteeing real-time correction if Nominatim changes a result
- changing `RaceAssembler` geocoding behavior

---

## Current State

`RaceAssembler.ResolveFallbackCoordinatesAsync(...)` only calls geocoding when:

1. the selected discovery has no coordinates
2. no other discovery entry already has coordinates
3. a location string exists

Relevant code:

- `Shared/Services/RaceAssembler.cs`

So geocoding is already a fallback path, but it still becomes a blocker because requests are serialized and repeated for the same locations over time.

---

## Proposed Solution

Introduce a shared cache abstraction used by `NominatimLocationGeocodingService`.

Suggested interface:

```csharp
public interface IGeocodingCache
{
    Task<GeocodeCacheEntry?> GetAsync(string location, string countryCode, CancellationToken cancellationToken);

    Task SetAsync(
        string location,
        string countryCode,
        (double lat, double lng)? coordinates,
        DateTimeOffset cachedAtUtc,
        CancellationToken cancellationToken);
}

public sealed record GeocodeCacheEntry(
    string Location,
    string CountryCode,
    bool HasCoordinates,
    double? Latitude,
    double? Longitude,
    DateTimeOffset CachedAtUtc);
```

The Nominatim service then becomes:

1. normalize location and country
2. check shared cache
3. if hit, return cached result
4. if miss, call Nominatim with existing throttling
5. persist hit or miss to shared cache

---

## Storage Recommendation

Use Azure Blob Storage or Azure Table Storage as the shared cache store.

### Option A: Blob-backed cache

Suggested layout:

```
Container: geocoding-cache

by-key/{sha256(location|country)}.json
```

Blob payload:

```json
{
  "location": "Sater, Dalarna",
  "countryCode": "SE",
  "hasCoordinates": true,
  "latitude": 60.35,
  "longitude": 15.75,
  "cachedAtUtc": "2026-07-09T12:34:56.0000000Z"
}
```

Pros:

- already uses blob storage elsewhere in the repo
- easy to inspect manually
- trivial DI wiring in both `Backend` and `PmtilesJob`

Cons:

- one blob read per cache lookup unless a small local memory cache is kept on top

### Option B: Table-backed cache

Suggested key:

- partition key: first 2-3 chars of hash or country code
- row key: `sha256(location|country)`

Pros:

- better fit for key-value lookups at scale
- lower per-entry overhead than many blobs

Cons:

- slightly more moving parts than blobs
- less aligned with current organizer storage patterns

Recommendation: start with blob-backed cache unless lookup volume grows enough to justify Table Storage.

---

## Freshness Policy

Keep the cache intentionally simple.

Recommended behavior:

- cache successful results indefinitely
- cache misses indefinitely
- record `cachedAtUtc`
- do not auto-refresh based on TTL
- allow explicit invalidation for specific keys if needed later

Reasoning:

- race location geocoding is mostly stable
- stale coordinates are lower risk than repeated rate-limited lookups
- negative caching prevents repeated bad queries from hammering Nominatim
- operational simplicity matters more than theoretical freshness

If correction is needed later, an admin or one-off script can delete selected cache entries.

---

## Local Cache Layer

Retain a small in-process memory cache inside `NominatimLocationGeocodingService`.

Recommended lookup order:

1. in-memory dictionary
2. shared persisted cache
3. Nominatim

This preserves fast repeated lookups inside one run while still sharing results across services.

The existing `.geocoding-cache/nominatim-cache.json` snapshot can then become optional debug tooling rather than the primary persistence layer.

---

## Concurrency and Rate Limiting

Keep the current 1 request/second gate for live Nominatim requests.

Important point:

- the shared cache reduces how often services need to hit Nominatim
- it does not remove the need to serialize live requests

The existing `SemaphoreSlim` and 429 handling are still appropriate inside one process.

If many hosts call Nominatim simultaneously, this design still helps because most repeated locations should become cache hits after the first request.

---

## Integration Points

### 1. New shared cache implementation

Add a concrete implementation such as:

- `Shared/Services/BlobGeocodingCache.cs`

### 2. `NominatimLocationGeocodingService`

Update constructor dependencies:

```csharp
public sealed class NominatimLocationGeocodingService(
    HttpClient httpClient,
    IGeocodingCache geocodingCache,
    ILogger<NominatimLocationGeocodingService> logger)
    : ILocationGeocodingService
```

### 3. DI wiring in both hosts

Update:

- `Backend/Program.cs`
- `PmtilesJob/Program.cs`

Register the shared cache before registering `ILocationGeocodingService`.

---

## Observability

Add counters or structured logs for:

- in-memory cache hits
- shared cache hits
- shared cache misses
- live Nominatim requests
- 429 responses
- geocoding misses persisted to cache

This is needed to verify that the shared cache is actually removing pressure from Nominatim.

---

## Rollout Plan

1. Add `IGeocodingCache` and blob-backed implementation
2. Wire it into `NominatimLocationGeocodingService`
3. Keep existing in-memory cache as first-level cache
4. Add hit/miss logging
5. Run PMTiles build and backend workloads against the shared cache
6. Inspect hit rate and request reduction before considering any freshness features

---

## Expected Outcome

This should:

- reduce repeated Nominatim traffic across all services
- reduce PMTiles build stalls caused by repeated geocode fallbacks
- keep the geocoding service simple
- avoid introducing another always-on service just for caching
