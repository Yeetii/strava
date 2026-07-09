# Race Incremental Tiled PBF Plan

## Goal

Replace the current monolithic race PMTiles build with an incremental tiled PBF pipeline that:

- builds one tile or shard at a time
- stores blobs in Azure Blob Storage
- can run inside Azure Functions
- updates affected data one organizer at a time

This is intended as a future direction for improving race generation and assembly. It is not meant to be implemented immediately.

## Current State

The current race pipeline is centered around a whole-world PMTiles build:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs` streams all organizer blobs
- it assembles all races via `RaceAssembler`
- it writes one large GeoJSON file
- it runs `tippecanoe`
- it uploads one `race-tiles/production/trails.pmtiles`

This works, but it is not a good fit for Azure Functions and it is not incremental.

Highways already use a better shape for incremental work:

- canonical blob-backed shard storage
- per-request or per-tile build flow
- in-process clipping, simplification, encoding, and gzip
- deterministic `{z}/{x}/{y}.pbf` storage

Relevant code:

- `API/Endpoints/Tiles/GetHighwayTilePbf.cs`
- `Shared/Services/Shards/BlobTileService.cs`
- `Shared/Services/Shards/BlobShardRepository.cs`
- `Shared/Services/Shards/MvtTileEncoder.cs`

## Important Existing Constraint

The per-organizer assembled race blobs currently written under organizer `races/` folders are marker-only transparency artifacts.

Relevant code:

- `Shared/Services/BlobOrganizerStore.cs`
- `Shared/Services/RaceAssembler.cs`

Specifically:

- line geometries are downgraded to centroids by `RaceAssembler.CreateTransparencyMarker(...)`
- these blobs are useful for admin/debug visibility
- these blobs are not sufficient to rebuild race line tiles

So an incremental race tile pipeline needs a new place to persist full assembled geometry.

## Main Conclusion

Incremental tiled PBF generation for races is feasible, but it should be built as a new shard-plus-tile pipeline rather than as an extension of the current PMTiles archive build.

Also:

- PBF vector tiles are technically editable
- but they should be treated as read-modify-write blobs, not byte-patched in place

In practice, updating one organizer inside an existing PBF shard means:

1. decode the shard
2. remove old features for that organizer
3. add the new features for that organizer
4. re-encode the shard
5. upload it back with ETag concurrency

## Recommended Architecture

### 1. Organizer blob remains source of truth

Keep organizer blobs as they are now:

- `race-organizers/{organizerKey}/organizer.json`

`RaceAssembler` still assembles features from organizer documents.

### 2. Add full assembled organizer snapshots

Add a new organizer-level assembled snapshot that preserves full geometry.

Suggested path:

- `race-organizers/{organizerKey}/assembled-full.json`

This snapshot should contain:

- full assembled race features
- stable `featureId` values
- full line geometry for route races
- point geometry for point-only races

This snapshot is needed so organizer updates can diff old vs new geometry without scanning many shards.

### 3. Add canonical race shards

Add a new blob container, for example:

- `race-shards`

Use a canonical shard zoom of `z8` first, because `RaceAssembler` already stores races at zoom 8 today.

Each shard contains races from multiple organizers.

### 4. Add derived served race tiles

Store served vector tiles separately from canonical shards.

Suggested path shape:

- `race-tiles/{z}/{x}/{y}.pbf`

These are clipped and optionally simplified display tiles derived from canonical shards.

## Canonical Shard Data Model

Two viable options:

### Option A: Canonical shards stored as MVT/PBF

Pros:

- one format end-to-end
- can be directly inspected as vector-tile payloads

Cons:

- updates require decode-modify-reencode every time
- MVT is an output format, not a pleasant mutable source-of-truth format

### Option B: Canonical shards stored as JSON or custom internal binary

Pros:

- simpler mutation logic
- clearer organizer replacement logic
- easier future debugging

Cons:

- served tiles require an extra encode step

Recommendation: prefer Option B.

Use canonical shards as the mutable internal store, and `.pbf` only as the served/cache tile format.

## Canonical Shard Feature Requirements

Each canonical shard feature should carry stable organizer-scoped identity so one organizer can be replaced cleanly.

Minimum useful fields:

- `organizerId`
- `featureId`
- geometry
- core properties used for rendering and lookup

Useful properties:

- `name`
- `distance`
- `raceType`
- `date`
- `website`

If canonical shards are purely internal, they can safely carry more metadata than public tiles need.

## Geometry Rule

Canonical shards should store full unsimplified geometry.

Do not store already-clipped or already-simplified geometry as the canonical mutable source.

Reason:

- repeated organizer updates would otherwise accumulate quality loss
- served tiles should be clipped and simplified from the canonical geometry at build time

## Organizer Update Flow

When organizer `O` changes:

1. load previous full assembled snapshot for `O`
2. assemble the new snapshot for `O`
3. compute old intersecting canonical shard keys
4. compute new intersecting canonical shard keys
5. take the union of those shard keys
6. for each affected shard:
7. load shard blob and ETag if it exists
8. remove all features for organizer `O`
9. add new features from organizer `O` that intersect that shard
10. re-save shard with optimistic concurrency
11. write updated organizer full assembled snapshot

This is the core reason shard features need stable `organizerId` and `featureId` values.

## Served Tile Build Flow

When tile `{z}/{x}/{y}` is requested or invalidated:

1. determine intersecting canonical race shards
2. load those shards
3. collect candidate features
4. clip features to requested tile bounds
5. simplify by zoom if desired
6. encode MVT using existing encoder patterns
7. gzip payload
8. upload or return `{z}/{x}/{y}.pbf`

This should mirror the highways flow as much as possible.

## Azure Functions Fit

This architecture is a much better fit for Azure Functions than the current whole-world PMTiles build.

Good fit:

- one organizer update
- one canonical shard rewrite
- one served tile build
- blob-triggered or HTTP-triggered work

Poor fit:

- whole-dataset `tippecanoe` builds
- large temporary files
- single giant PMTiles publish operations

## Why Not Build Tiles Directly From Organizer Blobs

It is theoretically possible to build one race tile by loading organizer blobs and running `RaceAssembler` on demand, but it is not recommended.

Problems:

- too much repeated assembly work
- too much blob IO per tile request
- too much latency variance for Azure Functions
- hard to keep bounded under load

Canonical race shards are the cleaner intermediate layer.

## Concurrency

Shard rewrites should use blob ETag optimistic concurrency.

Basic pattern:

1. download current shard and ETag
2. modify in memory
3. upload with `IfMatch`
4. retry on conflict

This matters because different organizer updates may touch the same shard.

## Risks

### Shard invalidation complexity

A single organizer update may affect:

- multiple canonical shards
- multiple derived served tiles

### Long routes crossing many shards

One race line may need to be duplicated into several canonical shards.

This is acceptable if measured and kept at reasonable shard zoom.

### Low-zoom fan-out

For zooms below canonical shard zoom, one served tile may depend on many canonical shards.

This is the same class of problem highways already solve.

### Property-schema decisions

Decide early whether served race tiles should contain:

- only ids and minimal display properties
- or richer metadata

## Suggested Blob Layout

### Existing

- `race-organizers/{organizerKey}/organizer.json`

### New

- `race-organizers/{organizerKey}/assembled-full.json`
- `race-shards/{z}/{x}/{y}.json` or internal binary
- `race-tiles/{z}/{x}/{y}.pbf`

If canonical shards are stored as PBF instead:

- `race-shards/{z}/{x}/{y}.pbf`

## Implementation Sequence

1. add full assembled organizer snapshot support
2. add shard-key intersection logic for race features
3. add canonical race shard repository
4. add organizer update flow that rewrites affected shards
5. add race tile build service derived from canonical shards
6. add race tile endpoint such as `GET /tiles/races/{z}/{x}/{y}.pbf`
7. optionally add background invalidation or prebuild for derived tiles
8. keep PMTiles in parallel during rollout if frontend still depends on it

## Recommended Decision

If this work is picked up later, the default recommendation is:

- mutable canonical race shards in JSON or custom internal binary
- served race tiles in gzipped MVT `.pbf`
- organizer-at-a-time updates with shard read-modify-write
- Azure Blob ETag concurrency

This keeps the system incremental without forcing MVT/PBF to be the primary mutable storage format.

## Search Terms

Useful search terms when revisiting this work:

- race PMTiles replacement
- incremental race tiles
- race shard storage
- race assembly optimization
- organizer-at-a-time shard updates
- Azure Functions race tiles
