# PMTiles Build Performance Plan

## Problem

`RaceFromOrganizersPmtilesBuildService` currently builds race PMTiles as a full-world batch job.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`
- `PmtilesJob/PmtilesUtilityService.cs`
- `Shared/Services/RaceAssembler.cs`
- `Shared/Services/BlobOrganizerStore.cs`

The current build path is:

1. stream all organizers from blob storage
2. assemble every organizer into race features
3. write transparency artifacts back to blob storage
4. accumulate every feature in memory
5. serialize one giant GeoJSON file
6. run `tippecanoe`
7. upload one PMTiles file

This works, but it combines several different bottlenecks into one long-running job.

---

## Current Bottlenecks

### 1. Full re-assembly on every run

Every organizer is assembled every time, even if no race-relevant input changed.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`

### 2. Transparency writes are on the hot path

The build enqueues `WriteAssembledRacesAsync(...)` during assembly.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`
- `Shared/Services/BlobOrganizerStore.cs`

These writes are useful for inspection, but they are not required to produce the final PMTiles archive.

### 3. Full in-memory feature accumulation

The job stores all generated `Feature` objects in one large list before writing GeoJSON.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`

This increases memory pressure as dataset size grows.

### 4. One giant GeoJSON serialization step

The build constructs a `FeatureCollection`, calls `ToJson()`, and writes one large file.

Relevant code:

- `PmtilesJob/RaceFromOrganizersPmtilesBuildService.cs`

This is a CPU and memory heavy stage independent of blob I/O.

### 5. `tippecanoe` remains a whole-dataset step

`PmtilesUtilityService.BuildPmtilesAsync(...)` runs `tippecanoe` on the full GeoJSON input.

Relevant code:

- `PmtilesJob/PmtilesUtilityService.cs`

This is expected, but it means any upstream savings need to reduce the size and cost of the produced input set.

---

## Goal

Reduce PMTiles generation time and memory pressure without immediately replacing the whole-world PMTiles architecture.

---

## Status

This plan is now partially implemented.

Implemented:

- transparency writes are optional and disabled by default for race tile builds
- stage-level instrumentation is in place for organizer reads, geocoding, assembly, projection, GeoJSON serialization, tippecanoe, and final PMTiles size
- GeoJSON is streamed directly to disk during assembly instead of collecting all features into one in-memory `FeatureCollection`

Still pending:

- organizer-level incremental skipping
- `AssemblyHashes`-based build reuse
- any deeper architectural move away from whole-world PMTiles

---

## Non-Goals

- replacing PMTiles with a served tile API in this spec
- introducing shard-based race tiles here
- redesigning `RaceAssembler`

Incremental tiled PBF remains a separate future direction.

---

## Recommended Improvements

## 1. Add organizer-level incremental skipping

Status: not implemented.

Use existing organizer state to skip re-assembling organizers whose tile-relevant inputs have not changed.

Relevant existing fields:

- `ScraperHashes`
- `LastAssembledUtc`
- `AssemblyHashes`

Relevant code:

- `Shared/Models/RaceOrganizerDocument.cs`

Recommended first step:

- skip entire organizers before downloading or assembling full source blobs when summary state shows no relevant change

This depends on the summary/index work described in `specs/organizer-blob-read-optimization.md`.

---

## 2. Activate `AssemblyHashes` for future build reuse

Status: not implemented.

`RaceOrganizerDocument` already contains fields that appear intended for assembly-level change tracking.

Suggested use:

- after assembly, compute stable per-feature hashes
- persist them back as `AssemblyHashes`
- on later builds, compare old and new feature-level hashes to identify changed outputs

This is more complex than organizer-level skipping, but it creates a path toward reusing prior assembly results rather than regenerating everything.

---

## Suggested Implementation Order

### Phase 1: reduce unnecessary work

Remaining:

1. add summary-based organizer prefiltering
2. skip unchanged organizers

### Phase 2: deeper incremental reuse

Remaining:

3. persist and consume `AssemblyHashes`
4. evaluate whether whole-world PMTiles is still sufficient or whether shard-based tiles should take over

---

## Validation

For each phase, compare:

- total build wall-clock time
- total bytes fetched from organizer blob storage
- peak memory usage during GeoJSON generation
- number of organizers assembled
- number of Nominatim calls
- final PMTiles size

The build should still produce a valid PMTiles file and preserve existing visible tile output.

---

## Expected Outcome

This plan should improve PMTiles generation even before any larger architectural move.

Most likely order of impact:

1. skip unchanged organizers
2. reduce full organizer blob downloads
3. remove transparency side work from the hot path
4. lower memory and serialization cost by streaming GeoJSON

If these improvements are still insufficient, the next step should be the separate incremental tiled PBF direction rather than continuing to scale the monolithic PMTiles build indefinitely.
