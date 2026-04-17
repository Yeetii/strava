# Research: Geodata Analysis API

**Feature**: 001-geodata-analysis-api  
**Date**: 2026-04-17

## R1: Trace Storage — Cosmos Container & Partition Key

**Decision**: Store traces in a new `traces` Cosmos container with partition key `/userId`.

**Rationale**: All user-scoped containers in the project (activities, summitedPeaks, visitedPaths, visitedAreas) use `/userId` as partition key. This enables efficient single-partition queries when a user browses their library. Cross-user queries are not needed (no public sharing in v1).

**Alternatives considered**:
- `/id` partition key (used by users/sessions containers): Rejected — filtering by userId would require cross-partition queries for every library listing.
- Storing traces inside the existing `activities` container: Rejected — activities are tightly coupled to Strava's schema and change-feed triggers. Mixing GPX-uploaded traces would require discriminator fields and risk triggering unnecessary Strava processing workers.

## R2: GPX Parser Extension — Elevation, Timestamps, Waypoints

**Decision**: Extend the existing `GpxParser` to extract `<ele>`, `<time>`, and `<wpt>` elements. Create a new `TracePoint` record (`Lat, Lng, Elevation?, Timestamp?`) instead of reusing `Coordinate` (which has no elevation/timestamp and is used widely).

**Rationale**: The current parser only extracts lat/lng from `<trkpt>` and flattens all segments into one list. Elevation and timestamps are core to metric computation (FR-003, FR-004). Waypoints are needed for import (US1-AS3). Creating `TracePoint` avoids breaking the 20+ existing usages of `Coordinate` across the codebase.

**Alternatives considered**:
- Extending `Coordinate` with nullable `Elevation` and `Timestamp` properties: Rejected — `Coordinate` is used wherever lat/lng is needed (bounding boxes, tile calculations, polyline decoding). Adding optional fields would bloat unrelated code paths and risk serialization issues with existing Cosmos documents.
- Using a third-party GPX library (e.g. `NetTopologySuite.IO.GPX`): Rejected per Principle V (Simplicity) — the parsing logic is straightforward XML and the existing parser handles DTD-disabled safe parsing. Adding a dependency for XML attribute extraction is not warranted.

## R3: Metric Computation — Synchronous vs Async

**Decision**: Compute metrics synchronously on the API request path (`GET traces/{traceId}/metrics`). Cache the result in the trace document after first computation.

**Rationale**: SC-002 requires <2s for a 50 km trace. A 50 km trace at 1-point-per-second has ~18,000 points (5h moving). Elevation profile, grade distribution, and splits are single-pass O(n) algorithms over these points — well within 2s. Deferring to a Service Bus queue would add latency and complexity for a computation that completes in milliseconds.

**Alternatives considered**:
- Pre-compute on upload via Backend worker (queue-triggered): Rejected for v1 — adds queue infrastructure for a computation that's fast enough to run on-demand. Can be revisited if computation becomes expensive (e.g. DEM elevation lookup).
- Compute on every request without caching: Rejected — unnecessary repeated work. Once metrics are cached, subsequent reads are a single Cosmos point-read.

## R4: Elevation Smoothing Strategy

**Decision**: Apply a simple moving-window filter (5-point window) that replaces outlier elevation values. A point is an outlier if its elevation differs from the window median by more than 50 metres.

**Rationale**: GPS barometric altimeters produce occasional spikes (spec edge case: ±500m in one second). A 5-point median filter is robust against isolated spikes without distorting the actual profile shape. This runs in O(n) time with a sliding window.

**Alternatives considered**:
- Kalman filter: Rejected per Principle V — over-engineered for the noise profile of consumer GPS devices.
- No smoothing (raw data only): Rejected — spec FR-010 explicitly requires basic smoothing.
- Douglas-Peucker simplification: Rejected — reduces point count rather than fixing elevation values; not suited for altitude noise.

## R5: Route Similarity Algorithm

**Decision**: Use a sampled Fréchet-like approach: sample the reference trace at regular distance intervals (every 50m), then for each sample point find the nearest point on the candidate trace. The overlap percentage is the fraction of sample points where the nearest-candidate distance is ≤100m.

**Rationale**: Full Fréchet distance is O(n×m) with high constant factors. Sampling at 50m intervals reduces a 50km trace to 1,000 points. For each sample point, a bounding-box spatial index on the candidate trace enables fast nearest-neighbour lookup. This satisfies SC-004 (90% precision) while being fast enough for interactive use.

**Alternatives considered**:
- Hausdorff distance: Rejected — sensitive to outlier points (a single GPS spike can dominate the metric).
- Full discrete Fréchet distance: Rejected — too slow for potentially comparing against thousands of candidate traces.
- Encode both traces as tile sets and compare set overlap: Rejected — too coarse at zoom levels that would be performant; would miss direction and ordering.

## R6: File Upload Approach

**Decision**: Accept GPX files as the raw request body (`Content-Type: application/gpx+xml` or `application/xml`). Read `HttpRequestData.Body` as a stream. No multipart form handling.

**Rationale**: Azure Functions isolated worker has no built-in multipart parser. Since we accept only GPX files (not mixed form data), a raw body upload is simpler and eliminates the need for a multipart parsing library. The frontend sends the file content directly. File size limit (50 MB) is enforced by reading up to the limit and rejecting if exceeded.

**Alternatives considered**:
- Multipart/form-data: Rejected — requires manual boundary parsing or a NuGet dependency. Only beneficial if uploading metadata alongside files, which can instead be sent as query parameters or headers.
- Azure Blob Storage upload + queue: Rejected per Principle III (Frugal Infrastructure) — adds a new Azure resource. In-memory parsing is fine for files up to 50 MB.

## R7: Plan Storage Model

**Decision**: Store plans in a new `plans` Cosmos container with partition key `/userId`. Waypoints are embedded as an array inside the plan document. Linked traces are stored as reference IDs (not embedded).

**Rationale**: A plan has at most a few dozen waypoints — well within Cosmos's 2 MB document limit. Embedding avoids extra reads for the common "get plan with all waypoints" query. Linked traces are references because they already exist in the traces container and should not be duplicated.

**Alternatives considered**:
- Separate waypoints container: Rejected — adds a join query for every plan read. Waypoints are always accessed with their parent plan.
- Embedding trace geometry in plans: Rejected — traces can be large (thousands of points) and are already stored separately. References keep plan documents small.

## R8: Strava Activity Bridge

**Decision**: Existing Strava activities are NOT migrated into the traces container. Instead, the trace library query endpoint queries both the `traces` container and the `activities` container, merging results. The activities are projected into the same response schema as traces.

**Rationale**: Migrating ~5,000 activities per user would require a one-time batch job, duplicate data, and create a sync problem (new Strava activities would need to be written to both containers). Querying two containers and merging is simpler and avoids data duplication. The projection can happen at the API layer — both return the same summary fields.

**Alternatives considered**:
- One-time migration + dual-write on new activities: Rejected per Principle V — complex ongoing synchronization for a problem that's solved by merging at query time.
- Replace the activities container entirely: Rejected — existing workers (SummitsWorker, VisitedPathsWorker, VisitedAreasWorker) depend on the current Activity model and change-feed. Changing the container would require rewriting the entire processing pipeline.

## R9: Duplicate Detection

**Decision**: Compute SHA-256 hash of the GPX file content (after normalizing whitespace). Store as `contentHash` on the Trace document. On upload, query for existing traces with the same hash and userId. Return a warning header but proceed with the import.

**Rationale**: Content hash is a reliable, fast duplicate check. Normalizing whitespace handles reformatting differences between export tools. Warning (not blocking) satisfies spec requirement — users may intentionally re-upload the same GPX for different efforts.

**Alternatives considered**:
- Block duplicate uploads: Rejected — spec explicitly states "a second import is allowed."
- Hash only the coordinate data (not full file): Rejected — two files with different metadata but identical coordinates would collide, which may not be the user's intent.
