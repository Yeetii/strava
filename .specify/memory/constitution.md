<!--
Sync Impact Report
  Version change: 0.0.0 → 1.0.0 (initial ratification)
  Added principles:
    - I. Geo-First Performance
    - II. Event-Driven & Async
    - III. Frugal Infrastructure
    - IV. Tiled Caching
    - V. Simplicity
  Added sections:
    - Technology Stack
    - Development Workflow
    - Governance
  Removed sections: (none — initial version)
  Templates requiring updates:
    - .specify/templates/plan-template.md ✅ compatible (Constitution Check section generic)
    - .specify/templates/spec-template.md ✅ compatible (no principle-specific refs)
    - .specify/templates/tasks-template.md ✅ compatible (phase structure generic)
  Follow-up TODOs: none
-->

# Peakshunters Constitution

## Core Principles

### I. Geo-First Performance

Every geospatial operation MUST use the cheapest filter first:

- Bounding-box rejection MUST precede distance calculations
- Tiled spatial indexing (slippy tiles) MUST be the primary
  access pattern for OSM features
- Proximity thresholds MUST be defined as constants, not
  magic numbers (e.g. `MaxPeakDistanceMeters = 50`)
- Polygon/line intersection checks run only on candidates
  that survive the bounding-box pass

Rationale: The dataset is large (global OSM), Cosmos RU
budget is tight, and users expect sub-second map responses.

### II. Event-Driven & Async

Data flows through queues, not synchronous call chains:

- Strava webhooks enqueue fetch jobs; change-feed triggers
  enqueue processing jobs; workers write results back
- Workers MUST be idempotent — re-processing the same
  activity ID produces the same outcome
- Real-time updates reach the frontend via SignalR, never
  by polling

Rationale: Decoupled workers scale independently, tolerate
transient failures, and keep the API layer fast.

### III. Frugal Infrastructure

The system runs on Azure free / low-cost tiers:

- All Cosmos writes MUST pass through the shared write gate
  (`CosmosWriteThrottle`) to stay within RU limits
- Bulk upserts MUST be preferred over individual writes when
  handling batches
- Overpass queries MUST use multi-mirror rotation with retry
  and back-off to avoid bans
- New Azure resources MUST justify their cost before adoption

Rationale: This is a personal project; cost discipline keeps
it sustainable.

### IV. Tiled Caching

OSM data MUST be cached by slippy-tile key with a 30-day TTL:

- Point features (peaks) are stored directly on their tile
  document
- Non-point features (paths, areas, admin boundaries) use
  pointer documents to avoid cross-tile duplication
- Cache misses trigger on-demand Overpass fetches that
  populate the tile for subsequent requests
- Deduplication MUST use `LogicalId` when aggregating across
  tiles

Rationale: Overpass mirrors are external and rate-limited;
the tiled cache turns repeated queries into cheap Cosmos
reads.

### V. Simplicity

- No abstractions for one-time operations
- No speculative features — implement what is needed now
- Shared library code MUST be used by at least two consumers
  (API + Backend) to justify its existence
- Prefer flat, explicit code over deep inheritance or
  generic frameworks

Rationale: A small codebase is easier to evolve than a
"well-architected" one nobody can navigate.

## Technology Stack

- **.NET 8 / C# 12** — nullable reference types enabled
- **Azure Functions v4** (isolated worker) for all compute
- **Cosmos DB** (SQL API, direct mode) for all persistence
- **Azure Service Bus** for async job queues
- **Azure SignalR Service** for real-time push
- **BAMCIS.GeoJSON** for Feature/FeatureCollection types
- **Polly** for resilience policies
- **xUnit + Moq** for testing; BenchmarkDotNet for perf
- Deployment: GitHub Actions → Azure Functions (OIDC auth)
- Route constraint: `admin/` prefix is reserved by Azure
  Functions and MUST NOT be used in HttpTrigger routes

## Development Workflow

- Features are developed on branches and merged via PR
- CI builds and deploys API and Backend independently
  (path-filtered GitHub Actions)
- `local.settings.json` holds secrets and is git-ignored
- `func start` is the local run command; restart after
  Backend code changes
- Tests live in `Backend.Tests/` and `Shared.Tests/`;
  run with `dotnet test`

## Governance

This constitution is the authoritative reference for
architectural decisions. Amendments require:

1. A description of the change and its rationale
2. A version bump following semver:
   - MAJOR: principle removal or incompatible redefinition
   - MINOR: new principle, section, or material expansion
   - PATCH: wording, typo, or non-semantic clarification
3. Update of the "Last Amended" date below

All PRs SHOULD be checked against the principles above.
Complexity beyond the current patterns MUST be justified
in the PR description.

**Version**: 1.0.0 | **Ratified**: 2026-04-17 | **Last Amended**: 2026-04-17
