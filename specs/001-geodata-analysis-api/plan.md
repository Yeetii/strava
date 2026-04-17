# Implementation Plan: Geodata Analysis API

**Branch**: `001-geodata-analysis-api` | **Date**: 2026-04-17 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-geodata-analysis-api/spec.md`

## Summary

Build a source-agnostic GPS trace library API with deep geometric analysis (elevation profiles, grade distribution, splits), route-based effort comparison, and adventure planning workspaces. Extends the existing Azure Functions + Cosmos DB architecture with new Trace/Plan entities, a GPX upload endpoint, and a metric computation engine in Shared/Geo. Existing Strava activities are bridged into the unified trace model. The API is designed for multiple consumers (Trailscope frontend built on gpx.studio, personal dashboard).

## Technical Context

**Language/Version**: C# 12 / .NET 8  
**Primary Dependencies**: Azure Functions v4 (isolated worker), BAMCIS.GeoJSON, Polly, System.Text.Json  
**Storage**: Cosmos DB (SQL API, direct mode) — partition key `/userId` for user-scoped data  
**Testing**: xUnit + Moq; BenchmarkDotNet for perf-critical geo code  
**Target Platform**: Azure Functions (Linux consumption plan)  
**Project Type**: Web service API (Azure Functions HTTP triggers)  
**Performance Goals**: <5s GPX upload+parse for 10 MB files; <2s metric computation for 50 km traces; <1s library queries for 5,000 traces  
**Constraints**: Cosmos free-tier RU budget (write gate), no new Azure resources without cost justification  
**Scale/Scope**: Single user at a time (session-scoped); libraries up to ~5,000 traces; files up to 50 MB

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| **I. Geo-First Performance** | ✅ PASS | Bounding-box filter for library spatial queries; metric computation is per-trace O(n) on points — no cross-trace spatial search needed except route matching, which uses existing bbox+distance pattern |
| **II. Event-Driven & Async** | ✅ PASS | GPX upload is synchronous (user waits for parse result) but metric computation for large files can be deferred to a queue if needed. Plan does not introduce synchronous cross-service calls. Existing Strava pipeline untouched. |
| **III. Frugal Infrastructure** | ✅ PASS | No new Azure resources. Traces stored in existing Cosmos DB. GPX upload via HTTP trigger (no blob storage needed for v1 — file parsed in memory). Write gate used for all Cosmos writes. |
| **IV. Tiled Caching** | ⚠️ N/A | Traces are user-scoped, not tiled OSM data. This principle applies to OSM feature caches, not to the trace library. No violation. |
| **V. Simplicity** | ✅ PASS | Metric computation lives in Shared/Geo (used by API for on-demand metrics and potentially Backend for precomputation). No new abstraction layers. Flat service classes. |

**Gate result: PASS** — no violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/001-geodata-analysis-api/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── api-endpoints.md
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
API/
├── Endpoints/
│   ├── Traces/
│   │   ├── UploadGpx.cs          # POST   traces/upload
│   │   ├── GetTraces.cs          # GET    traces
│   │   ├── GetTrace.cs           # GET    traces/{traceId}
│   │   ├── DeleteTrace.cs        # DELETE traces/{traceId}
│   │   ├── GetTraceMetrics.cs    # GET    traces/{traceId}/metrics
│   │   └── GetRouteMatches.cs    # GET    traces/{traceId}/matches
│   ├── Plans/
│   │   ├── CreatePlan.cs         # POST   plans
│   │   ├── GetPlans.cs           # GET    plans
│   │   ├── GetPlan.cs            # GET    plans/{planId}
│   │   ├── UpdatePlan.cs         # PUT    plans/{planId}
│   │   ├── DeletePlan.cs         # DELETE plans/{planId}
│   │   └── PlanWaypoints.cs      # POST/DELETE plans/{planId}/waypoints
│   └── Aggregates/
│       └── GetTraceStats.cs      # GET    traceStats
├── Program.cs                    # Add Trace + Plan collection registrations

Shared/
├── Geo/
│   ├── GpxParser.cs              # Extend: elevation, timestamps, waypoints, multi-track
│   ├── TraceMetricsCalculator.cs # NEW: elevation profile, grade, splits, smoothing
│   ├── RouteSimilarity.cs        # NEW: geometric overlap % between two traces
│   └── GeoSpatialFunctions.cs    # Extend: distance-along-line helper
├── Models/
│   ├── Trace.cs                  # NEW: unified trace model
│   ├── TraceMetrics.cs           # NEW: computed metrics record
│   ├── Plan.cs                   # NEW: adventure plan model
│   ├── Waypoint.cs               # NEW: named POI model
│   └── ImportBatch.cs            # NEW: upload batch grouping
└── Services/
    ├── TraceCollectionClient.cs  # NEW: Cosmos client for traces
    └── PlanCollectionClient.cs   # NEW: Cosmos client for plans

Shared.Tests/
├── TraceMetricsCalculatorTests.cs
├── RouteSimilarityTests.cs
└── GpxParserTests.cs             # Extend with elevation/timestamp cases
```

**Structure Decision**: Follows the existing project layout exactly. New endpoint files under `API/Endpoints/Traces/` and `API/Endpoints/Plans/`. New geo logic in `Shared/Geo/`. New models in `Shared/Models/`. New collection clients in `Shared/Services/`. No new projects needed — the existing API / Shared / Shared.Tests structure accommodates everything.

## Complexity Tracking

> No constitution violations — this section is empty.
