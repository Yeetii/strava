# Data Model: Geodata Analysis API

**Feature**: 001-geodata-analysis-api

## Overview

Two new Cosmos containers (`traces`, `plans`) plus new model types in `Shared/Models/`. All user-scoped, partition key `/userId`, following the existing `IDocument` pattern.

---

## Cosmos Containers

### `traces` (new)

| Property | Type | Description |
|---|---|---|
| Partition key | `/userId` | Consistent with all user-scoped containers |
| TTL | -1 (no expiry) | |
| Indexing | Default + spatial on `boundingBox` | |

### `plans` (new)

| Property | Type | Description |
|---|---|---|
| Partition key | `/userId` | |
| TTL | -1 | |
| Indexing | Default | |

---

## Entities

### TracePoint

New value type for GPS points with elevation and time. Does NOT replace `Coordinate` (used in 20+ places for simple lat/lng).

```csharp
// Shared/Models/TracePoint.cs
namespace Shared.Models;

public record TracePoint(
    double Lat,
    double Lng,
    double? Elevation = null,
    DateTime? Timestamp = null
);
```

### Trace

Cosmos document in `traces` container.

```csharp
// Shared/Models/Trace.cs
namespace Shared.Models;

public class Trace : IDocument
{
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string Name { get; set; }
    public string? Description { get; set; }

    /// "gpx-upload" | "manual"
    public required string Source { get; set; }
    public required string SportType { get; set; }
    public required DateTime Date { get; set; }

    /// Ordered points of the track
    public required List<TracePoint> Points { get; set; }

    /// SHA-256 of normalized GPX content (null for manual traces)
    public string? ContentHash { get; set; }

    /// Links this trace to its upload batch (null for manual traces)
    public string? ImportBatchId { get; set; }

    /// Precomputed bounding box [minLng, minLat, maxLng, maxLat]
    public double[]? BoundingBox { get; set; }

    /// Cached metrics (null until first computation)
    public TraceMetrics? Metrics { get; set; }
}
```

**Design notes**:
- Points are embedded. A 50 km trace at 1pt/sec ≈ 18,000 points × ~40 bytes ≈ 720 KB, well under the 2 MB Cosmos limit.
- BoundingBox enables spatial filtering at query time without decoding all points.
- Metrics are cached in-document to avoid a second read. Invalidated if points change (future edit feature).

### TraceMetrics

Embedded in the Trace document, not a separate container.

```csharp
// Shared/Models/TraceMetrics.cs
namespace Shared.Models;

public class TraceMetrics
{
    /// Total distance in metres
    public double TotalDistance { get; set; }

    /// Cumulative elevation gain in metres
    public double CumulativeAscent { get; set; }

    /// Cumulative elevation loss in metres (positive number)
    public double CumulativeDescent { get; set; }

    /// Highest elevation in metres
    public double? ElevationHigh { get; set; }

    /// Lowest elevation in metres
    public double? ElevationLow { get; set; }

    /// Sampled elevation profile: (distanceAlongRoute, elevation)
    public List<double[]>? ElevationProfile { get; set; }

    /// Grade distribution: buckets from <-20% to >20% in 5% steps
    public Dictionary<string, double>? GradeDistribution { get; set; }

    /// Total elapsed time (null if no timestamps)
    public double? ElapsedTimeSeconds { get; set; }

    /// Moving time — elapsed minus stationary time (null if no timestamps)
    public double? MovingTimeSeconds { get; set; }

    /// Average moving pace in min/km (null if no timestamps)
    public double? AverageMovingPace { get; set; }

    /// Distance-based splits at configured interval
    public List<TraceSplit>? Splits { get; set; }
}
```

### TraceSplit

```csharp
// Shared/Models/TraceSplit.cs (nested or separate file)
namespace Shared.Models;

public class TraceSplit
{
    /// Split number (1-based)
    public int Number { get; set; }

    /// Distance of this split in metres
    public double Distance { get; set; }

    /// Elapsed time for this split in seconds
    public double? ElapsedTimeSeconds { get; set; }

    /// Elevation gain in this split
    public double? Ascent { get; set; }

    /// Elevation loss in this split
    public double? Descent { get; set; }
}
```

### ImportBatch

Cosmos document in `traces` container (same partition key — userId).

```csharp
// Shared/Models/ImportBatch.cs
namespace Shared.Models;

public class ImportBatch : IDocument
{
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required DateTime UploadTimestamp { get; set; }
    public required string SourceFilename { get; set; }
    public int TraceCount { get; set; }
    public int WaypointCount { get; set; }

    /// Discriminator for querying only batches in the traces container
    public string Type { get; set; } = "importBatch";
}
```

**Design notes**:
- Stored in the `traces` container to avoid a new container. Distinguished by `Type` discriminator.
- Queried only when viewing upload history — low frequency, acceptable cross-type scan within partition.

### Plan

Cosmos document in `plans` container.

```csharp
// Shared/Models/Plan.cs
namespace Shared.Models;

public class Plan : IDocument
{
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string Name { get; set; }
    public string? Description { get; set; }
    public required DateTime CreatedDate { get; set; }
    public DateTime? UpdatedDate { get; set; }

    /// Planned route as an ordered list of points (may come from a GPX or manual drawing)
    public List<TracePoint>? RoutePlan { get; set; }

    /// Named waypoints along the route
    public List<PlanWaypoint> Waypoints { get; set; } = [];

    /// References to past trace IDs for effort comparison
    public List<string> LinkedTraceIds { get; set; } = [];

    /// Cached metrics for the planned route (null until computed)
    public TraceMetrics? RouteMetrics { get; set; }
}
```

### PlanWaypoint

Embedded in Plan document.

```csharp
// Shared/Models/PlanWaypoint.cs
namespace Shared.Models;

public class PlanWaypoint
{
    public required string Name { get; set; }

    /// "water" | "camp" | "summit" | "resupply" | "custom"
    public required string Type { get; set; }

    public required double Lat { get; set; }
    public required double Lng { get; set; }
    public double? Elevation { get; set; }
    public string? Description { get; set; }

    /// Distance along the planned route in metres (computed)
    public double? DistanceAlongRoute { get; set; }
}
```

---

## Entity Relationships

```
User (1) ──── (*) Trace
  │                 │
  │                 ├── (1) TraceMetrics  [embedded]
  │                 ├── (0..1) ImportBatch [co-located in container]
  │                 └── (*) TracePoint    [embedded array]
  │
  └──── (*) Plan
              ├── (*) PlanWaypoint        [embedded array]
              ├── (*) TracePoint          [embedded as RoutePlan]
              ├── (0..1) TraceMetrics     [embedded as RouteMetrics]
              └── (*) Trace               [reference via LinkedTraceIds]
```

---

## Strava Activity Bridge

Existing `Activity` documents in the `activities` container are NOT migrated. The trace library query endpoint projects Activity fields into the same response shape:

| Trace field | Activity source |
|---|---|
| Id | Id |
| UserId | UserId |
| Name | Name |
| Source | `"strava"` (constant) |
| SportType | SportType |
| Date | StartDateLocal |
| TotalDistance | Distance |
| CumulativeAscent | TotalElevationGain |
| ElevationHigh | ElevHigh |
| ElevationLow | ElevLow |
| MovingTime | MovingTime |
| SummaryPolyline | SummaryPolyline (for map rendering) |

This projection happens at the API layer. Activities support a subset of metrics (no elevation profile, no grade distribution, no splits — those require raw points which Strava's summary polyline doesn't have).

---

## Validation Rules

| Entity | Rule |
|---|---|
| Trace.Name | Required, 1–200 chars |
| Trace.Points | ≥2 points required |
| Trace.SportType | Non-empty string |
| TracePoint.Lat | -90 to 90 |
| TracePoint.Lng | -180 to 180 |
| TracePoint.Elevation | -500 to 9000 (if present) |
| Plan.Name | Required, 1–200 chars |
| PlanWaypoint.Name | Required, 1–100 chars |
| PlanWaypoint.Type | Must be one of: water, camp, summit, resupply, custom |
| ImportBatch.SourceFilename | Required, ≤500 chars |
| File upload size | ≤50 MB |

---

## State Transitions

### Trace lifecycle

```
Upload → Parsed → Stored (metrics: null)
                     │
              GET /metrics
                     │
                 Stored (metrics: cached)
```

No delete-and-recreate cycle. Metrics are computed on first `GET` and cached.

### Plan lifecycle

```
Created → Updated (PATCH) → Deleted (soft-delete not needed, hard delete)
```

Plans have no async processing. All mutations are synchronous API calls.
