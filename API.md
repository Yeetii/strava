# API Reference

Base URL: `https://peakshunters.erikmagnusson.com/api`

## Authentication

Authenticated endpoints require a `session` cookie obtained from `POST /{authCode}/login`.
Requests without a valid session return `401 Unauthorized`.

---

## Auth

### `POST /{authCode}/login`

Exchange a Strava OAuth authorization code for a session.

**Path params**
| Param | Type | Description |
|---|---|---|
| `authCode` | string | OAuth code from Strava callback |

**Response** `200 OK`
Sets a `session` cookie (HttpOnly, SameSite=None, Secure, MaxAge=30 days). No response body.

**Errors**
- `400` — token exchange failed

---

## User Sync

### `GET /userSync`

🔒 Authenticated

Returns the authenticated user's synced Trailscope snapshot.

**Response** `200 OK` `application/json`
```ts
{
  settings: Array<{
    key: string
    updatedAt: number
    deleted: boolean
    value: unknown | null
  }>
  files: Array<{
    key: string
    updatedAt: number
    deleted: boolean
    value: unknown | null
  }>
}
```

Conflict rule: newest `updatedAt` wins.

### `POST /userSync`

🔒 Authenticated

Accepts local sync changes and returns the merged authoritative snapshot.

**Request body** `application/json`
```ts
{
  settings: Array<{
    key: string
    updatedAt: number
    deleted: boolean
    value: unknown | null
  }>
  files: Array<{
    key: string
    updatedAt: number
    deleted: boolean
    value: unknown | null
  }>
}
```

**Response** `200 OK` `application/json`
Same shape as `GET /userSync`.

For architecture, implementation notes, and local-dev gotchas, see `USER_SYNC.md` in the repo root.

---

## SignalR

### `GET /connectSignalR`

Returns connection info for the Azure SignalR hub. Pass the `session` cookie value as the `session` header to authenticate the SignalR connection.

**Response** `200 OK` `application/json`
```ts
{
  url: string
  accessToken: string
}
```

---

## Activities

### `GET /activities`

🔒 Authenticated

All activities for the current user that have a polyline, as a GeoJSON FeatureCollection.

**Response** `200 OK` `application/json` — GeoJSON `FeatureCollection`

Each Feature:
```ts
{
  type: "Feature"
  id: string                  // activity ID
  geometry: {
    type: "LineString"
    coordinates: [number, number][]   // [lon, lat]
  }
  properties: {
    id: string
    userId: string
    name: string
    description: string | null
    distance: number | null         // metres
    movingTime: number | null       // seconds
    elapsedTime: number | null      // seconds
    calories: number | null
    totalElevationGain: number | null  // metres
    elevHigh: number | null
    elevLow: number | null
    sportType: string               // e.g. "Run", "TrailRun", "BackcountrySki"
    startDate: string               // ISO 8601
    startDateLocal: string          // ISO 8601
    timezone: string | null
    startLatLng: [number, number] | null
    endLatLng: [number, number] | null
    athleteCount: number | null
    averageSpeed: number | null
    maxSpeed: number | null
  }
}
```

---

## Paths

### `GET /paths/{x}/{y}`

OSM hiking/cycling paths in a slippy tile (zoom 11), fetched from cache or Overpass on first request.

**Path params**
| Param | Type |
|---|---|
| `x` | int |
| `y` | int |

**Response** `200 OK` — GeoJSON `FeatureCollection` of path LineStrings with OSM tags as properties.

---

## Races

### `GET /races/{x}/{y}`

Trail running race routes indexed by slippy tile.

**Path params**
| Param | Type |
|---|---|
| `x` | int |
| `y` | int |

**Query params**
| Param | Type | Default |
|---|---|---|
| `zoom` | int | `8` |

**Response** `200 OK` — GeoJSON `FeatureCollection` of race LineStrings.

---

### `GET /activities/{x}/{y}`

Paths in a slippy tile (zoom 11), same data source as `/paths/{x}/{y}`.

**Path params**
| Param | Type |
|---|---|
| `x` | int |
| `y` | int |

**Response** `200 OK` — GeoJSON `FeatureCollection`

---

### `GET /visitedPaths`

🔒 Authenticated

All paths the current user has visited.

**Response** `200 OK` `application/json`
```ts
Array<{
  pathId: string          // OSM way ID — join against /paths/{x}/{y} features
  name: string | null     // OSM name tag, if present
  type: string | null     // OSM highway tag, e.g. "path", "footway", "track"
  timesVisited: number
  activityIds: string[]   // sorted ascending
  dates: string[]         // ISO 8601, sorted ascending by activity start date
}>
```

Ordered by `timesVisited` descending.

---

## Peaks

### `GET /peaks`

Peaks near a coordinate, returned as a GeoJSON FeatureCollection.

**Query params**
| Param | Type | Required | Description |
|---|---|---|---|
| `lat` | number | ✓ | Latitude (±90) |
| `lon` | number | ✓ | Longitude (±180) |
| `radius` | number | | Radius in metres, clamped to [40000, 100000] |

**Response** `200 OK` — GeoJSON `FeatureCollection` of peak Points with OSM tags as properties.

**Errors**
- `400` — invalid lat/lon

---

### `GET /peaks/{x}/{y}`

OSM peaks in a slippy tile.

**Path params**
| Param | Type |
|---|---|
| `x` | int |
| `y` | int |

**Query params**
| Param | Type | Default |
|---|---|---|
| `zoom` | int | `11` |

**Response** `200 OK` — GeoJSON `FeatureCollection` of peak Points.

---

### `GET /summitedPeaks`

🔒 Authenticated

All peaks the current user has summited, as a GeoJSON FeatureCollection.

**Response** `200 OK` — GeoJSON `FeatureCollection`

Each Feature is a peak Point with OSM properties plus:
```ts
properties: {
  // ...OSM tags (name, ele, etc.)
  summited: true
  summitsCount: number
  firstAscent: string | undefined   // ISO 8601
  lastAscent: string | undefined    // ISO 8601
}
```

---

### `GET /peaksGroups`

All configured peak groups (e.g. mountain ranges, lists).

**Response** `200 OK` `application/json`
```ts
Array<{
  id: string          // UUID
  parentId: string | null
  name: string
  amountOfPeaks: number
  peakIds: string[]
  boundrary: GeoJSON.Feature | null   // note: intentional spelling in API
}>
```

---

### `GET /gridIndices`

Slippy tile coordinates containing the given peaks.

**Query params**
| Param | Type | Required | Description |
|---|---|---|---|
| `peakIds` | string | ✓ | Comma-separated peak IDs |

**Response** `200 OK` `application/json`
```ts
string[]   // each entry is "x,y"
```

**Errors**
- `400` — missing `peakIds`

---

## Protected Areas

### `GET /protectedAreas/{x}/{y}`

National parks, nature reserves, and protected areas in a slippy tile.

**Path params**
| Param | Type |
|---|---|
| `x` | int |
| `y` | int |

**Query params**
| Param | Type | Default |
|---|---|---|
| `zoom` | int | `8` |

**Response** `200 OK` — GeoJSON `FeatureCollection` of area Polygons/MultiPolygons with OSM tags as properties.

---

### `GET /visitedProtectedAreas`

🔒 Authenticated

All protected areas the current user has visited.

**Response** `200 OK` `application/json`
```ts
Array<{
  areaId: string            // OSM ID, e.g. "relation:2202162" — join against /protectedAreas/{x}/{y}
  name: string
  areaType: string          // "national_park" | "nature_reserve" | "protected_area"
  timesVisited: number
  activityIds: string[]     // sorted ascending
  dates: string[]           // ISO 8601, sorted ascending by activity start date
  wikidata: string | null   // Wikidata QID, e.g. "Q123456"
  wikimediaCommons: string | null
}>
```

Ordered by `timesVisited` descending, then `name` ascending.

---

## Stats

### `GET /summitsStats`

🔒 Authenticated

Aggregated summit statistics for the current user.

**Response** `200 OK` `application/json`
```ts
{
  totalPeaksClimbed: number
  totalPeaksClimbedCategorized: number[]  // 9 entries; index = floor(elevationMetres / 1000)
  mostVisitedPeaks: Array<{
    id: string
    name: string
    count: number
    elevation: number | null
  }>                                      // top 5, ordered by count descending
}
```

---

### `GET /{userId}/skiDays`

Ski day statistics. Requires function API key (`?code=...`).

**Path params**
| Param | Type |
|---|---|
| `userId` | string |

**Query params**
| Param | Type | Description |
|---|---|---|
| `before` | string (ISO 8601) | Filter activities before this date |
| `after` | string (ISO 8601) | Filter activities after this date |

**Response** `200 OK` `application/json`
```ts
{
  alpineSkiDays: number
  backcountrySkiDays: number
  nordicSkiDays: number
  snowboardDays: number
  backcountrySkiElevationGain: number
  alpineSkiVerticalDrop: number
  totalSkiDays: number
}
```

---

### `GET /{userId}/streaks`

Activity streak statistics. Requires function API key (`?code=...`).

**Path params**
| Param | Type |
|---|---|
| `userId` | string |

**Query params**
| Param | Type | Description |
|---|---|---|
| `before` | string (ISO 8601) | Filter activities before this date |
| `after` | string (ISO 8601) | Filter activities after this date |

**Response** `200 OK` `application/json`
```ts
{
  currentActivityStreak: number
  longestActivityStreak: number
  currentRunningStreak: number
  longestRunningStreak: number
  // date fields present but not yet computed
}
```

---

## Routing

### `POST /route`

Proxy to GraphHopper routing API.

**Request body** `application/json` — GraphHopper route request object

**Response** — GraphHopper response, status code and body passed through verbatim.

---

## Job Queue

### `POST /queue/{jobType}`

🔒 Authenticated

Re-queue activity processing jobs for the current user.

**Path params**
| Param | Type | Values |
|---|---|---|
| `jobType` | string | `summits` \| `visitedPaths` \| `visitedAreas` |

**Query params**
| Param | Type | Description |
|---|---|---|
| `startDate` | string (ISO 8601) | Only queue activities on or after this date |
| `endDate` | string (ISO 8601) | Only queue activities on or before this date |

**Response** `200 OK` `application/json`
```ts
number   // count of jobs queued
```

**Errors**
- `400` — unknown `jobType`
