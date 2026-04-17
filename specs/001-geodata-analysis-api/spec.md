# Feature Specification: Geodata Analysis API

**Feature Branch**: `001-geodata-analysis-api`  
**Created**: 2026-04-17  
**Status**: Draft  
**Input**: User description: "Build an API for an interactive web app that allows deep analysis of GPS trace data, adventure/race/trip planning, and a GPX library with past efforts and geometric metrics. Keep the backend general enough for multiple consumers (Trailscope, personal dashboard, future projects)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Import GPS Traces from Any Source (Priority: P1)

A user uploads one or more GPX files to build a personal trace library. The system parses each file, extracts the track geometry and timestamps, and stores the trace alongside any metadata embedded in the GPX (name, description, waypoints). Traces imported from Strava continue to work as before, but the upload path treats Strava as just one of many possible sources.

**Why this priority**: Without source-agnostic ingestion, every downstream feature (analysis, planning, comparison) has no data to operate on. This is the foundation.

**Independent Test**: Upload a GPX file exported from a Garmin device. Retrieve the stored trace and verify that coordinates, timestamps, and embedded metadata are preserved accurately.

**Acceptance Scenarios**:

1. **Given** a user is authenticated, **When** they upload a valid GPX file containing one track with timestamped points, **Then** the system stores the trace and returns the trace identifier and a summary (distance, duration, bounding box).
2. **Given** a GPX file contains multiple tracks or track segments, **When** imported, **Then** each track segment is stored as a separate trace linked to the same import batch.
3. **Given** a GPX file contains waypoints alongside tracks, **When** imported, **Then** waypoints are stored and associated with the import but kept as distinct entities from the trace geometry.
4. **Given** a user already has traces from Strava, **When** they view their trace library, **Then** Strava-sourced and GPX-uploaded traces appear together in a unified list.
5. **Given** a GPX file is malformed or contains no track data, **When** uploaded, **Then** the system rejects it with a clear error message and does not create partial records.

---

### User Story 2 — Geometric Metric Computation (Priority: P2)

After a trace is stored, the user requests computed metrics for it. The system calculates an elevation profile, cumulative ascent and descent, grade (steepness) per segment, distance splits (e.g. per-kilometre pace or per-100m-elevation effort), and moving time versus total time when timestamps are available.

**Why this priority**: Geometric analysis is the core differentiator — the reason users come to this tool rather than just storing files. It unlocks the "deep analysis" goal.

**Independent Test**: Import a known GPX trace with verified elevation data. Request metrics and compare computed cumulative ascent, average grade, and split paces against hand-calculated expected values.

**Acceptance Scenarios**:

1. **Given** a stored trace with elevation data, **When** the user requests metrics, **Then** the system returns cumulative ascent, cumulative descent, maximum elevation, minimum elevation, and an elevation profile (ordered series of distance-elevation points).
2. **Given** a stored trace with timestamps, **When** metrics are requested, **Then** the system returns total moving time, total elapsed time, average moving pace, and distance-based splits (default 1 km intervals).
3. **Given** a trace with elevation data, **When** the user requests grade analysis, **Then** the system returns a distribution of grades (percentage of distance at 0–5%, 5–10%, 10–15%, 15–20%, 20%+) and the steepest sustained segment (at least 200 m long).
4. **Given** a trace without elevation data, **When** metrics are requested, **Then** the system returns distance and time metrics but omits elevation-dependent fields rather than failing.
5. **Given** a trace without timestamps (e.g. a planned route), **When** metrics are requested, **Then** the system returns geometry-only metrics (distance, elevation profile, grade) and omits time-based fields.

---

### User Story 3 — Browse and Search the Trace Library (Priority: P3)

A user browses their full collection of traces — regardless of source — filtering by date range, sport type, distance range, elevation range, geographic area (map viewport), or free-text name search. Each trace in the listing includes summary metrics computed in US2 so the user can quickly scan without opening each trace individually.

**Why this priority**: A library is only useful if you can navigate it efficiently. This story makes the imported data explorable and turns the system from a data dump into a usable tool.

**Independent Test**: Import 20+ traces spanning different dates, sport types, and regions. Verify that filtering by each criterion independently returns the correct subset, and that combined filters intersect correctly.

**Acceptance Scenarios**:

1. **Given** a user has traces in their library, **When** they request the list without filters, **Then** traces are returned in reverse-chronological order with summary metrics (distance, duration, ascent, sport type, date).
2. **Given** a user specifies a map bounding box, **When** they request traces, **Then** only traces whose geometry intersects the bounding box are returned.
3. **Given** a user filters by sport type "Trail Run" and minimum ascent of 500 m, **When** they request traces, **Then** only trail runs with at least 500 m cumulative ascent are returned.
4. **Given** a user searches by name "Kebnekaise", **When** results load, **Then** traces whose name or description contains the search term appear.
5. **Given** the user requests a page of results, **When** there are more traces than the page size, **Then** results are paginated and the response includes a continuation token.

---

### User Story 4 — Route-Based Effort Comparison (Priority: P4)

A user selects a trace and asks "show me all my previous efforts on this route." The system identifies past traces that follow a substantially similar path (allowing for minor GPS drift) and presents them side-by-side with comparative metrics: elapsed time, moving time, pace splits, and elevation gain.

**Why this priority**: Effort comparison is the main analytical payoff for returning users and makes the system valuable over time as the library grows.

**Independent Test**: Import three traces that follow the same trail (with natural GPS variation) and one that follows a different trail. Request comparison for one of the three. Verify the two similar traces are found and the dissimilar trace is excluded.

**Acceptance Scenarios**:

1. **Given** a user selects a trace, **When** they request route matches, **Then** the system returns traces whose geometry overlaps the selected trace by at least 80% of distance.
2. **Given** matching traces are found, **When** results are displayed, **Then** each match includes side-by-side time, pace, and elevation metrics alongside the reference trace.
3. **Given** a user has no other traces on the same route, **When** they request matches, **Then** the system returns an empty list with a clear message.
4. **Given** two traces follow the same route in opposite directions, **When** matched, **Then** both directions are recognized as the same route.

---

### User Story 5 — Adventure Planning Workspace (Priority: P5)

A user creates a "plan" — a named container that groups planned routes, waypoints, notes, and references to past traces or races. Plans can be used to prepare for a race, a multi-day trek, or a day trip. The user can draw or import a planned route, attach waypoints (water sources, camps, resupply points), and see projected metrics based on the route geometry.

**Why this priority**: Planning is a distinct use case from analysis of past data, but it reuses the same geometric engine and trace storage. It completes the "all-in-one" vision.

**Independent Test**: Create a plan, add a planned route via GPX import, add three waypoints. Retrieve the plan and verify all components and projected metrics are present.

**Acceptance Scenarios**:

1. **Given** an authenticated user, **When** they create a plan with a name and optional description, **Then** the plan is stored and returned with its identifier.
2. **Given** a plan exists, **When** the user imports a GPX file as the planned route, **Then** the route geometry is stored, and projected distance, ascent, descent, and grade distribution are computed automatically.
3. **Given** a plan has a route, **When** the user adds a waypoint with a name, type (water/camp/summit/resupply/custom), and coordinates, **Then** the waypoint is stored, its distance-along-route is calculated relative to the planned route, and it appears in route order.
4. **Given** a plan exists, **When** the user links a past trace to the plan, **Then** the trace reference is stored and its metrics are available for comparison against the planned route.
5. **Given** a plan with a planned route, **When** the user requests an elevation profile, **Then** the profile includes waypoint markers at their positions along the route.

---

### User Story 6 — Multi-Consumer API Access (Priority: P6)

A second application (e.g. a personal dashboard) queries the same API to read trace summaries, computed metrics, and aggregate statistics without being tied to a particular frontend's conventions. The API uses stable, documented endpoints that return structured data suitable for any client.

**Why this priority**: This is a cross-cutting quality attribute more than a user-facing feature, but it shapes the API design from day one. Deferring it leads to Trailscope-specific endpoints that are costly to generalise later.

**Independent Test**: Using only the public API (no frontend), fetch a user's trace list, retrieve detailed metrics for one trace, and request aggregate statistics. Verify that responses are self-describing and require no frontend-specific knowledge.

**Acceptance Scenarios**:

1. **Given** a valid authentication token, **When** any client sends a request to the trace list endpoint, **Then** it receives paginated, filterable results in a consistent schema.
2. **Given** a trace identifier, **When** any client requests metrics, **Then** the response contains all computed fields in a documented, versioned structure.
3. **Given** a user with traces, **When** any client requests aggregate statistics (total distance, total ascent, trace count by sport type, activity by month), **Then** the response returns the aggregated data.

---

### Edge Cases

- What happens when a GPX file is larger than 50 MB? The system rejects it with a size-limit error before parsing.
- What happens when a trace has fewer than two points? The system stores it but marks metrics as unavailable.
- How does the system handle duplicate imports (same GPX uploaded twice)? Duplicates are detected by content hash and the user is warned, but a second import is allowed (they may represent distinct efforts on the same route file).
- What happens when elevation data contains obvious GPS noise spikes (e.g. ±500 m in a single second)? The elevation profile applies basic outlier smoothing before metric computation.
- What happens when a user deletes a trace that is referenced by a plan? The reference is preserved as a tombstone (name + metrics snapshot retained, geometry removed).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept GPX file uploads and extract tracks, track segments, and waypoints into stored entities.
- **FR-002**: System MUST continue to ingest activity data from the existing Strava pipeline without modification to the webhook or fetch flow.
- **FR-003**: System MUST compute the following geometric metrics for any trace with sufficient data: total distance, cumulative ascent, cumulative descent, elevation high/low, elevation profile, and grade distribution.
- **FR-004**: System MUST compute the following time-based metrics when timestamps are present: total elapsed time, moving time, average moving pace, and distance-based splits at configurable intervals.
- **FR-005**: System MUST provide a filterable, paginated trace library endpoint supporting filters for: date range, sport type, distance range, elevation range, bounding box, and name search.
- **FR-006**: System MUST identify traces that follow substantially the same route as a given reference trace (≥80% geometric overlap) for effort comparison.
- **FR-007**: System MUST allow creation, retrieval, update, and deletion of adventure plans containing planned routes, waypoints, notes, and references to past traces.
- **FR-008**: System MUST compute projected metrics (distance, ascent, descent, grade distribution) for planned routes and calculate distance-along-route for each attached waypoint.
- **FR-009**: System MUST expose aggregate statistics (total distance, total ascent, trace count by sport type, monthly activity breakdown) via a dedicated endpoint.
- **FR-010**: System MUST apply basic elevation smoothing to filter GPS noise spikes before computing elevation-dependent metrics.
- **FR-011**: System MUST return consistent, structured error responses across all endpoints so clients can handle failures uniformly.
- **FR-012**: System MUST authenticate requests using the existing session-based mechanism and scope all trace, plan, and metric data to the authenticated user.
- **FR-013**: System MUST detect potentially duplicate GPX imports (by content hash) and warn the user while still allowing the import.

### Key Entities

- **Trace**: A recorded or planned GPS track. Key attributes: name, source (strava / gpx-upload / manual), sport type, date, geometry (ordered series of lat/lng/elevation/time points), content hash, import batch identifier.
- **TraceMetrics**: Computed analysis output for a trace. Key attributes: total distance, cumulative ascent, cumulative descent, elevation high/low, elevation profile series, grade distribution, moving time, elapsed time, splits.
- **Waypoint**: A named point of interest. Key attributes: name, type (water / camp / summit / resupply / custom), coordinates, optional description, optional distance-along-route.
- **Plan**: An adventure planning container. Key attributes: name, description, planned route (Trace reference), waypoint collection, linked past trace references, creation date.
- **ImportBatch**: Groups traces and waypoints from a single GPX upload. Key attributes: upload timestamp, source filename, number of traces extracted.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can upload a GPX file and see the stored trace with summary metrics within 5 seconds for files up to 10 MB.
- **SC-002**: Elevation profile and grade analysis for a 50 km trace complete within 2 seconds.
- **SC-003**: Trace library filtering by any single criterion returns results within 1 second for libraries of up to 5,000 traces.
- **SC-004**: Route matching identifies similar efforts with at least 90% precision (true matches / returned matches) on a test set of 10 known route pairs.
- **SC-005**: A second application (dashboard) can consume trace data using only the public API documentation, without any coordination with the frontend team.
- **SC-006**: All endpoints return structured error responses that a generic client can parse without endpoint-specific error handling.
- **SC-007**: The system handles GPX files from at least 5 different devices/applications (Garmin, Suunto, Coros, Komoot, Strava export) without format-specific workarounds.

## Assumptions

- The existing Strava OAuth authentication and session mechanism will be reused for user identity; no new auth system is needed for v1.
- GPS trace data is stored per-user; there is no multi-user sharing or public trace visibility in this version.
- Elevation data is taken from the GPX file as-is; server-side elevation lookup (DEM correction) is out of scope for v1.
- The frontend (Trailscope) consumes this API but the spec does not prescribe any frontend changes.
- Route matching uses geometric similarity (shape comparison); semantic matching (recognising "the same trail" by name) is out of scope.
- Planned routes are imported via GPX or drawn on a map in the frontend; server-side route generation (turn-by-turn) is not in scope — the existing GraphHopper proxy covers that need.
- The personal dashboard project is the primary second consumer; the API does not need a formal public API programme or developer portal for v1.
