# Visited paths storage optimization
Branch: refactor/visited-paths-storage
Delivery strategy: ask-on-risk
Test runner: dotnet test

## Specs

S1: "A few tables are quite large in storage size, is there something simple that could be done to optimize this? Especially visited paths seems like it would be optimizable given the simple data structure"
S2: "the osm highway id fit in a BIGINT, does that save anything?"
S3: "name and type are not used for visited paths in frontend so they can be removed from the api endpoint too"
S4: "an index with user id is probably needed though as all queries will be by user?"
S5: "go for it, wipe the existing table in the migration. i will rebuild the data"

Acceptance criteria (agreed design, see Log):
- Replace `activity_visited_paths` with `activity_visited_tiles`: `PRIMARY KEY (activity_id, tile_x, tile_y)`, `user_id TEXT NOT NULL`, `highway_ids BIGINT[] NOT NULL`, composite index `(user_id, tile_x, tile_y)`. No `name`, `type`, `created_at`.
- Migration `03-*.sql` drops the old table; data is rebuilt by the user.
- `name`/`type` removed from `VisitedPathDto`, `VisitedPathInput`, `VisitedPathLink` and `API.md`.
- OSM highway id parsed from `way/123` to `123`; all ids assumed to be ways (no type discriminator).
- `GetVisitedPaths` filters user + tiles in SQL and aggregates with `unnest`; no full-user in-memory scan.
- `CountByUser` preserves the displayed number via `SUM(cardinality(highway_ids))`.
- Focused and full `dotnet test` pass.

## Tasks

T1 (S1-S5): Replace activity_visited_paths with the grouped activity_visited_tiles model across DDL, migration, repository, worker, API endpoints, admin queries, docs and tests | route: inline | status: complete | commit: 61389e8

## Log

L1: 2026-11-20 - User: "A few tables are quite large in storage size, is there something simple that could be done to optimize this? 

Especially visited paths seems like it would be optimizable given the simple data structure

activity_visited_paths
141.2 MB
786,970 rows

admin_boundaries
163.8 MB

protected_areas
243.1 MB
47,887 rows

activities
351.6 MB
150,356 rows

5,808 rows"

L2: 2026-11-20 - Measured live database: 821,182 rows; index sizes 41 MB PK / 18 MB highway (0 scans) / 8 MB user (881 scans); 24,562 dead tuples (3.1%, autovacuum current). Bloat and name/type normalization ruled out as material savings.

L3: 2026-11-20 - Measured grouping: 3,165 activities with paths, 6,368 distinct (activity, tile) pairs, 285,266 distinct highways, 0 NULL tiles, 0 missing highway ids. Confirms ~129 highways per (activity, tile) and ~129x row reduction from grouping.

L4: 2026-11-20 - User decisions: store OSM highway id as BIGINT (numeric part of `way/123`); drop `name`/`type` from storage and API; keep and index `user_id` because all queries are user-scoped; wipe the existing table in the migration because data will be rebuilt.

L5: 2026-11-20 - User: "go for it, wipe the existing table in the migration. i will rebuild the data". Implementation authorized. Route: inline (one coupled behavior across files; no writer-trigger reason). Risk: none beyond the explicitly authorized table wipe and API field removal; existing behavior touched is covered by tests.

L6: 2026-11-20 - Implemented. Added 03-replace-activity-visited-paths.sql (drops old table; user rebuilds), grouped write/aggregate queries, new PostGIS integration tests for grouping and tile-filtered aggregation. Supersedes T10 of cosmos-to-postgres-migration.md.

L7: 2026-11-20 - Verification: RED observed before implementation (CS0019 on long highway id). After implementation: dotnet build 0 errors; focused VisitedPathsWorkerTests 6/6; new ActivityVisitedPathRepositoryTests 2/2 against real PostGIS; full suite 698/698 green.
