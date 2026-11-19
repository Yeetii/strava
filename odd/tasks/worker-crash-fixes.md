# Worker crash fixes (non-Overpass)
Branch: main
Delivery strategy: uncommitted fixes reported for review; no branch/PR requested
Test runner: dotnet test

## Specs

S1: "there are some crashes in the worker, we don't need to resolve the overpass ones but the others" — fix every non-Overpass crash in the pasted worker-heavy-1 log.

S2: DI crash — `System.InvalidOperationException: Unable to resolve service for type 'Stigar.Core.Services.StravaTokenService' while attempting to activate 'Stigar.Workers.StravaActivityFetcher'.` at `Stigar.Workers/Program.cs:line 31`.

S3: Ingestion crash — `Npgsql.PostgresException (0x80004005): 42804: column "properties" is of type jsonb but expression is of type text` in `Stigar.Core.Services.Postgres.Spatial.AdminBoundaryRepository.BulkUpsert` (`/src/Stigar.Core/Services/Postgres/Spatial/AdminBoundaryRepository.cs:line 185`).

S4: FK crash — `23503: insert or update on table "activity_visited_areas" violates foreign key constraint "activity_visited_areas_area_id_fkey"` in `Stigar.Core.Services.Postgres.Spatial.ActivityVisitedAreaRepository.AddRange` (`ActivityJunctionRepositories.cs:line 144`) via `Stigar.Workers.VisitedAreasWorker.ProcessJob` (`VisitedAreasWorker.cs:line 133`).

S5: FK crash — `23503: insert or update on table "activity_summits" violates foreign key constraint "activity_summits_peak_id_fkey"` in `Stigar.Core.Services.Postgres.Spatial.ActivitySummitRepository.AddRange` (`ActivityJunctionRepositories.cs:line 43`) via `Stigar.Workers.SummitsWorker.ProcessSummitJob` (`SummitsWorker.cs:line 78`).

S6: Out of scope — `Overpass mirror https://overpass-api.de/api/interpreter transient connection error ... Network is unreachable (overpass-api.de:443)`. Explicitly excluded by the user.

## Tasks

T1 (S2): Register StravaTokenService in the workers host DI graph | route: inline | status: done (BackendServices.cs:100) + BackendServiceRegistrationTests
T2 (S3): Bind admin-boundary `properties` as jsonb in AdminBoundaryRepository.AddParameters | route: inline | status: done (line 244) + BulkUpsert_StoresPropertiesAsJsonb
T3 (S5): Write summit links with the stored peaks.id format so activity_summits_peak_id_fkey is satisfied | route: inline | status: done (DomainRules.ToStoredPeakId, SummitsWorker, RunSummitsForActivity) + worker/helper tests
T4 (S4): Drop the obsolete area_id → protected_areas(id) FK on existing databases via an idempotent migration wired into deploy | route: inline | status: done (02-drop-activity-visited-areas-fk.sql, migrate.sh, deploy.sh, Makefile)

## Log

L1: User: "there are some crashes in the worker, we don't need to resolve the overpass ones but the others" (full worker-heavy-1 log pasted).

L2: Diagnosis. (a) API/Program.cs:115 registers `AddScoped<StravaTokenService>()`; Stigar.Workers/BackendServices.cs registers `AuthenticationApi` (line 95) but omits StravaTokenService, so StravaActivityFetcher cannot be activated. (b) ProtectedAreaRepository.AddParameters:188 uses `AddJsonb("properties", ...)`; AdminBoundaryRepository.AddParameters:244 uses `AddValue`, producing 42804 against the jsonb column. (c) Ingestion stores peaks with `StoredFeature.Id = "peak:<id>"` (StoredFeature.cs:162, PeakRepository.AddParameters:196) but SummitsWorker.cs:70 / RunSummitsForActivity.cs:78 write the normalized `<id>`, so peak_id never matches peaks.id. (d) `git log -p -- deploy/init-db/01-init.sql` shows `area_id TEXT NOT NULL REFERENCES protected_areas(id)` was removed from fresh-install DDL; existing volumes keep `activity_visited_areas_area_id_fkey`, which is invalid now that regions and `country:<ISO2>` ids are also stored.

L3: Route: inline (phase-local fixes, disjoint edit surfaces). Risk: item 1 (schema migration in T4) and item 3 (link id format is consumed by API read endpoints) → independent verification after the focused checks.

L4: Test-first evidence. RED: `BulkUpsert_StoresPropertiesAsJsonb` failed against the Testcontainers PostGIS database with 42804 before the fix; the summit worker test would not compile without ToStoredPeakId; the DI test failed because AuthenticationApi needed StravaClientSecret in test config. GREEN after fixes: Workers.Tests focused 7/7, Core.Tests AdminBoundary 5/5. Full `dotnet test strava.sln`: 666 passed / 0 failed (Core 269, Workers 365, Pmtiles 32).

L5: Migration proof: against a throwaway PostgreSQL 16 container with the legacy `activity_visited_areas_area_id_fkey`, the SQL removed the constraint (BEFORE present, AFTER empty) and a second run was idempotent (NOTICE, no error). `bash -n` passes on migrate.sh and deploy.sh.

L6: Native RDD assessment (after concurrent actor staged the worktree) returned risk=high, review_due=true, reasons executable_mode/process_boundary/shell_source on the deploy scripts. An independent read-only verifier was launched before entering the review lifecycle. Note: unrelated concurrent edits to Stigar.Api/Program.cs, Stigar.Workers.Pmtiles/Program.cs and deploy/docker-compose.yml are NOT part of this task and were left untouched.

L7: Independent verifier (gentle-ai-verify) returned no blockers. It raised two advisories, both addressed: migrate.sh now uses the credentials Compose injects into the container instead of shell-side .env defaults, and BackendServiceRegistrationTests now activates StravaActivityFetcher itself. Full suite re-run green: 666 passed / 0 failed.

L8: User consented to native review ("Review this change"). review.start created lineage review-7ee432f3b8c6fcaf (high, lenses review-risk/resilience/reliability, workspace projection). The six Task launches returned `opencode_review_transport_relay_refused (reason: provider_failed)` synchronously, but the three lens reviewers actually completed asynchronously with valid raw JSON results; each was admitted via the bound `review.capture-result` with its exact collect arguments, preflighted first. Native reduction returned `approved` with six informational advisories and no correction. `review.acknowledge-approved` burned authority (`action: acknowledged`, authority burned). Review outcome is informational; no commit/push/PR was requested or performed.

L9: Informational advisories (non-blocking, separate later work): migrate.sh has no post-loop assertion after the 30-attempt pg_isready wait; deploy.sh relies on its existing `set -euo pipefail` to propagate a migration failure (present at line 2); no automated test for the migration discovery/wiring (manual container proof in L5); jsonb regression test is a SkippableFact and is skipped without Docker; no API read-endpoint test for prefixed summit peak ids; the unrelated FormOptions change has no test.
