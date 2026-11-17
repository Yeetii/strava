# Self-hosted race PMTiles build
Branch: feature/self-hosted-race-tiles. Delivery strategy: exception-ok (concurrent session shares this worktree; keep changes uncommitted, user owns delivery). Test runner: `dotnet test strava.sln` with focused project runs per task.

## Specs

**S1 — Shared organizer-store abstraction (Shared/)**
"Add IOrganizerStore covering the public surface used by API, Backend and PmtilesJob: GetByIdAsync, ResolveOrganizerKeyAsync, WriteAsync, SetRedirectAsync, WriteDiscoveryAsync, WriteDiscoveriesAsync, WriteScraperOutputAsync, PatchScraperPropertiesAsync, PatchLastScrapedAsync, GetIdsDueForAutomaticScrapeAsync, GetCountsAsync, UploadRouteGpxAsync, WriteAssembledRacesAsync, StreamAllAsync, StreamAllWithSourceAsync, StreamIdentitiesAsync, StreamMetadataWithoutGeometriesAsync, StreamRedirectsAsync, StreamAssembledRacesAsync, GetAssembledRaceAsync, GetLastOrganizerReadMetricsSnapshot."
"Keep static DeriveOrganizerKey as a neutral helper (OrganizerUrlRules) so it is not part of the interface."
"Implement FileSystemOrganizerStore with the layout below. Use atomic temp-file plus rename writes; there is no Blob ETag concurrency on disk."
"Make BlobOrganizerStore implement the same interface and add a config-selected DI registration (OrganizerStore:Provider = Blob or FileSystem, plus RaceDataDir)."
"Add new configuration keys in Shared/Constants/AppConfig.cs."
Directory layout assumption: "{organizerKey}/organizer.json, {organizerKey}/routes/*.gpx, transparency under {organizerKey}/races/".

**S2 — Switch non-queue wiring (API/, Backend/)**
"Resolve IOrganizerStore in API/Program.cs and inject it into the 11 admin endpoints that currently take BlobOrganizerStore: GetOrganizerStats, GetOrganizerRedirectCandidates, PostManualGpx, PostRaceEvent, GetAssembledRaceById, PostManualMistralScrape, GetAdminStatus, GetRaceOrganizers, GetRaceOrganizerById, PostOrganizerRedirect, GetAssembledRaces."
"Do the same in Backend/BackendServices.cs, RaceDiscoveryService.cs, QueueAllScrapeJobs.cs, ScrapeRaceWorker.cs, ScrapeRaceMistralWorker.cs. Leave all Service Bus calls structurally intact — the other session removes them."
"Coordination point: the other session must keep injecting IOrganizerStore when it strips the queues."

**S3 — Redis geocoding cache (Shared/, PmtilesJob/)**
"Add RedisGeocodingCache implementing IGeocodingCache, using GeocodingCacheKeyParts and the existing TTL options."
"Add a registration extension alongside AddSharedGeocodingCache; switch PmtilesJob/Program.cs to it using RedisConnectionString."

**S4 — Race build on local disk (PmtilesJob/)**
"Replace the Blob-backed organizer stream with the filesystem-backed IOrganizerStore stream; remove BlobServiceClient and the Blob staging/copy publish in ValidateAndPublishPmtilesAsync."
"Write the validated PMTiles to the configured local output path."
"Keep --write-transparency, but write assembled races to disk through WriteAssembledRacesAsync; default off as today."
"Keep tippecanoe invocation, validation, progress logging and temp cleanup unchanged."
"Add the weekly Hangfire recurring job (scheduler mode) and a one-shot CLI path sharing the same build service."
"PMTiles output path /data/races/tiles/trails.pmtiles, served at /tiles/trails.pmtiles."
"Cache headers preserve the old blob behavior (no-cache) — public access without the cacheable variant."

**S5 — One-off seed export (Tools/)**
"New command that reads all organizer documents and route GPX files from Azure Blob and writes the local directory layout. Run manually once during rollout; not in the image."

**S6 — Container, Compose and CI**
"Replace the stale PmtilesJob/Dockerfile (it uses .NET 8 images for a net10.0 project and omits required source files) with a correct image: .NET 10 SDK build stage, tippecanoe compiled in, .NET 10 runtime final stage."
"Add a pmtiles service to deploy/docker-compose.yml: env_file .env, REDIS_CONNECTION_STRING, a shared volume with api, worker healthcheck, resource limits."
"Update .github/workflows/selfhosted-deploy.yml to build/push the pmtiles image and deploy/scripts/deploy.sh to deploy it."
"Retire .github/workflows/main_pmtilesjob-container.yml (Azure Container App deploy) after cutover."
"Document new variables in deploy/.env.example, deploy/README.md, deploy/DEPLOY.md and PmtilesJob/README.md."
"The image contains only the race build. Admin-area, filter and Cosmos/export commands are excluded."

**S7 — Public tile endpoint (API/)**
"Add GET /tiles/trails.pmtiles using Results.File with enableRangeProcessing true and content type application/octet-stream, reading the shared volume read-only. Public, no auth; CORS policy already covers the configured origins."

**S8 — Tests and verification**
"Unit tests: FileSystemOrganizerStore (read/write/redirects/routes/metadata), RedisGeocodingCache (get/set/expiry), build service (local output plus optional transparency), CLI parsing for new options, tile endpoint range handling."
"Keep existing PmtilesJob.Tests green; update mocks to the new store interface."
"Functional check: seed export, then weekly/one-shot build, then IsValidPmtilesFile, then API Range request, then verify bytes served."
"Differential check: compare feature count and output validity of the old Blob-based build against the new disk-based build on the same seed data."
"Confirm the pmtiles container needs no Azure connection strings."

**S9 — Scope boundary / coordination**
"Keep all Service Bus code structurally intact; only change the organizer store injection. Do not touch queue removal."
"Local disk, no Azure for this pipeline. Cosmos is not involved in the race build and stays untouched."

## Tasks

- T1 — S1,S9 — Shared store abstraction, filesystem store, blob implementation, config keys, DI selection — delegated writer A — done; commits 1e4a0da (abstraction/config), 08db838 (filesystem store)
- T2 — S3 — Redis geocoding cache + registration extension + tests — delegated writer B — done; commit 2e2086e
- T3 — S2 — API/Backend IOrganizerStore injection (queue code untouched) — delegated writer C — done; commits 1130aff (API + tiles), 3b3e93f (Backend)
- T4 — S4,S8 — PmtilesJob local-disk build, CLI, scheduler, tests — delegated writer D — done; commit 3284756
- T5 — S5 — Tools Blob→disk seed export — delegated writer E — done; commit d2c4b74
- T6 — S6 — Dockerfile, compose, CI, deploy script, docs — delegated writer F — done; commit dab8cb4
- T7 — S7 — API public tile endpoint with range processing — inline parent — done; commit 1130aff
- T8 — S1,S2,S3,S4,S8 — Parent integration, CLI --output wiring, full build/tests and functional checks — inline parent — done; commits 1e4a0da..dab8cb4

## Log

L1 (verbatim user request):

You are implementing an already-approved plan. This session cannot see the planning conversation, so this prompt is the complete specification. Do not renegotiate the decisions; if something is genuinely blocked, report the blocker rather than changing scope.

Repository: /Users/erik/.local/share/opencode/worktree/30cd62d5972c8ce2ce90d3d5d6ae43506b854263/clever-otter on branch feature/self-hosted-race-tiles.

Important coordination: a separate session is removing the Service Bus plumbing from the same pipeline. Keep all Service Bus code structurally intact; only change the organizer store injection. Do not touch queue removal.

The full plan is also saved in OpenChamber knowledge as "Self-hosted race PMTiles build — implementation plan" (id 13beae2b-dc7a-49db-80bf-f09d6eac1ec0).

# Self-hosted race PMTiles build — implementation plan

## Goal

Move race PMTiles generation off Azure Container Apps onto the self-hosted server, with organizer data and tile output on local disk. The pipeline uses no Azure services. Service Bus removal is explicitly out of scope (handled in another session).

## Decisions made (with rationale)

1. Local disk, no Azure for this pipeline. The race build today reads organizers from Azure Blob, caches geocoding in Azure Tables, and publishes production/trails.pmtiles to Blob. All three move to the server. Cosmos is not involved in the race build and stays untouched.
2. All non-queue organizer wiring moves to a filesystem store. API admin endpoints, discovery and scrapers will use an IOrganizerStore abstraction with a FileSystemOrganizerStore implementation. Queue plumbing changes belong to the other session.
3. Weekly Hangfire schedule, registered against the Redis already in the Compose stack; cadence matches the existing weekly scraper rhythm.
4. Dedicated pmtiles container. Own image with tippecanoe compiled in, its own Hangfire worker for the weekly job, and a one-shot CLI for manual runs. Chosen over installing tippecanoe in the shared plain-host image (would bloat api and worker-heavy) and over triggering a one-shot container from worker-schedule (Docker socket or duplicate polling loop).
5. Tiles served by the existing API at a public /tiles/trails.pmtiles endpoint with HTTP range support. No new Funnel port; CORS already configured; matches the old public Blob URL.
6. Geocoding cache in the existing Redis: a new RedisGeocodingCache behind the existing IGeocodingCache interface.
7. Transparency stays optional: --write-transparency writes assembled race JSON to local disk, default off.
8. One-off export command seeds the local directory from Blob during rollout. It is a manual Tools command, not part of the pmtiles image.
9. The image contains only the race build. Admin-area, filter and Cosmos/export commands are excluded.

## Ordered work

### 1. Shared organizer-store abstraction — Shared/
- Add IOrganizerStore covering the public surface used by API, Backend and PmtilesJob: GetByIdAsync, ResolveOrganizerKeyAsync, WriteAsync, SetRedirectAsync, WriteDiscoveryAsync, WriteDiscoveriesAsync, WriteScraperOutputAsync, PatchScraperPropertiesAsync, PatchLastScrapedAsync, GetIdsDueForAutomaticScrapeAsync, GetCountsAsync, UploadRouteGpxAsync, WriteAssembledRacesAsync, StreamAllAsync, StreamAllWithSourceAsync, StreamIdentitiesAsync, StreamMetadataWithoutGeometriesAsync, StreamRedirectsAsync, StreamAssembledRacesAsync, GetAssembledRaceAsync, GetLastOrganizerReadMetricsSnapshot.
- Keep static DeriveOrganizerKey as a neutral helper (OrganizerUrlRules) so it is not part of the interface.
- Implement FileSystemOrganizerStore with the layout below. Use atomic temp-file plus rename writes; there is no Blob ETag concurrency on disk.
- Make BlobOrganizerStore implement the same interface and add a config-selected DI registration (OrganizerStore:Provider = Blob or FileSystem, plus RaceDataDir).
- Add new configuration keys in Shared/Constants/AppConfig.cs.

### 2. Switch non-queue wiring — API/, Backend/
- Resolve IOrganizerStore in API/Program.cs and inject it into the 11 admin endpoints that currently take BlobOrganizerStore: GetOrganizerStats, GetOrganizerRedirectCandidates, PostManualGpx, PostRaceEvent, GetAssembledRaceById, PostManualMistralScrape, GetAdminStatus, GetRaceOrganizers, GetRaceOrganizerById, PostOrganizerRedirect, GetAssembledRaces.
- Do the same in Backend/BackendServices.cs, RaceDiscoveryService.cs, QueueAllScrapeJobs.cs, ScrapeRaceWorker.cs, ScrapeRaceMistralWorker.cs. Leave all Service Bus calls structurally intact — the other session removes them.
- Coordination point: the other session must keep injecting IOrganizerStore when it strips the queues.

### 3. Redis geocoding cache — Shared/, PmtilesJob/
- Add RedisGeocodingCache implementing IGeocodingCache, using GeocodingCacheKeyParts and the existing TTL options.
- Add a registration extension alongside AddSharedGeocodingCache; switch PmtilesJob/Program.cs to it using RedisConnectionString.

### 4. Race build on local disk — PmtilesJob/
- Replace the Blob-backed organizer stream with the filesystem-backed IOrganizerStore stream; remove BlobServiceClient and the Blob staging/copy publish in ValidateAndPublishPmtilesAsync.
- Write the validated PMTiles to the configured local output path.
- Keep --write-transparency, but write assembled races to disk through WriteAssembledRacesAsync; default off as today.
- Keep tippecanoe invocation, validation, progress logging and temp cleanup unchanged.
- Add the weekly Hangfire recurring job (scheduler mode) and a one-shot CLI path sharing the same build service.

### 5. One-off seed export — Tools/
- New command that reads all organizer documents and route GPX files from Azure Blob and writes the local directory layout. Run manually once during rollout; not in the image.

### 6. Container, Compose and CI
- Replace the stale PmtilesJob/Dockerfile (it uses .NET 8 images for a net10.0 project and omits required source files) with a correct image: .NET 10 SDK build stage, tippecanoe compiled in, .NET 10 runtime final stage.
- Add a pmtiles service to deploy/docker-compose.yml: env_file .env, REDIS_CONNECTION_STRING, a shared volume with api, worker healthcheck, resource limits.
- Update .github/workflows/selfhosted-deploy.yml to build/push the pmtiles image and deploy/scripts/deploy.sh to deploy it.
- Retire .github/workflows/main_pmtilesjob-container.yml (Azure Container App deploy) after cutover.
- Document new variables in deploy/.env.example, deploy/README.md, deploy/DEPLOY.md and PmtilesJob/README.md.

### 7. Public tile endpoint — API/
- Add GET /tiles/trails.pmtiles using Results.File with enableRangeProcessing true and content type application/octet-stream, reading the shared volume read-only. Public, no auth; CORS policy already covers the configured origins.

### 8. Tests and verification
- Unit tests: FileSystemOrganizerStore (read/write/redirects/routes/metadata), RedisGeocodingCache (get/set/expiry), build service (local output plus optional transparency), CLI parsing for new options, tile endpoint range handling.
- Keep existing PmtilesJob.Tests green; update mocks to the new store interface.
- Functional check: seed export, then weekly/one-shot build, then IsValidPmtilesFile, then API Range request, then verify bytes served.
- Differential check: compare feature count and output validity of the old Blob-based build against the new disk-based build on the same seed data.
- Confirm the pmtiles container needs no Azure connection strings.

## Files affected

- New: Shared/Services/IOrganizerStore.cs, Shared/Services/FileSystemOrganizerStore.cs, Shared/Services/RedisGeocodingCache.cs, Shared/Services/OrganizerStoreServiceCollectionExtensions.cs, Tools/ExportOrganizersToDisk.cs, API/Endpoints/Tiles/GetTrailsPmtiles.cs.
- Modified: Shared/Services/BlobOrganizerStore.cs, Shared/Services/GeocodingCacheServiceCollectionExtensions.cs, Shared/Constants/AppConfig.cs, all 11 API admin endpoints plus API/Program.cs, the 5 Backend files listed above, PmtilesJob/Program.cs, RaceFromOrganizersPmtilesBuildService.cs, PmtilesCommandLine.cs, appsettings.json, Dockerfile, README.md, deploy/docker-compose.yml, deploy/.env.example, deploy/README.md, deploy/DEPLOY.md, deploy/scripts/deploy.sh, .github/workflows/selfhosted-deploy.yml, PmtilesJob.Tests/RaceTileBuildServiceTests.cs.
- Removed after cutover: .github/workflows/main_pmtilesjob-container.yml.

## Risks

- Atomicity: disk writes need temp-file plus rename; without it, concurrent API/worker writes can corrupt organizer.json.
- The weekly build must run after the other session's scrape schedule; ordering needs coordination.
- tippecanoe is compiled from source, so the pmtiles image builds slowly.
- The api and pmtiles containers need a shared volume with correct permissions (API read-only).
- Public tiles traverse Tailscale Funnel relays; the runbook already flags PMTiles as the bandwidth workload to watch.
- Nominatim rate limits remain; Redis reduces but does not eliminate them.
- Disk usage from organizer JSON, route GPX, transparency files and PMTiles needs monitoring.
- The seed export temporarily needs the Blob connection string; after that, the container needs no Azure config.

## Explicit assumptions (not decided by you)

- Directory layout follows specs/scraper-filesystem-migration.md: {organizerKey}/organizer.json, {organizerKey}/routes/*.gpx, transparency under {organizerKey}/races/.
- PMTiles output path /data/races/tiles/trails.pmtiles, served at /tiles/trails.pmtiles.
- RedisConnectionString is already available in .env.
- Cache headers preserve the old blob behavior (no-cache) — public access without the cacheable variant.
- No Cosmos container is read or written by this feature.

Please implement this plan, with tests alongside behavior, and report what you changed and what verification you ran.

L2 (orchestrator analysis, 2026-11-17): Read the plan, spec, and current code. Confirmed BlobOrganizerStore's public surface, the 11 admin endpoints, 5 Backend files, PmtilesJob build service/CLI/Program, Shared geocoding cache, AppConfig, Compose/CI/deploy files and test projects. Confirmed concurrent session owns Service Bus removal. Route: 6 bounded delegated writers with disjoint surfaces plus inline parent integration; no work-unit commits because the worktree is shared. Key design: promote the four nested BlobOrganizerStore data types to top-level Shared.Services types so IOrganizerStore can reference them cleanly; add OrganizerDocumentSource.FileSystem and gate transparency writes on `!= Cache`; keep BlobOrganizerStore.DeriveOrganizerKey delegating to OrganizerUrlRules for untouched callers; default OrganizerStore:Provider=Blob to preserve existing behavior everywhere until self-hosted config opts into FileSystem.

L3 (rebase, 2026-11-17): Parent session reported that the Service Bus removal was merged into main as 33b5fa3 and that this branch was an ancestor. Stashed the in-progress changes (`git stash push -u`), fast-forwarded the branch with `git rebase main`, then `git stash pop`. Outcome: clean fast-forward, ZERO conflicts — main touched API/Backend/Shared.Hosting/QueueConfig and deploy docs, but none of the AppConfig.cs/Shared.csproj/ODD-doc files held by this feature. All in-progress changes were preserved. The "keep Service Bus structurally intact" constraint is SUPERSEDED and removed from all subsequent worker prompts; Service Bus is gone from the codebase (QueueClient/Hangfire is the sole transport). No Service Bus code was reintroduced.

L4 (integration decisions, 2026-11-17): (1) CLI --output was parsed but ignored by the build service; added optional outputPath ctor parameter, wired it from the parsed command in PmtilesJob/Program.cs, added ResolveOutputPath internal helper + test (CLI override > config > default). (2) Composer `command: schedule-race-tiles` kept — CLI defaults to one-shot, so restart:unless-stopped would otherwise loop builds. (3) `procps` kept in the runtime image — shared pgrep healthcheck needs it on the slim aspnet base. (4) Tile endpoint exposes internal BuildResult for a unit test; range handling is the framework contract of Results.File(enableRangeProcessing:true) — verified by result properties + byte serving (full 206 needs Kestrel/TestServer).

L5 (verification, 2026-11-17): `dotnet build strava.sln` → 0 errors, 19 pre-existing warnings (NU1510/NU1903/CS0436/CS8629/CS0414). `dotnet test strava.sln --no-build`: Shared.Tests 195/195, PmtilesJob.Tests 32/32, Backend.Tests 384/385 — the single failure is the documented pre-existing `RaceScrapeDiscoveryTests.DeriveEventKeyFromJob_ChoosesBestAvailableSourceUrl` (also failing on main). Role gate `deploy/scripts/verify-role-coverage.sh` green; Ruby YAML parse green; `bash -n deploy/scripts/deploy.sh` green; no ServiceBus/Azure.Messaging references in PmtilesJob/Shared. Functional (real binaries): seeded a local FileSystem store with 2 route geometries, started a throwaway Redis 7 container, ran one-shot build → 1 organizer, 2 features, valid PMTiles magic (1785 bytes), 2 transparency markers, correct local output path. Scheduler smoke: `schedule-race-tiles` registered `{hangfire}:recurring-job:race-pmtiles-weekly` with cron `0 5 * * 1`; Hangfire server started on Redis. Containers/volume removed after. Not run: real Blob seed export (no Azure credentials), Docker image build (tippecanoe source compile), and a true old-Blob-vs-new-disk differential (old publish path needs Azure Blob). Blob store behavior is pinned by the pre-existing 188 Shared.Tests that stayed green.

L6 (review, 2026-11-17): RDD is on (global). `gentle-ai review assess` → risk=high (process boundary: deploy.sh, deleted workflow), review_due=true. Candidate declared with all intended untracked files before START. User consented to "Review this change"; the exact granted invocation returned terminal `lens_context_budget_exceeded` — no review authority was created (51 files / ~3194 lines exceed the reviewer context budget). Per the stop reason this is terminal for this candidate; the feature is reported with that missing proof, and no review receipt or delivery authority exists.

L7 (independent verifier + final fix, 2026-11-17): `gentle-ai-verify` ran all probes in an isolated scratch copy: overall PASS, per-spec S1-S7/S9 satisfied, S8 partial only for environment-gated checks, zero blockers. It raised one advisory: only pmtiles opted into FileSystem in Compose, so API/scrapers would write Blob while the build read disk. Fixed in scope of decision #2: api, worker-schedule and worker-heavy now also set OrganizerStore__Provider=FileSystem, OrganizerStore__RaceDataDir=/data/races and mount race-data:/data/races; deploy/.env.example, README.md and DEPLOY.md updated. `docker compose --profile schedules --profile workers config` confirms all four services share the volume and provider; Ruby YAML parse green. Verifier scratch removed; no stray files in worktree.

L8 (work-unit commits, 2026-11-17): User authorized splitting the change into reviewable commits. Nine Conventional Commits on feature/self-hosted-race-tiles, oldest first: 1e4a0da store abstraction/config; 08db838 filesystem store (1178 lines, one coherent behavior — over the ~400-line heuristic, deliberately not code-golfed); 2e2086e Redis geocoding cache; 1130aff API injection + public tiles endpoint; 3b3e93f Backend store injection; 3284756 local-disk build + weekly Hangfire scheduler; d2c4b74 Tools seed export; dab8cb4 image/Compose/CI/deploy/docs (includes the retired workflow deletion); 4fbe75c this ODD doc. Worktree clean after the split. Every code commit was followed by its focused build/test command with green results; full suite re-run: Shared 195/195, PmtilesJob 32/32, Backend 384/385 (same documented pre-existing failure). `gentle-ai review assess --base-ref <parent> --committed-only` evaluates base-to-HEAD, so it reports the accumulated range (3212 lines) rather than one commit; native review of an individual slice needs a per-commit worktree or a PR slice. No pushes, PRs or delivery actions taken.

L9 (host bind mount, 2026-11-17): User chose `/opt/data/strava/races` as the on-server race data location. Replaced the named `race-data` volume with a host bind mount `${RACE_DATA_DIR:-/opt/data/strava/races}:/data/races` on api, worker-schedule, worker-heavy and pmtiles; removed the unused named volume; documented `RACE_DATA_DIR` in .env.example, README.md, DEPLOY.md and PmtilesJob/README.md. `docker compose config` confirms all four services mount the host path. Server must `mkdir -p /opt/data/strava/races` before recreating containers.
