# Technical Debt Remediation
Objective: Remediate critical security gaps, CI test omissions, repository bloat, stale documentation, architectural duplication, and compiler warnings identified in the repository technical debt scan. Branch: chore/tech-debt-scan. Delivery: work-unit commits per task on feature branch. Test runner: `dotnet test strava.sln`.

## Specs

S1. "Security: Authorize and Protect Backend Batch Endpoints & Pin Vulnerable Package"
- All endpoints in `Stigar.Api/Endpoints/Backend/` (`QueueAllSummitJobs`, `QueueAllVisitedPathsJobs`, `QueueAllVisitedAreasJobs`, `QueueAllScrapeJobsHttp`, `PostScrapeOrganizer`, `TestScrapeUrl`, `RunSummitsForActivity`, `BackfillVisitedPathOsmHighwayIds`, `BackfillAdminBoundaryMetrics`, `CleanupExcludedActivityTypes`, `StoreCuratedPeaksGroups`) must enforce `context.IsAdminAuthorized(configuration)`. Unauthenticated requests must return HTTP 401 Unauthorized.
- Pin `Microsoft.OpenApi` in `Stigar.Api.csproj` (and test projects if needed) to a version `>= 2.0.1` (or latest patched release) to mitigate `GHSA-v5pm-xwqc-g5wc`.
- Tighten `GetTrailsPmtiles.CorsPolicy` in `Stigar.Api/Program.cs`: remove `AllowCredentials()` when origin matching is open or align with configured origins.
- Add unit/integration tests in `Stigar.Workers.Tests` asserting 401 Unauthorized for unauthenticated calls and success when authorized with the admin API key.

S2. "CI/CD: Add Automated Unit Testing to Deployment and PR Workflows"
- In `.github/workflows/selfhosted-deploy.yml`, add `dotnet test --no-build` in the `verify` job (or dedicated `test` job) after building the solution, ensuring tests execute before any Docker image build or deployment.
- Add a dedicated PR and CI workflow `.github/workflows/ci.yml` that triggers on pull requests and pushes to `main`, running `dotnet build` and `dotnet test`.

S3. "Hygiene: Purge Committed Binaries and Temporary Bloat from Git"
- Remove `urllib.request` (23.9 MB PostScript dump), `tmp-output.txt`, `admin-boundaries.pmtiles` (28 KB), `admin-boundaries.pmtiles-journal` (4.6 KB), and `skyrunning-html-snippets.txt` from Git tracking (`git rm --cached`).
- Update `.gitignore` to explicitly ignore `*.pmtiles`, `*.pmtiles-journal`, `urllib.*`, and `tmp-*.txt`.

S4. "Documentation: Purge Stale Cosmos DB & Service Bus Context"
- Remove obsolete `COSMOS_CONTAINERS.md` and `USER_SYNC.md` (or replace with PostgreSQL table documentation).
- Clean `API.md` (line 5 reference to `COSMOS_CONTAINERS.md`).
- Clean `.github/copilot-instructions.md`, `.github/agents/cosmos-1000ru-pressure.agent.md`, and `.specify/memory/constitution.md` to remove obsolete Cosmos DB partition key, 1000 RU/s throttling, `CosmosWriteThrottle`, and `CollectionClient<T>` rules that poison agent context.

S5. "Architecture: Unify DI Registrations, Eliminate Lifecycle Drift, and Remove Dual JSON Serializers"
- Consolidate duplicated service registrations between `Stigar.Api/Program.cs` and `Stigar.Workers/BackendServices.cs` into shared extension methods in `Stigar.Core/Services/` (e.g. `AddStigarSharedServices()`).
- Unify `ISummitsCalculator` lifetime registration across both hosts (preventing Singleton vs Scoped drift).
- Replace `Newtonsoft.Json` (`JObject`) in `Stigar.Core/Services/OverpassClient.cs`, `Stigar.Api/Endpoints/Backend/StravaWebhook.cs`, and `Stigar.Workers.Tests/StravaWebhookTests.cs` with `System.Text.Json` (`JsonDocument` / `JsonNode`).
- Remove `Newtonsoft.Json` package reference from `Stigar.Core.csproj`.

S6. "Code Quality: Fix All 23 Compiler Warnings and Add .editorconfig"
- Fix nullability warnings (`CS8602`, `CS8604`, `CS8629`) in `RaceAssembler.Distance.cs`, `GetSkiDays.cs`, and `GarminCourseEndpoints.cs`.
- Replace obsolete Hangfire `RecurringJobOptions.QueueName` (`CS0618`) in `RaceTilesScheduler.cs` and `RaceTileBuildServiceTests.cs` with the non-obsolete `AddOrUpdate` queue parameter.
- Resolve unused field in `SummitsBenchmarks.cs` and xUnit analyzer warning `xUnit2031` in `OrganizerRedirectProbeHelperTests.cs`.
- Remove unnecessary PackageReference warnings (`NU1510`) in `Stigar.Workers.Pmtiles.csproj`.
- Add logging to swallowed exceptions in `ItraDiscoveryAgent.cs:190`.
- Add standard `.editorconfig` file in repo root to standardize formatting and Roslyn analyzer rules.

## Tasks

T1. S1 — Authorize backend batch endpoints with `IsAdminAuthorized`, pin `Microsoft.OpenApi`, tighten CORS, and add auth tests (route: inline; status: todo).
T2. S2 — Update `.github/workflows/selfhosted-deploy.yml` and add `.github/workflows/ci.yml` to execute `dotnet test` (route: inline; status: todo).
T3. S3 — Untrack large binary files and temporary clutter from Git, and update `.gitignore` (route: inline; status: todo).
T4. S4 — Remove stale Cosmos DB / Service Bus documentation files and clean agent instructions / constitution (route: inline; status: todo).
T5. S5 — Consolidate shared DI graph between API and Workers, resolve `ISummitsCalculator` lifecycle drift, and replace `Newtonsoft.Json` with `System.Text.Json` (route: inline; status: todo).
T6. S6 — Fix all 23 compiler warnings (`CS8602`, `CS8604`, `CS8629`, `CS0618`, `xUnit2031`, `NU1510`), add exception logging in `ItraDiscoveryAgent`, and add root `.editorconfig` (route: inline; status: todo).

## Log

L1 (2026-11-20): Original request: "Do a technical debt scan of the repo" followed by "Turn into tasks with enough context for other agents to work on."
L2 (2026-11-20): Scan revealed 6 distinct debt categories: unauthenticated batch endpoints in `Stigar.Api/Endpoints/Backend/`, zero test execution in CI/CD, 24MB committed PostScript/binary clutter, stale Cosmos DB documentation causing context poisoning, DI registration duplication and dual JSON serializer usage, and 23 compiler warnings.
