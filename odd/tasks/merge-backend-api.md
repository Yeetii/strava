# Merge backend-api into api

Objective: fold the Backend HTTP functions into the single `api` host so only one public HTTP container remains. Branch: main. Delivery: work-unit commits on a feature branch; test runner `dotnet test strava.sln`.

## Specs

S1. "before doing this, do a rewrite and merge the backend api into the normal api. there is no reason to have them as seperate anymore right?" — Authorized scope: remove the separate `backend-api` container and serve its HTTP functions from the `api` container.

S2. Acceptance: the merged `api` host maps every HTTP trigger from both the API and Backend assemblies, with no route/verb collisions and no lost functions. DI is registered exactly once per service (no duplicate `CosmosClient`, `CollectionClient<T>`, `ServiceBusClient`, or named HttpClient registrations), and API behavior (Cosmos connection mode and retry settings) is preserved.

S3. Acceptance: `ApiUrl`/`BackendApiUrl` resolve to the merged host for workers; no container depends on a removed `backend-api` service; deploy workflow, smoke test, role-coverage gate, and docs no longer reference `backend-api`.

S4. Verification: `dotnet test strava.sln` passes (the known pre-existing `RaceScrapeDiscoveryTests.DeriveEventKeyFromJob_ChoosesBestAvailableSourceUrl` failure excepted), `deploy/scripts/verify-role-coverage.sh` is green, and a live request to the merged host reaches the Strava webhook route.

## Tasks

T1. S1-S2 — Map exact DI overlap, API-only registrations, and cross-assembly route collisions (route: delegated read-only explorer `ses_e167bfe90ffeSLJhjMu3neyRiw`; status: done). Trigger: mapping two DI graphs and two trigger inventories exceeded the inline evidence budget. Result: no route/verb collisions; 3 Backend-only services needed; API Cosmos settings must be kept.
T2. S2 — Rewrite `api` host to reference Backend, register the 3 Backend-only services, and map both assemblies (route: inline; status: done, commit: e7e79cf).
T3. S3 — Update compose, env docs, scripts, and docs to remove `backend-api` (route: inline; status: done, commit: this commit).
T4. S4 — Cross-assembly route/verb test in `PlainHostTests`, full suite, role gate, compose validation (route: inline; status: done, commit: e7e79cf). Live webhook verification is the next P1b action, deferred because it needs server access.

## Log

L1 (2026-11-17): before doing this, do a rewrite and merge the backend api into the normal api. there is no reason to have them as seperate anymore right?
