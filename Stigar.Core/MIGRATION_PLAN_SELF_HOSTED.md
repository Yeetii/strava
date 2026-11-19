# Self-Hosted Migration Plan (in-repo summary)

> **This is a summary, not the source of truth.** The canonical plan is the OpenChamber
> plan `1794838390741-self-hosted-migration-plan.md` (`Self-Hosted Migration Plan`),
> rebaselined 2026-11-17 against the repo. This file exists so the repository carries
> an accurate, self-contained copy of the current state and the phase outline for
> readers who do not have the OpenChamber plan open. Keep the two in sync.
>
> Detail lives in [`specs/functions-runtime-exit.md`](../specs/functions-runtime-exit.md)
> (Functions/Service Bus exit, P0–P3) and
> [`docs/adr/0001-staged-azure-exit-in-csharp-with-rust-pilot.md`](../docs/adr/0001-staged-azure-exit-in-csharp-with-rust-pilot.md).
> Deployment is documented in [`deploy/README.md`](../deploy/README.md) and
> [`deploy/DEPLOY.md`](../deploy/DEPLOY.md).

## Goal

Move all backend workloads and data onto a self-hosted, containerized server and decommission the Azure footprint.

Completed: compute runs on fixed-size plain .NET 10 containers, queues/timers run on Hangfire/Redis, structured data lives in PostgreSQL + PostGIS, and race/shard data lives on local disk. All Azure SDK packages, shims, tooling and deploy workflows have been removed.

---

## Current state (2026-11-18)

| Area | Reality |
| --- | --- |
| Hosts | Plain .NET 10 (ASP.NET Core Kestrel `api` + Generic Host workers). No Functions runtime, no `host.json`, no per-function whitelist env vars. |
| Containers | `api`, `worker-schedule` (profile `schedules`), `worker-heavy` (profile `workers`), `pmtiles`, `redis`, `postgres`. |
| Roles | `WORKER_ROLE` = `api` \| `schedule` \| `heavy` (`Shared.Hosting`). |
| Queues / timers | 11 queues + 12 weekly crons on Hangfire/Redis. No Service Bus SDK. |
| Change feeds | Replaced by PostgreSQL polling jobs; no change-feed processor remains. |
| Data | PostgreSQL + PostGIS for structured data; race/organizer/shard/tile data on local bind mounts. |
| Redis | Durable Hangfire storage: `--appendonly yes`, `--maxmemory-policy noeviction`, plus the shared geocoding cache. |
| Public access | Tailscale Funnel → `https://<machine>.<tailnet>.ts.net`; API publishes `127.0.0.1:8080:80`. |
| CI / deploy | `.github/workflows/selfhosted-deploy.yml`: builds `api`/`backend`/`pmtiles` → GHCR (SHA + `latest`), joins tailnet as `tag:ci`, rsyncs `deploy/`, runs `deploy.sh` (health-check + auto-rollback). The two Azure Function App deploy workflows are removed. |
| Guardrail | `deploy/scripts/verify-role-coverage.sh`: 11 queues / 0 feeds / 12 crons each map to one role; legacy function-list env vars rejected. |
| Monitoring / backups | Beszel. **No automated host backup job in the repo** (`backups/` is gitignored). Back up `/opt/data/strava` (Redis AOF + PostgreSQL data) before storage changes. |

**Still on Azure:** nothing — all Azure services are decommissioned.

**Not done:** general Redis read caching beyond geocoding, and the operational proof listed below.

### Decisions made during implementation

- **No Caddy/TLS** — Tailscale Funnel terminates TLS on our own node.
- **No Prometheus/Grafana** — Beszel covers host and container metrics.
- **Durable Redis** — AOF + `noeviction`; the old `allkeys-lru` default would drop queued jobs. Cache keys need a separate instance/DB or explicit TTL + `maxmemory` policy.
- **Shared change-feed leases** — cutover needs deliberate handover (lease takeover, or new prefix + bounded overlap).

---

## Phase outline

**Phases 1–3 — completed in code.** Functions runtime + Service Bus exit. Superseded by `specs/functions-runtime-exit.md` (P0–P3): plain hosts, merged `api`, Hangfire queues/timers, in-process change feeds, role-coverage gate. Service Bus removal went beyond the original plan.

Still-open proof (do not claim done): worker soak against production traffic; P0 baseline of Cosmos RU/s, queue depth and endpoint latencies; P1b OpenAPI diff, webhook test event and Trailscope smoke tests.

**Phase 4 — reduce Cosmos load.** Geocoding (`RedisGeocodingCache`, shared by Backend and PmtilesJob) is the first cache-aside win. Remaining: cache-aside on summit/visited/status reads; precomputed read models; reduce polling; batch/debounce writes; keep handlers idempotent.

**Phase 5 — local Postgres for read offloading.** Deploy Postgres with persistent storage, backups and a tested restore. Scope: structured Cosmos entities only (`activities`, `users`, `userSyncItems`, `sessions`, `visitedPaths`, `visitedAreas`, `peaksGroups`, `summitedPeaks`, `osmFeatures`). Race/organizer data stays on the filesystem — do not mirror it into Postgres. Cosmos remains source of truth for writes.

**Phase 6 — prepare full migration.** Target schema (normal tables + `jsonb`, indexed query paths), respecting the partition keys in [`COSMOS_CONTAINERS.md`](COSMOS_CONTAINERS.md) — `races`/`osmFeatures` use composite `(x, y)` keys, `raceOrganizers` is keyed by organizer key/id. Migration tooling (export/transform/import/validate). Selective dual-write with per-entity flags.

> **Risk:** the test suite is unit-only with mocks — no integration tests. Dual-write and cutover logic is exactly what those would pin; treat it as a known risk in each phase's verification.

**Phase 7 — final cutover.** Short controlled write freeze per entity: pause/queue writes → final sync → validate counts → switch reads then writes → resume. Keep Cosmos read-only through a validation period.

**Phase 8 — decommission (complete).** All Azure services are gone: Functions runtime, Service Bus, Tables, Blob and Cosmos have been removed from source, along with their SDK packages, dead shims, migration tooling, Azure Workbooks and the two Azure Function App deploy workflows. Historical specs and ADRs are kept as the migration record.

---

## Rollback

- Backend/worker: last-known-good image tag (`scripts/deploy.sh <tag>`) + compose profiles. **Not** "route back to Azure Functions" once compute is decommissioned — the durable rollback is the image tag plus the Redis AOF snapshot.
- Read offloading: point reads back at Cosmos behind the feature flag.
- Final DB migration: Cosmos kept read-only, verified backups, flag to switch DB target, record-count validation.

---

## Next actions

1. Add a host backup job — there is still none in the repo; back up the Redis AOF and PostgreSQL data directory.
2. Finish general read caching beyond the geocoding cache.
3. Capture an operational baseline on the self-hosted stack: endpoint latency, Hangfire queue depth, Funnel bandwidth for tile endpoints.
4. Keep `dotnet test strava.sln` and `deploy/scripts/verify-role-coverage.sh` green on future changes.
