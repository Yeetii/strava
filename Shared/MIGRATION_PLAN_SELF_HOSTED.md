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

Reduce Azure costs by moving database-heavy and compute-heavy backend workloads onto a self-hosted, containerized server while keeping the project operational during the transition.

What still drives cost: Cosmos DB RU pressure, excessive reads/writes, and the remaining Azure store footprint (Blob, Tables). Azure Functions scaling is no longer a problem — compute is self-hosted on fixed-size containers.

---

## Current state (2026-11-17)

| Area | Reality |
| --- | --- |
| Hosts | Plain .NET 10 (ASP.NET Core Kestrel `api` + Generic Host workers). **No Azure Functions runtime, no `host.json`, no per-function whitelist env vars.** |
| Containers | `api`, `worker-schedule` (profile `schedules`), `worker-heavy` (profile `workers`), `pmtiles`, `redis`. |
| Roles | `WORKER_ROLE` = `api` \| `schedule` \| `heavy` (`Shared.Hosting`). |
| Queues / timers | 11 queues + 12 weekly crons on Hangfire/Redis. **Service Bus SDK removed from source.** |
| Change feeds | 3 Cosmos feeds in-process via SDK `ChangeFeedProcessor`, same lease containers. |
| Redis | Durable Hangfire storage: `--appendonly yes`, `--maxmemory-policy noeviction`. Not yet a general read cache. |
| Public access | Tailscale Funnel → `https://<machine>.<tailnet>.ts.net`; API publishes `127.0.0.1:8080:80`. No Caddy/TLS or DNS cutover. |
| Race/organizer data | `IOrganizerStore` / `FileSystemOrganizerStore` (`OrganizerStore__Provider=FileSystem`), host dir `/opt/data/strava/races` → `/data/races`. `pmtiles` builds tiles there; API serves `/tiles/trails.pmtiles`. |
| Geocoding cache | Single `RedisGeocodingCache` used by both Backend and PmtilesJob. |
| CI / deploy | `.github/workflows/selfhosted-deploy.yml`: builds `api`/`backend`/`pmtiles` → GHCR (SHA + `latest`), joins tailnet as `tag:ci`, rsyncs `deploy/`, runs `deploy.sh` (health-check + auto-rollback). |
| Guardrail | `deploy/scripts/verify-role-coverage.sh`: 11 queues / 2 feeds / 12 crons each map to one role; legacy function-list env vars rejected. |
| Monitoring / backups | Beszel. **No automated host backup job in the repo** (`backups/` is gitignored) — adding one is part of adding Postgres. Cosmos/Storage have platform backups. |

**Still on Azure:** Cosmos DB (`activities`, `users`, `userSyncItems`, `sessions`, `visitedPaths`, `visitedAreas`, `peaksGroups`, `summitedPeaks`, `osmFeatures`, `raceOrganizers`), Blob.

**Not done:** Postgres (no `Npgsql`/EF/PostgreSql reference anywhere), general Redis read caching beyond geocoding, and the operational proof listed below.

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

**Phase 8 — decommission.** Order: **Tables → Blob → Cosmos**. The `PmtilesJob` Rust pilot is a parallel ADR-0001 track, not a phase of this plan.

---

## Rollback

- Backend/worker: last-known-good image tag (`scripts/deploy.sh <tag>`) + compose profiles. **Not** "route back to Azure Functions" once compute is decommissioned — the durable rollback is the image tag plus the Redis AOF snapshot.
- Read offloading: point reads back at Cosmos behind the feature flag.
- Final DB migration: Cosmos kept read-only, verified backups, flag to switch DB target, record-count validation.

---

## Next actions

1. Capture the baseline: Cosmos RU/call volume per container, endpoint latency, Hangfire queue depth, Funnel bandwidth for tile endpoints.
2. Finish Phase 4 caching (the geocoding cache is Redis-only; Azure Tables is retired).
3. Pilot Postgres on one derived container (e.g. `visitedPaths` or `peaksGroups`), serve reads, measure against the baseline.
4. Add a host backup job — there is none in the repo today.
