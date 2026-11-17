# ADR 0001: Staged Azure exit in C# with a Rust pilot — not a full Rust rewrite

- **Status:** Accepted
- **Date:** 2026-11-16
- **Deciders:** Erik Magnusson
- **Related:** [`Shared/MIGRATION_PLAN_SELF_HOSTED.md`](../../Shared/MIGRATION_PLAN_SELF_HOSTED.md), [`deploy/DEPLOY.md`](../../deploy/DEPLOY.md), [`specs/functions-runtime-exit.md`](../../specs/functions-runtime-exit.md), [`specs/pmtile-build-performance-plan.md`](../../specs/pmtile-build-performance-plan.md)

---

## Decision

Exit Azure in **C#**, by executing the existing phased plan in
`Shared/MIGRATION_PLAN_SELF_HOSTED.md` (workers → Redis → PostgreSQL → cutover → decommission).

In parallel, port **`PmtilesJob` to Rust** as a contained pilot.

**Do not** rewrite `API` or `Backend` in Rust now.

Re-evaluate a broader Rust port only after PostgreSQL and Redis are live and the
pilot has produced measurable numbers (see [Re-evaluation triggers](#re-evaluation-triggers)).

---

## Goals

| # | Goal | Weight |
|---|------|--------|
| 1 | Leave Azure entirely (transition period allowed) | Primary |
| 2 | Performance | Primary |
| 3 | Project structure | Primary |
| 4 | Code quality | Primary |
| 5 | Learn Rust | Side benefit, not a goal |

---

## Context

### What exists today

- **~44,000 LOC of C# on .NET 10** (estimates ±20%, measured by sampling):
  `API` ≈ 7,100 · `Backend` ≈ 18,500 · `Shared` ≈ 17,100 · `PmtilesJob` = 1,667.
  Plus ~9,200 LOC of tests.
- Everything runs as **Azure Functions isolated workers** — including the HTTP API.
- **Compute is already self-hosted** (`deploy/docker-compose.yml`), but all data
  services remain on Azure: Cosmos DB, Service Bus, Blob Storage, Tables
  (geocoding cache), Azure SignalR. `PmtilesJob` deploys to Azure Container Apps.
- The test suite is **unit-only with mocks — no integration tests**. This is the
  safety net (or lack of one) available to any large structural change.

### Why the question came up

Migrating to Rust with a worker architecture on the self-hosted server was
proposed as an alternative to the C# path. The decision below is the outcome of
comparing both paths against the goals above.

### Rust ecosystem, November 2026

| Capability | State |
|------------|-------|
| Web framework | `axum` 0.8.9 — default choice, Tokio/Tower ecosystem |
| Job queue | `apalis` 1.0.0 (stable, Oct 2026) — Redis/PostgreSQL/SQLite backends, retries, cron, workflows |
| Cosmos DB SDK | `azure_data_cosmos` 1.0.0 **GA, Oct 2026** — official, stable |
| Blob SDK | `azure_storage_blob` 1.x — official, stable |
| Service Bus SDK | **None (official)** — AMQP would be hand-built via `fe2o3-amqp` |
| Tables SDK | **None (official)** |
| SignalR server | **None** — raw WebSockets + frontend client rewrite |

### Performance evidence (Nov 2026, external benchmarks)

- **Throughput: near-parity.** A controlled 4-core comparison measured ASP.NET
  Core 10 at 46,662 req/s vs Rust/axum at 46,687 req/s. Other benchmarks give
  Rust 1.5–2x. None of this matters here: this API's latency is dominated by
  Cosmos round-trips from the home server to an Azure region, the Strava API,
  and Overpass calls with 10-minute timeouts.
- **Memory: Rust wins ~8x** (≈5–9 MB vs ≈46–110 MB idle per app). Real, but not
  decisive for a server running ~4 containers.
- **Tail latency: Rust flatter** (no GC pauses). Dominated by I/O in this system.
- **The actual performance lever is data locality:** PostgreSQL on the same box
  as the workers (plan Phase 5–7) removes the cloud round-trip from every read.
  That win is language-agnostic.

---

## Options considered

| | A. Full Rust rewrite now | B. C# Azure exit only | C. C# exit + Rust pilot ✅ |
|---|---|---|---|
| API performance | Language parity; DB still remote unless also migrated | Local Postgres removes cloud round-trips | Same as B |
| Worker architecture | `axum` + `apalis` — clean, but all Functions trigger semantics rebuilt by hand | Functions → explicit app + worker topology (plan Phase 2) | B's topology + pilot as standalone Rust worker |
| Azure transition | **Big bang**: no official Rust SDKs for Service Bus, Tables, SignalR; change-feed, lock renewal, dead-lettering hand-built before anything can be turned off | Gradual: feature flags, dual-write, per-workload cutover, rollback at every step | Gradual in C#; pilot fully decoupled from the API |
| Code quality | Great eventually; degraded for months during rewrite, with a mock-only test net | Keeps tested, layered code; sheds the Functions model | Risk contained to 1,667 LOC |
| Effort | ~44k LOC + trigger semantics — realistically a year of part-time work | The plan, as written | Plan + bounded pilot |
| Learning Rust | Immediate, sink-or-swim | None | On a contained, CPU-bound surface where Rust genuinely wins |

---

## Why the full rewrite loses (goal by goal)

1. **Performance** — throughput parity; the dominant lever is local data, not
   language. Rust's real edges (memory, p99) are marginal at this scale.
2. **Project structure** — the structural pain is the **Azure Functions
   programming model** (trigger attributes, function-per-file, dual JSON stacks:
   Newtonsoft + System.Text.Json). Plan Phase 2 ("refactor Functions into app
   services and workers") fixes that in either language. Rust is orthogonal to
   the win.
3. **Code quality** — a 44k-LOC rewrite in a newly-learned language, validated
   only by mock-based unit tests, degrades quality before it improves it. The
   current code is layered, benchmarked, and deployed.
4. **Transition** — C# keeps the Functions machinery (change feed, Service Bus,
   timers) working while stores are swapped one at a time behind the existing
   abstractions in `Shared`. Rust would force the hardest infrastructure work
   (queues, change feed, realtime) **first**, turning the "transition period"
   into a big-bang cutover. SignalR alone: in C#, leaving Azure SignalR is
   removing one line (self-hosted ASP.NET Core SignalR is built in); in Rust,
   no server implementation exists, so realtime means raw WebSockets **and** a
   Trailscope client rewrite.

---

## Consequences

**Positive**

- Azure exit proceeds gradually, per-workload, with rollback — matching the
  existing plan's risk model.
- The structure goal is met by Phase 2 (plain ASP.NET Core API + worker
  containers) without a rewrite.
- The Rust pilot satisfies the learning goal at contained risk, on a workload
  where Rust's advantages are real (CPU-bound tile/geometry processing, no GC
  pauses on long builds).
- Porting `PmtilesJob` also removes its Azure Container Apps deployment —
  one more Azure service off the bill, and it can run as another compose
  service on the home server.
- `Shared.Benchmarks` (`FindPointsNearRouteBenchmark`) provides an existing
  harness to measure what Rust actually buys, before any bigger bet.

**Negative**

- Two languages in the repo after the pilot. Acceptable: the pilot is a leaf
  worker with no API coupling.
- .NET memory overhead stays (~50–110 MB per app). Acceptable for ~4 containers.
- The rewrite question will come back. This ADR exists so the answer is
  re-derived from evidence, not vibes.

**Neutral**

- Rust skills build in a corner that does not gate delivery.

---

## Execution sequence

The Functions-runtime exit itself is now specified in detail by the separate
"Azure Functions Runtime Exit — Refactoring Plan" (four phases: Redis
foundations → plain hosts → Hangfire queue swap → decommission). That spec
supersedes Phase 2 of the older migration plan for the HTTP/worker topology;
PostgreSQL and the Rust pilot remain unchanged.

| Step | Work | Source |
|------|------|--------|
| 1 | Validate `worker-heavy` against production traffic | Plan: "Immediate next actions" |
| 2 | Phase 2 — shed the Functions model **and exit Service Bus** (one campaign, sequenced: traffic first, then queues) | [`specs/functions-runtime-exit.md`](../../specs/functions-runtime-exit.md) |
| 3 | Phase 4 — Redis caching (queue/timer storage lands with the Functions exit above) | Plan: Phase 4 |
| 4 | Phase 5–7 — PostgreSQL, dual-write, cutover | Plan: Phases 5–7 |
| 5 | **In parallel, any time: port `PmtilesJob` to Rust** (pilot; own spec to follow) | This ADR |
| 6 | Phase 8 — decommission remaining Azure services | Plan: Phase 8 |

---

## Re-evaluation triggers

Revisit this decision when any of these hold:

1. **PostgreSQL + Redis are live** (post-Phase 7): a strangler-style Rust port
   of hot endpoints becomes practical — `sqlx`, `apalis`, and `axum` are
   first-class against those backends.
2. **The pilot shows a large win** on the tile-build benchmark: extend Rust to
   the `Shared/Geo` / `Shared/Services/Shards` hot paths next, still as leaf
   workers.
3. **Server memory becomes a constraint**: Rust's ~8x memory advantage starts
   to matter.
4. **Traffic grows by orders of magnitude**: Rust's tail-latency stability
   starts to matter.

None of these are expected on the current trajectory; they are recorded so the
decision is falsifiable, not final.

---

## References

- Migration plan: `Shared/MIGRATION_PLAN_SELF_HOSTED.md`
- Azure SDK for Rust releases (Cosmos DB 1.0.0 GA, Oct 2026): <https://azure.github.io/azure-sdk/releases/latest/rust.html>
- Apalis 1.0.0 announcement (Oct 2026): <https://apalis.dev/blog/introducing-v1-2026-10-16>
- Controlled .NET 10 vs Rust throughput comparison: <https://arunprakashg.com/blogs/cross-language-backend-benchmark>
- Sharkbench framework resource comparison (memory): <https://sharkbench.dev>
