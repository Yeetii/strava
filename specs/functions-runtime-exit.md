# Azure Functions Runtime Exit — Refactoring Plan

- **Status:** Proposed
- **Date:** 2026-11-16
- **Scope:** Remove the Azure Functions runtime from all four containers; remove
  Azure Service Bus in the same campaign. **Cosmos DB, Blob, Tables, and Azure
  SignalR stay.**
- **Related:** [`docs/adr/0001-staged-azure-exit-in-csharp-with-rust-pilot.md`](../docs/adr/0001-staged-azure-exit-in-csharp-with-rust-pilot.md),
  [`Shared/MIGRATION_PLAN_SELF_HOSTED.md`](../Shared/MIGRATION_PLAN_SELF_HOSTED.md),
  [`deploy/docker-compose.yml`](../deploy/docker-compose.yml)

---

## Problem

Compute already runs in self-hosted Docker containers, but the app is still the
Functions programming model:

| Symptom | Evidence |
|---------|----------|
| Every container runs the Functions isolated-worker image | `deploy/docker-compose.yml:17-24` (`x-functions-env`) |
| Roles are split via `AzureFunctionsJobHost__functions__N` env vars, not code | `deploy/docker-compose.yml:77-90, 114-125, 154-168` |
| Production external traffic still enters through the Azure Function Apps; the home stack is internal-only, warm standby | `deploy/docker-compose.yml:3-9`, `Shared/MIGRATION_PLAN_SELF_HOSTED.md:29-31, 52-59` |
| Background semantics (queue consumers, change feeds, timers) exist only as Functions triggers | 12 × `ServiceBusTrigger`, 3 × `CosmosDBTrigger`, 12 × `TimerTrigger` (see inventory) |
| Queue scaling depends on a custom rescaler reading Azure Monitor | `Backend/ServiceBusRescheduler` via `Backend/Program.cs:205-208` |
| `host.json` runtime semantics are invisible app behavior | `Backend/host.json:15-23` |

Removing the Functions runtime while keeping these behaviors requires replacing:
the trigger bindings (Service Bus, Cosmos change feed, timer), the HTTP host,
and the runtime's execution semantics.

---

## Solution

Convert the four containers to plain .NET 10 hosts, in a four-phase campaign
where each phase is independently deployable and rollbackable:

| Concern | Replacement |
|---------|-------------|
| HTTP host (API endpoints + 14 Backend functions) | **ASP.NET Core (Kestrel)** — single merged `api` host, route-identical minimal APIs |
| Timer triggers (12 weekly crons) | **Hangfire recurring jobs** on Redis (cron strings carry over verbatim) |
| Service Bus queues (12) | **Hangfire jobs** on Redis (durable, retried, dashboard) |
| Service Bus runtime semantics | Per-queue Hangfire servers (see [Semantics mapping](#semantics-mapping)) |
| Cosmos change feed (3 triggers) | **Cosmos SDK `ChangeFeedProcessor`** in-process, same lease containers |
| Azure SignalR | **Stays.** Negotiate endpoint moves to ASP.NET Core; worker-side output is a stub today (`Backend/NewSummitedPeak.cs:24` returns `[]`) |
| ServiceBusRescheduler | **Deleted** with Service Bus; worker counts become static config |
| Functions base images | Plain `mcr.microsoft.com/dotnet/aspnet` runtime images |

### The sequencing constraint that shapes this plan

> **Azure Functions cannot reach a Redis instance on the home network.**
> Consumption-plan Function Apps have no way to join the tailnet or a VPN, and
> the home Redis must never be publicly exposed.
>
> While any production producer still runs inside an Azure Function App
> (today: all external traffic), queues **must stay on Service Bus**.
> Therefore: convert the runtime first (keeping Service Bus), take over
> production traffic, then swap queues. Swapping both at once would either
> split producers across two transports (split-brain) or require exposing Redis
> publicly.

This is why "leave Service Bus in the same move" is one campaign with two
sequenced phases, not one big-bang cutover.

### Decision: Hangfire (and what was rejected)

| Option | Verdict | Why |
|--------|---------|-----|
| **Hangfire on Redis** (via `Hangfire.Redis.StackExchange`) | ✅ chosen | One dependency covers timers **and** durable queues, with retries, dead-letter handling, and a dashboard (replacing lost Azure-side visibility). Already compatible with the migration plan's "Redis for caching and queueing" |
| Quartz.NET | ❌ | Scheduler only — queues would still need a second DIY mechanism |
| Redis Streams DIY | ❌ | Fidelity to batch semantics, but you own the consumer loop, retries, DLQ, and monitoring forever |
| Keep Service Bus | ❌ (deferred by decision) | User decision: exit in this campaign. Also removes the last Azure Monitor dependency |

Licensing note: Hangfire core is LGPL (fine for personal/self-hosted);
`Hangfire.Redis.StackExchange` storage is MIT.

---

## Target architecture

```
                    internet
                        │
              Tailscale Funnel (decided 2026-11-16)
                        │
        ┌───────────────┴───────────────┐
        │  api           (ASP.NET Core, Kestrel)  ← API + Backend HTTP functions
        │  worker-schedule (Generic Host: Hangfire server, recurring jobs)
        │  worker-heavy    (Generic Host: Hangfire servers + Cosmos
        │                   ChangeFeedProcessors)        ← replaces Functions triggers
        │  redis          (durable: AOF, noeviction)
        └───────────────┬───────────────┘
                        │ outbound only
        Cosmos DB · Blob · Tables · Azure SignalR      ← stay (per scope)
```

Container roles, healthchecks, mem/cpu limits, and the deploy pipeline are
preserved from `deploy/docker-compose.yml`; only the images and the
function-list env vars change (role selection becomes an app argument/env var
of the plain hosts).

---

## Inventory

### Service Bus queues (12) — cutover units

Queue names from `Shared/Constants/ServiceBusConfig.cs:22-33`; trigger config
from the worker classes and `Backend/host.json:16-23`.

| Queue | Consumer | Batching | Producers (grep-verified, verify all sites at implementation) |
|-------|----------|----------|----------------------------------------------------------------|
| `calculateSummitsJobs` | `SummitsWorker` | batch 20 | `QueueActivityJobs` (change feed), `QueueAllSummitJobs` (HTTP backfill), admin endpoints |
| `calculateVisitedPathsJobs` | `VisitedPathsWorker` | batch 20 | `QueueActivityJobs`, `QueueAllVisitedPathsJobs`, admin |
| `calculateVisitedAreasJobs` | `VisitedAreasWorker` | batch 20 | `QueueActivityJobs`, `QueueAllVisitedAreasJobs`, admin |
| `activitiesfetchjobs` | `StravaActivitiesFetcher` | single | `PostLogin` (API!), self-chaining |
| `activityFetchJobs` | `StravaActivityFetcher` | single | `StravaActivitiesFetcher` (chained per activity) |
| `activityprocessed` | `ActivityEventWorker` | batch 20 | `SummitsWorker:147`, `RunSummitsForActivity:107` |
| `activityDeleteJobs` | `DeleteActivityWorker` | single | API/Backend call sites |
| `scrapeRace` | `ScrapeRaceWorker` | batch 20 | `QueueAllScrapeJobs` (timer), `PostRaceEvent` (admin) |
| `raceDiscoveryJobs` | `RaceDiscoveryWorker` | single | `RaceDiscoveryService` (chunked), `PostRaceDiscovery` (admin) |
| `mistralScrape` | `ScrapeRaceMistralWorker` | single | `PostManualMistralScrape` (admin) |
| `enrichAdminBoundaryJobs` | `AdminBoundaryEnrichmentWorker` | batch 20 | `OsmFeaturesChangeTrigger` (change feed), `ReEnrichAdminBoundaries` (admin) |
| `accountDeleteJobs` | `DeleteAccountWorker` | single | API call sites |

`ServiceBusConfig.NonTimeCriticalQueues` exists for the rescaler — deleted with it.

### Change feed triggers (3)

| Function | Container watched | Notes |
|----------|-------------------|-------|
| `QueueActivityJobs` | `activities` | fans out to the three calculate queues (`QueueActivityJobs.cs:30-36`) |
| `OsmFeaturesChangeTrigger` | `osmFeatures` | enqueues enrichment jobs (`OsmFeaturesChangeTrigger.cs:42,77`) |
| `NewSummitedPeak` | `summitedPeaks` | SignalR output **stub** — returns `[]` (`NewSummitedPeak.cs:24`) |

All three use `CreateLeaseContainerIfNotExists = true` and today share lease
containers with the Azure-side triggers (`Shared/MIGRATION_PLAN_SELF_HOSTED.md:68-71`).

### Timers (12 — all weekly crons, verbatim cron strings)

`QueueAllScrapeJobs` (`0 0 3 * * 2`), `DiscoverTrailrunningSwedenRaces`
(`0 0 3 * * 1`), and 10 × `Discover*` at `0 0 2 * * 1`
(trigger greps; full list in `deploy/docker-compose.yml:114-125`).

### HTTP (73 endpoints total)

- `api`: 59 endpoint classes under `API/Endpoints/` — route-identical
  conversion to minimal APIs.
- Backend HTTP: 14 functions (webhook, token refresh, admin, backfill) now
  hosted in the merged `api` container, including the **Strava webhook** whose
  public URL must not change semantics.
- SignalR negotiate: `ConnectSignalR` (`API/Endpoints/ConnectSignalR.cs:14-15`,
  hub `peakshunters`, user from `{headers.session}`).

---

## Semantics mapping

What `Backend/host.json` and the bindings give for free today, and their
replacements:

| Today (Functions) | Replacement (plain host) | Notes |
|---|---|---|
| `maxConcurrentCalls: 1` per queue | One Hangfire server per queue, `WorkerCount = 1` | The container-role split is preserved; serialization semantics identical |
| `maxMessageBatchSize: 20`, `prefetchCount: 20` | Per-item Hangfire jobs | Functions batching amortized trigger overhead, not Cosmos reads — but **measure Cosmos RU per queue before/after**; if RU rises on the heavy calculators, introduce a batcher job per queue |
| `maxAutoLockRenewalDuration: 10 min` | Not needed — jobs run to completion | Net win for long Mistral/race-discovery jobs |
| `functionTimeout: 00:09:59` | Not needed | **Watch for previously-killed jobs now running longer** |
| `AutoCompleteMessages = false` + manual complete/abandon/dead-letter | Hangfire automatic retry with attempts + backoff per queue | `ServiceBusCosmosRetryHelper` keeps its Cosmos-side 429 retry logic; its ServiceBus abandon paths are deleted |
| Change feed with shared lease containers | SDK `ChangeFeedProcessor` reusing the same lease containers | Cutover strategy per feed — see [Change-feed conversion](#change-feed-conversion). `NewSummitedPeak` is a stub, so its feed is trivial |
| Functions invocation logs / App Insights | Hangfire dashboard + existing Beszel + logs | Rescaler's Azure Monitor dependency disappears |

---

## Change-feed conversion

The `CosmosDBTrigger` **is** the .NET SDK's `ChangeFeedProcessor` — the Functions
extension builds one on the same `Microsoft.Azure.Cosmos` package `Shared`
already references. Converting a feed means re-hosting the same processor, not
reimplementing its behavior.

### Hosted-service shape (one per feed, in worker-heavy)

```csharp
sealed class QueueActivityJobsFeed(CosmosClient cosmos, IServiceProvider sp) : BackgroundService
{
    private ChangeFeedProcessor? _processor;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var monitored = cosmos.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer);
        var leases = cosmos.GetContainer(DatabaseConfig.CosmosDb, /* this feed's lease container */ "leases");

        _processor = monitored
            .GetChangeFeedProcessorBuilder<Activity>("activity-feed",
                (ctx, changes, token) => HandleChangesAsync(ctx, changes, token)) // the old Run() body
            .WithInstanceName($"{Environment.MachineName}:worker-heavy") // unique per replica
            .WithLeaseContainer(leases)
            .WithMaxItems(100) // was MaxItemsPerInvocation
            .Build();
        await _processor.StartAsync();
        await Task.Delay(Timeout.Infinite, ct); // run until host shutdown
    }

    public override async Task StopAsync(CancellationToken ct)
    {
        if (_processor is not null) await _processor.StopAsync(); // releases leases cleanly
        await base.StopAsync(ct);
    }
}
```

Mapping: trigger `containerName`/`databaseName` → builder on the monitored
container; lease container (the one `CreateLeaseContainerIfNotExists` created)
→ `.WithLeaseContainer(...)`; `MaxItemsPerInvocation` → `.WithMaxItems(n)`;
instance name must be unique per process/replica. Checkpointing is automatic
after a successful batch; a thrown handler leaves the lease unadvanced and
retries the batch — handlers must stay idempotent (identical to today's
Functions semantics). Multiple replicas load-balance partitions across
themselves — the same competing-consumer behavior the compose file documents
for queues.

### Cutover strategies (per feed)

| | A. Lease takeover | B. Fresh prefix + overlap |
|---|---|---|
| How | New processor on the **same lease container and same prefix** the extension used | New processor with a **new prefix** (independent lease set) |
| Continuation | Preserved: Azure stops renewing → leases expire (defaults: renew 17s, expire 60s) → home acquires them and resumes from the stored continuation | Fresh start via `.WithStartTime(azureTriggerDisabledAt - 5 min)` — **overlap, never gap** |
| Prerequisite | Read the extension's prefix from the lease doc IDs in Data Explorer (IDs like `stream 42` — the shared leading part); pass via `.WithLeaseConfiguration(leasePrefix)` | None — always works |
| Risk | SDK lease-format drift between the extension's embedded processor and ours (checkable in a minute: does the processor acquire the existing lease docs, or create new ones alongside?) | Duplicated changes during the overlap window → idempotent handlers absorb them |

**Try A first, fall back to B.** A is a two-minute experiment against the real
lease containers and gives a zero-gap cutover.

> **Never** run Azure and home processors on the same monitored container with
> **different** prefixes: that is two independent lease sets processing the
> full stream twice — duplicates, not load balancing. Coexistence only shares
> work when prefix **and** lease container match.

Per feed:

- `QueueActivityJobs` (activities → 3-queue fan-out): a gap means an activity
  never gets its calculate jobs — takeover or overlap matters; duplicates are
  harmless (jobs re-run, results overwrite).
- `OsmFeaturesChangeTrigger`: backfill safety nets already exist
  (`ReEnrichAdminBoundaries`, force-job paths) — lowest stakes.
- `NewSummitedPeak`: currently a stub (`Backend/NewSummitedPeak.cs:24` returns
  `[]`) — convert for shape parity or omit until SignalR work resumes; nothing
  observable changes.

---

## Phases

Each phase deploys and rolls back on its own. Keep both Azure Function Apps
deployed and functional until P3.

### P0 — Foundations (no behavior change)

1. Add `redis` to `deploy/docker-compose.yml`:
   `--appendonly yes`, `--maxmemory-policy noeviction`.
   ⚠️ `Shared/MIGRATION_PLAN_SELF_HOSTED.md:65-67` warns the eventual Redis
   must **not** use `allkeys-lru` when durable — this is that moment.
2. Public exposure decided: **Tailscale Funnel** (see [decision](#decision-tailscale-funnel-2026-11-16)).
   Tailnet prep: MagicDNS + HTTPS certificates enabled, `funnel` node attribute
   added to the tailnet policy — scoped to the server node, not
   `autogroup:member`.
3. Baseline: capture per-queue depth, Cosmos RU/s, and endpoint latencies for
   before/after comparison.
4. Empty-state check: Hangfire storage schema initializes on first run.

**Verify:** Redis survives `docker compose restart`; baseline metrics captured.
**Rollback:** remove the service. Nothing depends on it yet.

### P1a — Workers become plain hosts (Service Bus stays)

1. Create `Backend.Host` (or a role argument on `Backend`): a Generic Host that
   starts, per role, the following — **no Functions packages**:
   - Hangfire servers, one per queue, `WorkerCount = 1`, on Redis;
   - `ChangeFeedProcessor` per change feed (SDK, fresh lease prefix);
   - Hangfire recurring jobs with the 12 cron strings.
2. Consumers first use the **`ServiceBusProcessor`** SDK path (P1a) — Hangfire
   arrives for timers only in this phase. Queue jobs stay on Service Bus.
3. Worker takeover, per the handshake already documented in
   `deploy/docker-compose.yml:139-143`: enable `COMPOSE_PROFILES=workers`,
   disable the matching Azure worker functions. Take over one queue group at a
   time (change feeds: cutover per the [change-feed recipe](#change-feed-conversion)).
4. Replace `scripts/verify-function-coverage.sh` with a role-coverage gate
   (every queue/feed/cron mapped to exactly one container role).

**Verify:** each taken-over queue drains; change-feed cutover is gap-free
(strategy A) or a bounded overlap (strategy B, measure per feed); Cosmos RU
within baseline.
**Rollback:** re-enable the Azure functions; stop the home role. Lease
containers: the Azure triggers resume from their own leases.

### P1b — HTTP becomes ASP.NET Core; production traffic enters the home server

1. Convert `api` (59 endpoints) and the 14 Backend HTTP functions to ASP.NET
   Core minimal APIs in one merged host — route-identical; the Backend-only
   services are registered in `API/Program.cs`.
2. `ConnectSignalR` → standard negotiate endpoint via
   `AddSignalR().AddAzureSignalR()` (Azure SignalR stays; Trailscope's flow
   unchanged if the response shape is kept).
3. Public exposure via Tailscale Funnel: `tailscale funnel` on the host (or a
   tailscale sidecar on the compose network) proxying 443 to the api container.
   Public URL becomes `https://<machine>.<tailnet>.ts.net`. Consequences handled
   in this phase:
   - Trailscope switches its API base URL; MCP clients likewise
   - Strava webhook subscription callback URL updated (same route path)
   - Session cookies reset once (hostname change → one re-login per user)
   - Measure tile-endpoint bandwidth through the funnel relays — caps are
     unpublished and non-configurable; API JSON and webhooks are safe, the
     MVT/PMTiles endpoints are the workload to watch
   - Optionally require auth on requests carrying `Tailscale-Funnel-Request: ?1`
     (Tailscale strips spoofed copies of this header)
4. OpenAPI: regenerate from ASP.NET Core and diff against the current contract
   using the existing `openapi:sync` tooling (`package.json:2-7`) — the Trailscope
   frontend and bruno collection consume this contract.
5. No DNS cutover: the old `peakshunters.erikmagnusson.com/api` URL stays on the
   Azure Function App until P3; consumers move to the funnel URL per step 3.
6. Unify the webhook path from Newtonsoft to System.Text.Json while touching it
   (the dual-JSON-stack debt flagged in ADR-0001's context).

**Verify:** OpenAPI diff is empty (or explicitly approved); Trailscope smoke
tests pass against the home API; webhook receives a Strava test event; session
auth works end to end.
**Rollback:** DNS/proxy points back at the Azure API Function App (still
deployed, still working).

### P2 — Service Bus → Hangfire on Redis (per queue)

Once production traffic (and therefore all producers) runs in home containers:

1. Per queue from the [inventory](#service-bus-queues-12--cutover-units),
   in one deploy: swap every producer call site
   (`SendMessageAsync` → `IBackgroundJobClient.Enqueue`) and the consumer
   (Service Bus processor → Hangfire job class). Job bodies are unchanged —
   only the envelope changes.
2. Start with one low-stakes queue (e.g. `accountDeleteJobs`), then batches.
3. Drain each queue before flipping it; disable dead-letter checks after the
   soak period.
4. Delete `ServiceBusRescheduler` and the Service Bus wiring from
   `Program.cs` of all hosts.

**Verify:** per-queue: no orphaned messages (queue depth zero), Hangfire
dashboard shows retries working (force one failure), Cosmos RU within baseline.
**Rollback:** flip the queue back to Service Bus producer+consumer in one
deploy (both code paths can coexist behind a per-queue feature flag during P2).

### P3 — Decommission

1. Stop and delete both Azure Function Apps and the Service Bus namespace
   (export/peek-lock any dead-lettered messages first).
2. Remove Azure Functions and Service Bus packages from all `.csproj`; delete
   `host.json`, `local.settings.json` references, `x-functions-env`, and
   `AzureFunctionsJobHost__functions__N` plumbing.
3. Switch images to plain `aspnet:10.0` runtime images (smaller, faster start).
4. Azure footprint after this plan: **Cosmos DB, Blob, Tables, Azure SignalR**
   — exactly the ADR-0001 boundary.

**Verify:** nothing references Service Bus or Functions packages
(`grep -r "Azure.Functions\|Azure.Messaging.ServiceBus"`); stack cold-starts
clean; deploy pipeline green.

---

## Decision: Tailscale Funnel (2026-11-16)

**Public exposure of the home server (P1b gate) — decided: Tailscale Funnel.**

Why: no open inbound ports, the home IP is never published (public DNS resolves
to Tailscale's relays, not the server), and **TLS terminates on our own node** —
the relays pass ciphertext they cannot decrypt (verifiable via Certificate
Transparency logs). That matches Caddy's confidentiality with a strictly
smaller external attack surface, and beats Cloudflare Tunnel, which terminates
TLS in plaintext at its edge. It also reuses infrastructure already in place —
CI already joins the tailnet.

Options considered:

| Option | Pros | Cons |
|--------|------|------|
| **Tailscale Funnel** ✅ | Zero port-forwarding; TLS handled; reuses the existing tailnet (CI already joins it); preserves the "no public ports" posture | Traffic relays through Tailscale; bandwidth/latency overhead; another Tailscale dependency |
| Caddy + DNS | Standard, full control, no third-party relay | Requires port-forward 80/443 + dynamic DNS (residential IP) or DNS challenge; Caddy config to own; home IP exposed |
| Cloudflare Tunnel | No port-forward, CDN features, keeps a custom domain | Terminates TLS in plaintext at Cloudflare's edge — they can read all traffic; third-party cloud dependency cuts against the leaving-Azure direction |

Accepted trade-offs: the public URL moves to `<machine>.<tailnet>.ts.net`
(custom domains are not supported — a CNAME breaks on the certificate;
tailscale/tailscale#11563); relay bandwidth caps are unpublished and
non-configurable (fine for API JSON and webhooks — tile traffic must be
measured); no WAF — session-cookie auth remains the only gate, as it is behind
Azure today; the feature is labelled beta.

---

## Out of scope

- Redis **caching** (rest of migration-plan Phase 4) — only queue/timer storage lands here
- Cosmos → PostgreSQL (migration-plan Phase 5+)
- Self-hosted SignalR replacing Azure SignalR (needs a Redis backplane + Trailscope client change)
- Blob/Tables exit
- `PmtilesJob` Rust pilot — separate spec, per ADR-0001

## Implementation log

- 2026-11-17 — **Service Bus SDK removed.** All 12 queues now produce and
  consume through Hangfire unconditionally; the `HANGFIRE_QUEUES` flag and its
  dual transport are gone. SDK types were replaced by SDK-free look-alikes in
  `Shared.Hosting/QueueAbstractions.cs` (`QueueMessage` keeps a `BinaryData`
  body so worker deserialization is unchanged). `QueueClient.CreateSender`
  enqueues `QueueJobDispatcher` jobs; delayed sends use
  `IBackgroundJobClient.Schedule`. The SDK processor consumer and
  `ServiceBusAdministrationClient` injections were deleted; `GetAdminStatus`
  now reports Hangfire enqueued/scheduled/failed counts. Removed the SDK package
  reference, `ServiceBusConnection` config, and the plaintext keys that were
  committed in three `local.settings.json` files. Build, full suite (382 passed,
  one known pre-existing scraper failure), role gate, and new transport tests
  are green.

## Checklist

- [ ] P0: Redis durable (AOF, `noeviction`) in compose; exposure decided — Tailscale Funnel
- [ ] P1a: all 12 queues, 3 feeds, 12 crons have a plain-host home; role-coverage gate green
- [ ] P1a: change-feed cutover gaps measured and bounded
- [ ] P1b: OpenAPI diff empty vs current contract; webhook verified end to end
- [ ] P1b: production traffic served from home; Azure API app idle
- [x] P2: all 12 queues on Hangfire; no Service Bus producer or consumer in code (2026-11-17)
- [ ] P2: Cosmos RU/s within P0 baseline (or batcher jobs added where not) — OPERATIONAL
- [x] P3: no Service Bus SDK references remain (2026-11-17); images are plain runtime images
- [ ] ADR-0001's execution sequence updated to point at this spec

## References

- ADR-0001: `docs/adr/0001-staged-azure-exit-in-csharp-with-rust-pilot.md`
- Self-hosted migration plan: `Shared/MIGRATION_PLAN_SELF_HOSTED.md`
- Current deploy: `deploy/docker-compose.yml`, `deploy/DEPLOY.md`
- Queue inventory: `Shared/Constants/QueueConfig.cs`, `Backend/host.json`
- Hangfire Redis storage: `Hangfire.Redis.StackExchange` (MIT)
- 2026 C# background-jobs landscape (Hangfire vs Quartz vs hosted services):
  research summarized in ADR-0001 references
