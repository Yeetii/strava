---
description: Guide Cosmos DB work when the 1000 RU/s budget is near capacity, including failure modes and mitigations.
---

## Scope

Use this guidance when changing Cosmos-heavy code paths in this repository, especially workers and batch jobs that can saturate the 1000 RU/s budget.

## What Breaks Near 1000 RU/s

- Cosmos returns 429 throttles when reads or writes exceed the available RU/s.
- Latency rises before hard throttling because requests queue behind the throughput limit.
- Retries amplify the load if many workers fail at once and immediately try again.
- Batch processors lose lock time while they wait on Cosmos, which can cause message re-delivery and duplicate work.
- Concurrent writers can push the account into a feedback loop: more throttles, more retries, more queue buildup.
- Large fan-out queries and full document writes burn RU faster than the budget can absorb.
- Hot partitions can be saturated even when the overall account still has headroom.

## Common Root Causes

- Full document upserts or rewrites when only a few fields changed.
- `SELECT *` queries when only a subset of fields is needed.
- High parallelism across multiple workers writing to the same container.
- Repeated count or scan queries on large containers.
- Long-running Cosmos work inside message handlers that delays lock renewal.
- Retry storms after a transient 429 or timeout.
- Uneven partition key access that concentrates traffic on a single logical partition.

## Preferred Mitigations

- Use patch operations instead of full document replacements when changing a small number of fields.
- Use projected queries instead of `SELECT *`.
- Keep worker-side concurrency low enough that Cosmos writes stay below the RU ceiling.
- Defer non-time-critical Service Bus work when active queue depth or recent Cosmos throttles indicate pressure.
- Add jittered or scheduled retry behavior instead of immediate re-enqueue loops.
- Cache expensive shared lookups so each message does not repeat the same Cosmos reads.
- Split large batches when the worker can make progress incrementally.
- Prefer point reads and key lookups over broad scans.
- Avoid hot partitions by checking the partition key design before adding new write paths.
- Use lock renewal before and after slow Cosmos phases only when the message still has a real peek-lock token.

## Repository-Specific Signals

- `Shared/Services/CollectionClient.cs` already uses a global write throttle to avoid overrunning the Cosmos budget.
- `Backend/ServiceBusCosmosRetryHelper.cs` tracks Cosmos pressure and can defer non-time-critical work.
- `API/Endpoints/Admin/GetAdminStatus.cs` exposes RU throughput and queue depth signals that are useful when diagnosing saturation.

## Worker Guidance

- In `SummitsWorker`, `VisitedPathsWorker`, `VisitedAreasWorker`, `DeleteAccountWorker`, `AdminBoundaryEnrichmentWorker`, `RaceDiscoveryWorker`, and `ScrapeRaceWorker`, prefer to back off before the first expensive Cosmos call if the system is already under load.
- If a handler catches `CosmosException` with `TooManyRequests`, record the pressure signal and reschedule rather than continuing with more Cosmos work in the same invocation.
- Keep message settlement separate from long Cosmos operations so lock expiry does not cascade into duplicate processing.
- When queue backlog is high, schedule work into the future instead of adding more pressure to the current burst.

## Practical Rule of Thumb

- If the work is time-sensitive and small, keep it immediate.
- If the work is non-time-critical, batchable, or expensive, defer it when queue depth or recent throttles show that Cosmos is near the 1000 RU/s ceiling.
- If the operation writes many documents or touches a hot partition, treat it as a candidate for throttling mitigation first, not as a candidate for more parallelism.