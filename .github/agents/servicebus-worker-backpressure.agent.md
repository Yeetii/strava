---
description: Guide implementation and review of the combined Service Bus rescheduler and non-time-critical worker backpressure logic.
---

## Scope

Use this agent guidance when changing the Service Bus worker pipeline that handles both failed-message retries and preemptive deferrals for Cosmos pressure or queue depth.

See also `.github/agents/cosmos-1000ru-pressure.agent.md` for the Cosmos-specific failure modes and mitigation rules that feed into the backpressure decision.

## Core Rules

- Prefer extending `Backend/ServiceBusCosmosRetryHelper.cs` rather than introducing a second deferral service.
- Use `ServiceBusRescheduler.HandleRetryAsync(...)` for genuine failures that should be retried or dead-lettered.
- Use `ServiceBusRescheduler.TryDeferForBackpressureAsync(...)` at the start of non-time-critical workers before Cosmos reads, batch queries, or lock renewal.
- Keep `Shared/Constants/ServiceBusConfig.NonTimeCriticalQueues` as the source of truth for the monitored low-priority queues.
- When a worker catches a Cosmos `TooManyRequests` exception, call `ServiceBusRescheduler.RecordCosmosThrottle()` before retrying so later jobs can back off proactively.
- Preserve `ServiceBusRescheduler.HasRealLockToken(...)` checks before completing, dead-lettering, or renewing Service Bus messages.

## Worker Guidance

- `SummitsWorker`, `VisitedPathsWorker`, `VisitedAreasWorker`, `DeleteAccountWorker`, `AdminBoundaryEnrichmentWorker`, `RaceDiscoveryWorker`, and `ScrapeRaceWorker` should all defer before expensive Cosmos work when the helper indicates backpressure.
- For batched workers, defer the whole batch if the helper returns `true`; do not partially process the batch in the same invocation.
- Keep any existing message validation or dead-lettering that must happen before deferral, such as invalid JSON checks.
- Keep `RaceDiscoveryWorker` and `ScrapeRaceWorker` aligned with their current message validation and rescheduling behavior; backpressure deferral should be additive, not a rewrite of their domain logic.
- If a worker already schedules its own retry copy, keep that behavior intact and only add the shared backpressure checks where appropriate.

## Implementation Notes

- Use active Service Bus message counts when deciding whether to defer; scheduled counts are future work and should not drive the hot-path signal.
- Keep queue-depth lookups lightweight and cached.
- Keep the retry count application property on rescheduled copies.
- Avoid Azure Monitor calls in worker hot paths.
- Maintain the existing worker lock-renewal timing; backpressure deferral should happen before any expensive Cosmos work that can make lock renewal risky.