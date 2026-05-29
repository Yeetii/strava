# Copilot Instructions

## Cosmos DB Performance

Documents in our Cosmos containers can be very large (multi-thousand lines). Always prefer:
- **Patch operations** (`PatchDocument` / `PatchOperation.Set` / `PatchOperation.Remove`) over full document upserts when updating a few fields.
- **Projected queries** (`SELECT c.id, c.x, c.y, c.properties.foo`) over `SELECT *` when only a subset of fields is needed.

## Bruno API Collection

All API endpoints must have a corresponding Bruno request file in the `bruno/` folder.
When adding, renaming, or removing an endpoint in the API project, update the matching `.bru` file under the appropriate subfolder (e.g. `bruno/^manage/` for admin endpoints).

## Service Bus Worker Backpressure

The backend uses a combined rescheduler in `Backend/ServiceBusCosmosRetryHelper.cs` (renamed to `ServiceBusRescheduler`) for both failed-message retries and preemptive deferrals.

- Prefer extending `ServiceBusRescheduler` instead of creating a second deferral service.
- Use `ServiceBusRescheduler.HandleRetryAsync(...)` for genuine failures that should be rescheduled or dead-lettered.
- Use `ServiceBusRescheduler.TryDeferForBackpressureAsync(...)` at the start of non-time-critical workers before Cosmos work or lock renewal.
- The non-time-critical queues are defined in `Shared/Constants/ServiceBusConfig.cs`; keep that list in sync when queues are added or renamed.
- When a worker catches a Cosmos `TooManyRequests` exception, call `ServiceBusRescheduler.RecordCosmosThrottle()` before retrying so later jobs can back off proactively.
- Preserve the existing lock-token behavior: `HasRealLockToken(...)` guards settlement calls for synthetic/admin-triggered messages.

Relevant workers that should follow this pattern are `SummitsWorker`, `VisitedPathsWorker`, `VisitedAreasWorker`, `DeleteAccountWorker`, `AdminBoundaryEnrichmentWorker`, `RaceDiscoveryWorker`, and `ScrapeRaceWorker`.

Implementation notes:
- Keep the retry-count application property on scheduled copies.
- Use active Service Bus message counts, not scheduled counts, when deciding whether to defer.
- Keep Cosmos hot-path checks lightweight; avoid Azure Monitor queries during worker execution.
