---
description: Historical note — Service Bus backpressure was removed; retries now belong to Hangfire.
---

## Status: obsolete

This guidance described `ServiceBusRescheduler` and `ServiceBusCosmosRetryHelper`,
which were removed when the Azure Functions runtime and Service Bus were retired.
It is kept only for historical context and must not be used for new work.

Queues now run exclusively on Hangfire/Redis:

- Producers use `QueueClient.CreateSender(...)` from `Shared.Hosting.QueueAbstractions`;
  sends become `QueueJobDispatcher` jobs.
- `QueueJobDispatcher.Execute` applies `[AutomaticRetry(Attempts = 5, ...)]`;
  worker bodies should let exceptions propagate so Hangfire owns retries.
- `QueueMessageGuard.HasLockToken` distinguishes synthetic messages from real
  ones; settlement calls are otherwise no-ops.
- Queue names live in `Shared/Constants/QueueConfig.cs`.
