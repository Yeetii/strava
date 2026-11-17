# Remove Service Bus — Hangfire as the sole queue transport

Objective: switch all queues over to Hangfire and remove the Azure.Messaging.ServiceBus SDK from the codebase. Branch: main. Delivery: uncommitted working-tree changes (user owns commits and deploys). Test runner: `dotnet test strava.sln`.

## Specs

S1. "just switch them all over and remove servicebus sdk" — Authorized scope: all 12 queues are produced and consumed through Hangfire on Redis; the `Azure.Messaging.ServiceBus` package and every SDK type usage (`ServiceBusClient`, `ServiceBusSender`, `ServiceBusMessage`, `ServiceBusReceivedMessage`, `ServiceBusMessageActions`, `ServiceBusTriggerAttribute`) are removed from source; the `HANGFIRE_QUEUES` dual-transport flag and its Service Bus fallback are retired.

S2. Acceptance: worker bodies keep their current logic and payload shapes; queue names, message bodies, retry semantics and the per-role worker split (api / schedule / heavy) are preserved; no Service Bus connection configuration remains in deploy config or docs.

S3. Verification: `dotnet test strava.sln` passes with only the known pre-existing `RaceScrapeDiscoveryTests.DeriveEventKeyFromJob_ChoosesBestAvailableSourceUrl` failure excepted; `deploy/scripts/verify-role-coverage.sh` is green; `grep` finds no `Azure.Messaging.ServiceBus` or `ServiceBusTrigger` in source.

## Tasks

T1. S1-S3 — Map the full Service Bus surface: producers, consumers, infrastructure seam, config/docs, tests, risk hotspots (route: delegated read-only explorer `ses_e15bdf061ffesn8wXLzW3Vdt4R`; status: done). Trigger: removal spans dozens of shared files and exceeds the inline evidence budget.
T2. S1-S2 — Introduce SDK-free queue types (`QueueMessage`, `QueueMessageActions`, `QueueClient`, `QueueMessageEnvelope`, `QueueException`) and swap every producer call site (route: inline; status: done).
T3. S1-S2 — Swap every consumer to the Hangfire dispatcher, delete the SDK processor consumer, retire the flag and the `HANGFIRE_QUEUES` fallback (route: inline; status: done).
T4. S1-S2 — Remove the package reference, Service Bus connection config (including plaintext keys in three `local.settings.json` files), and deploy/doc references (route: inline; status: done).
T5. S3 — Add/adjust regression tests (`QueueTransportTests`, `PlainHostTests`, `RaceDiscoveryMessageTests`, `ScrapeRaceWorkerTests`), run full suite and role gate, verify no SDK references remain (route: inline; status: done, full suite 382 passed + 1 known pre-existing failure, role gate green).

## Log

L1 (2026-11-17): just switch them all over and remove servicebus sdk

L2 (2026-11-17): i committed, go on with replacing service bus — user declined the remote m920s cutover and chose the local code replacement instead.

L3 (2026-11-17): Kept worker bodies unchanged by renaming SDK types to SDK-free look-alikes with the same member surface (BinaryData body, MessageId, ApplicationProperties, settlement method names). Hangfire is now unconditional; QueueClient.CreateSender enqueues a QueueJobDispatcher job, with delayed sends using IBackgroundJobClient.Schedule. GetAdminStatus now reports Hangfire enqueued/scheduled/failed counts instead of Service Bus queue depths. Risk: item 3 (shared queue types used by every producer and worker) — mitigated by build, full suite, role gate and new transport tests.
