# Hangfire.Console for all workers
Objective: give every Hangfire job a Console tab and forward each worker's existing ILogger output into it, so job results (e.g. summit counts) are visible in the dashboard. Branch: main. Delivery: uncommitted working-tree changes (user owns commits and deploys). Test runner: `dotnet test strava.sln`.

## Specs

S1. "introduce the hanfire.console for all workers" — Authorized scope: `Hangfire.Console` 1.4.3 is referenced by Shared.Hosting, enabled once per process from `QueueTransport.AddHangfireStorage`. Every Hangfire job target (all 11 queue workers via `QueueJobDispatcher.Execute`, all timer jobs via `ITimerJobDispatcher.RunAsync`, and the race PMTiles job via `IRaceTilesBuildJob.RunAsync`) gets a Console tab in the dashboard. No worker method signatures, enqueue expressions, or persisted job payloads change.

S2. "for calculateSummitsJobs i'd like to log the amount of summits that were found" — The existing `ILogger.LogInformation("Activity {ActivityId} has {SummitCount} summits", ...)` in `Backend/SummitsWorker.cs` must appear in that job's Console tab without editing the worker. All other workers' Information+ log lines appear in their own jobs' tabs the same way.

S3. Verification: `dotnet build strava.sln` is green; `dotnet test strava.sln` is green with new tests pinning the logger bridge and the filter attribute on every job target. `UseConsole()` is guarded so repeated registration cannot throw "Console is already initialized".

## Tasks

T1. S1 — Add the Hangfire.Console package, enable `UseConsole()` behind a process-wide guard, register the console logger provider (route: inline; status: done).
T2. S1-S2 — Add the ambient job console writer + server filter attribute, apply it to all five job targets (queue, timer, race PMTiles, two polling jobs), forward Information+ ILogger output (route: inline; status: done).
T3. S2-S3 — Add tests, run focused test and full suite, report results (route: inline; status: done).

## Log

L1 (2026-11-18): "introduce the hanfire.console for all workers"

L2 (2026-11-18): Prior context from this session: the user first asked "is it possible to log some output on the hangfire messages after completion? like for calculateSummitsJobs i'd like to log the amount of summits that were found." Investigation showed `SummitsWorker.cs:100` already logs the count with `ILogger`, but core Hangfire never surfaces `ILogger` output in the dashboard; the Hangfire job is `QueueJobDispatcher.Execute` and the worker is invoked reflectively without a `PerformContext`. Two routes were offered (Hangfire.Console package vs. job parameters); the user chose Hangfire.Console for all workers.

L3 (2026-11-18): Design decision — use a method-level `IServerFilter` attribute instead of adding `PerformContext` as a job parameter. Reason: adding a parameter changes the stored Hangfire job signature and would break jobs already queued in Redis at deploy time. The filter stores an `IDisposable` scope in `PerformingContext.Items` (shared with `PerformedContext` via Hangfire's copy constructor), so per-job state is thread-safe and no DI singleton holds mutable state. An `AsyncLocal<IJobConsoleWriter>` carries the writer through the worker's `Parallel.ForEachAsync` child tasks.

L4 (2026-11-18): `Hangfire.Console.UseConsole()` throws if called twice in one process; `AddHangfireStorage` is reachable from several host entry points, so initialization is guarded with `Interlocked.Exchange`. The package is netstandard1.3 / Hangfire.Core >= 1.6.0, compatible with net10.0 and the repo's Hangfire.Core 1.8.21.

L5 (2026-11-18): Implementation complete. Files: `Shared.Hosting/JobConsole.cs` (new: `IJobConsoleWriter`, `JobConsole` AsyncLocal, `HangfireConsoleLoggerProvider`, `JobConsoleServerFilterAttribute`), `Shared.Hosting/HangfireQueueTransport.cs` (UseConsole guard + provider registration + attribute on `Execute`), `Shared.Hosting/FunctionWorkerHost.cs`, `Shared.Hosting/RaceTilesBuildJob.cs`, `Shared.Hosting/PollingJobContracts.cs` (attributes on the four interface job targets), `Shared.Hosting/Shared.Hosting.csproj` (Hangfire.Console 1.4.3), `Backend.Tests/HangfireConsoleLoggingTests.cs` (new, 9 tests). Five job targets covered: `QueueJobDispatcher.Execute` (all 11 queue workers), `ITimerJobDispatcher.RunAsync` (12 crons), `IRaceTilesBuildJob.RunAsync`, `IAdminBoundaryMetricsPollingJob.RunAsync`, `IQueueActivityJobs.PollAndQueueAsync`.

L6 (2026-11-18): Verification — RED observed first (new tests failed to compile: missing types). After implementation: focused tests 9/9 green, including a real `BackgroundJobPerformer` run proving the filter opens the writer around the job and a DI test proving worker `ILogger<T>` output is forwarded. `dotnet build strava.sln` → 0 errors, 14 warnings (pre-existing). `dotnet test strava.sln --no-build` → PmtilesJob.Tests 32/32, Backend.Tests 384/384, Shared.Tests 267/267 (683 total). No signature or persisted job payload changes, so already-queued jobs keep working. No commits, pushes or deploys made.

L7 (2026-11-18): Independent verifier found one blocker, now corrected. `PmtilesJob/Program.cs` calls `logging.ClearProviders()` in `ConfigureLogging`, which runs after `ConfigureServices` and removed the bridge registered by `AddHangfireStorage`; the race PMTiles job's log lines would never reach its console. Fix: registration changed to `TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, HangfireConsoleLoggerProvider>())`, plus a new `JobConsoleLoggingExtensions.AddJobConsoleLogging(this ILoggingBuilder)` called explicitly after `ClearProviders()` in `PmtilesJob/Program.cs`. `TryAddEnumerable` also prevents duplicate providers in hosts that do not clear. Verifier's regression probe added as a test and extended to assert exactly one provider survives.

L8 (2026-11-18): Verifier recheck session was cancelled by the runtime; the single bounded recheck was completed inline instead of relaunching (no loop). Final verification: focused tests 10/10, `dotnet build strava.sln` 0 errors, full suite PmtilesJob.Tests 32/32 + Backend.Tests 385/385 + Shared.Tests 267/267 = 684 total, 0 failed. S1 PASS, S2 PASS (after correction), S3 PASS. Not verified: live end-to-end rendering into a real Redis/dashboard Console tab (no Redis available); mechanism confirmed from Hangfire 1.8.21 and Hangfire.Console 1.4.3 source.
