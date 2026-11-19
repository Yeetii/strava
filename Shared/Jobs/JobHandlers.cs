using System.Linq.Expressions;
using Hangfire;
using Shared.Constants;

namespace Shared.Jobs;

/// <summary>
/// Native Hangfire job contracts. Producers enqueue a typed interface method;
/// the Workers host registers the single implementation. The contract lives in
/// the core assembly so every host sharing the Hangfire store can deserialize
/// a queued job without loading another host's assembly.
/// </summary>
public interface IScrapeRaceJob
{
    Task ExecuteAsync(ScrapeRaceMessage job, CancellationToken cancellationToken);
}

public interface IRaceDiscoveryJob
{
    Task ExecuteAsync(RaceDiscoveryMessage job, CancellationToken cancellationToken);
}

public interface IManualMistralScrapeJob
{
    Task ExecuteAsync(ManualMistralScrapeJob job, CancellationToken cancellationToken);
}

public interface IAdminBoundaryEnrichmentJob
{
    Task ExecuteAsync(AdminBoundaryIdJob job, CancellationToken cancellationToken);
}

public interface IActivityFetchJob
{
    Task ExecuteAsync(ActivityFetchJob job, CancellationToken cancellationToken);
}

public interface IActivitiesFetchJob
{
    Task ExecuteAsync(ActivitiesFetchJob job, CancellationToken cancellationToken);
}

public interface IActivityDeleteJob
{
    Task ExecuteAsync(ActivityDeleteJob job, CancellationToken cancellationToken);
}

public interface IAccountDeleteJob
{
    Task ExecuteAsync(AccountDeleteJob job, CancellationToken cancellationToken);
}

public interface ISummitsJob
{
    Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken);
}

public interface IVisitedPathsJob
{
    Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken);
}

public interface IVisitedAreasJob
{
    Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken);
}

/// <summary>Typed enqueue helpers so no producer hand-builds a transport envelope.</summary>
public static class JobEnqueueExtensions
{
    private static string Enqueue<TJob>(IBackgroundJobClient jobs, string queue, Expression<Func<TJob, Task>> call)
        where TJob : class => jobs.Enqueue(queue, call);
    private static string Schedule<TJob>(IBackgroundJobClient jobs, string queue, Expression<Func<TJob, Task>> call, TimeSpan delay)
        where TJob : class => jobs.Schedule(queue, call, delay);

    public static string EnqueueScrapeRace(this IBackgroundJobClient jobs, ScrapeRaceMessage job) =>
        Enqueue<IScrapeRaceJob>(jobs, QueueConfig.ScrapeRace, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueRaceDiscovery(this IBackgroundJobClient jobs, RaceDiscoveryMessage job, TimeSpan? delay = null) =>
        delay.HasValue
            ? Schedule<IRaceDiscoveryJob>(jobs, QueueConfig.RaceDiscoveryJobs, x => x.ExecuteAsync(job, CancellationToken.None), delay.Value)
            : Enqueue<IRaceDiscoveryJob>(jobs, QueueConfig.RaceDiscoveryJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueManualMistralScrape(this IBackgroundJobClient jobs, ManualMistralScrapeJob job) =>
        Enqueue<IManualMistralScrapeJob>(jobs, QueueConfig.MistralScrapeJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueAdminBoundaryEnrichment(this IBackgroundJobClient jobs, AdminBoundaryIdJob job) =>
        Enqueue<IAdminBoundaryEnrichmentJob>(jobs, QueueConfig.EnrichAdminBoundaryJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueActivityFetch(this IBackgroundJobClient jobs, ActivityFetchJob job) =>
        Enqueue<IActivityFetchJob>(jobs, QueueConfig.ActivityFetchJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueActivitiesFetch(this IBackgroundJobClient jobs, ActivitiesFetchJob job) =>
        Enqueue<IActivitiesFetchJob>(jobs, QueueConfig.ActivitiesFetchJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueActivityDelete(this IBackgroundJobClient jobs, ActivityDeleteJob job) =>
        Enqueue<IActivityDeleteJob>(jobs, QueueConfig.ActivityDeleteJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueAccountDelete(this IBackgroundJobClient jobs, AccountDeleteJob job) =>
        Enqueue<IAccountDeleteJob>(jobs, QueueConfig.AccountDeleteJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueSummits(this IBackgroundJobClient jobs, ActivityIdJob job) =>
        Enqueue<ISummitsJob>(jobs, QueueConfig.CalculateSummitsJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueVisitedPaths(this IBackgroundJobClient jobs, ActivityIdJob job) =>
        Enqueue<IVisitedPathsJob>(jobs, QueueConfig.CalculateVisitedPathsJobs, x => x.ExecuteAsync(job, CancellationToken.None));

    public static string EnqueueVisitedAreas(this IBackgroundJobClient jobs, ActivityIdJob job) =>
        Enqueue<IVisitedAreasJob>(jobs, QueueConfig.CalculateVisitedAreasJobs, x => x.ExecuteAsync(job, CancellationToken.None));
}
