using Hangfire;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Workers;

/// <summary>
/// Static helpers retained from the old OSM-features Cosmos change-feed trigger. The trigger
/// handler itself was removed: PostgreSQL has no change-feed equivalent, so
/// <see cref="AdminBoundaryMetricsPollingJob"/> polls stale admin boundaries instead.
/// </summary>
public static class OsmFeaturesChangeTrigger
{
    internal static bool ShouldTriggerEnrichment(StoredFeature document)
    {
        if (document.Kind is not (FeatureKinds.Peak or FeatureKinds.ProtectedArea))
            return false;

        if (document.Id.StartsWith("empty-", StringComparison.Ordinal))
            return false;

        if (StoredFeature.IsPointerDocument(document))
            return false;

        return document.Centroid != null;
    }
}

/// <summary>
/// Hangfire recurring polling job that replaces the OSM-features Cosmos change feed. Polls
/// <c>admin_boundaries</c> for rows whose <c>metrics_version</c> is behind the current version
/// and enqueues <c>EnrichAdminBoundaryJobs</c> messages in batches of 4000.
/// </summary>
public sealed class AdminBoundaryMetricsPollingJob(
    IBackendAdminQueries queries,
    IBackgroundJobClient jobs,
    IJobEpochService epochs,
    ILogger<AdminBoundaryMetricsPollingJob> logger) : IAdminBoundaryMetricsPollingJob
{
    public const string RecurringJobId = "poll-admin-boundary-enrichment";
    public const string CronExpression = "*/5 * * * *";
    private const int BatchSize = 4000;


    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var epoch = await epochs.GetAsync(JobEpochType.AdminBoundaryMetrics, cancellationToken);
        string? afterId = null;
        var totalQueued = 0;

        while (true)
        {
            var ids = await queries.GetStaleAdminBoundaryIds(
                epoch,
                afterId,
                BatchSize,
                cancellationToken);

            if (ids.Count == 0)
                break;

            foreach (var id in ids)
                jobs.EnqueueAdminBoundaryEnrichment(AdminBoundaryIdJob.From(id, epoch));
            await queries.MarkAdminBoundariesQueued(ids, DateTime.UtcNow, cancellationToken);
            totalQueued += ids.Count;

            logger.LogInformation(
                "Queuing {Count} admin boundary enrichment jobs from stale metrics poll (after={AfterId})",
                ids.Count, afterId ?? "<start>");

            if (ids.Count < BatchSize)
                break;

            afterId = ids[^1];
        }

        if (totalQueued == 0)
            logger.LogInformation("No stale admin boundaries found during metrics poll");
    }
}
