using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;

namespace Backend;

/// <summary>
/// Static helpers retained from the old OSM-features Cosmos change-feed trigger. The trigger
/// handler itself was removed: PostgreSQL has no change-feed equivalent, so
/// <see cref="AdminBoundaryMetricsPollingJob"/> polls stale admin boundaries instead.
/// </summary>
public static class OsmFeaturesChangeTrigger
{
    /// <summary>
    /// Service Bus message application property set when enrichment was triggered by new feature
    /// data rather than a schema version change. Kept for the worker's force-enrich handling;
    /// the new polling job only enqueues plain version-stale messages.
    /// </summary>
    internal const string ForceEnrichProperty = AdminBoundaryEnrichmentRules.ForceEnrichProperty;

    internal static bool ShouldEnrich(StoredFeature document)
        => AdminBoundaryEnrichmentRules.ShouldEnrich(document);

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
/// and enqueues <c>EnrichAdminBoundaryJobs</c> messages in batches of 100.
/// </summary>
public sealed class AdminBoundaryMetricsPollingJob(
    IBackendAdminQueries queries,
    QueueClient queueClient,
    ILogger<AdminBoundaryMetricsPollingJob> logger) : IAdminBoundaryMetricsPollingJob
{
    public const string RecurringJobId = "poll-admin-boundary-enrichment";
    public const string CronExpression = "*/5 * * * *";
    private const int BatchSize = 100;

    private readonly QueueSender _sender = queueClient.CreateSender(QueueConfig.EnrichAdminBoundaryJobs);

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        string? afterId = null;
        var totalQueued = 0;

        while (true)
        {
            var ids = await queries.GetStaleAdminBoundaryIds(
                AdminBoundaryMetricsEnricher.MetricsVersion,
                afterId,
                BatchSize,
                cancellationToken);

            if (ids.Count == 0)
                break;

            // Same application properties as the old admin-boundary change-feed branch: none.
            var messages = ids.Select(id => new QueueMessage(id)).ToList();
            await _sender.SendMessagesAsync(messages, cancellationToken);
            totalQueued += messages.Count;

            logger.LogInformation(
                "Queuing {Count} admin boundary enrichment jobs from stale metrics poll (after={AfterId})",
                messages.Count, afterId ?? "<start>");

            if (ids.Count < BatchSize)
                break;

            afterId = ids[^1];
        }

        if (totalQueued == 0)
            logger.LogInformation("No stale admin boundaries found during metrics poll");
    }
}
