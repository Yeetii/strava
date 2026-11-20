using Microsoft.AspNetCore.Mvc;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Api.Http;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Api.Endpoints.Admin;

public record QueueStatus(
    string QueueName,
    long EnqueuedCount);

public record PostgresTableStatus(
    string TableName,
    long RowCount,
    long SizeBytes);

public record AdminStatus(
    IReadOnlyList<QueueStatus> Queues,
    long ScheduledCount,
    long FailedCount,
    IReadOnlyList<PostgresTableStatus> PostgresTables,
    long? DatabaseSizeBytes,
    int? ActiveConnections,
    int NewActivitiesLast24Hours,
    int ProcessedNewActivitiesLast24Hours,
    int TotalRaceOrganizers,
    int TotalRaces);

internal sealed record ActivityWindowCountsProjection(
    int NewActivitiesLast24Hours,
    int ProcessedNewActivitiesLast24Hours);

public class GetAdminStatus(
    IAdminStatusRepository statusRepository,
    IOrganizerStore organizerStore,
    IConfiguration configuration,
    ILogger<GetAdminStatus> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/status",
            (HttpContext context, [FromServices] GetAdminStatus endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var (queues, scheduledCount, failedCount) = FetchQueueStatus();
        var tableTasks = statusRepository.TableNames.Select(FetchTableStatusAsync).ToArray();
        var databaseSizeTask = FetchDatabaseSizeBytesAsync();
        var activeConnectionsTask = FetchActiveConnectionsAsync();
        var activityWindowCountsTask = FetchActivityWindowCountsAsync();
        var raceCountsTask = organizerStore.GetCountsAsync();

        var tableStatuses = await Task.WhenAll(tableTasks);
        var databaseSizeBytes = await databaseSizeTask;
        var activeConnections = await activeConnectionsTask;
        var activityWindowCounts = await activityWindowCountsTask;
        var raceCounts = await raceCountsTask;

        var status = new AdminStatus(
            queues,
            scheduledCount,
            failedCount,
            tableStatuses,
            databaseSizeBytes,
            activeConnections,
            activityWindowCounts.NewActivitiesLast24Hours,
            activityWindowCounts.ProcessedNewActivitiesLast24Hours,
            raceCounts.OrganizerCount,
            raceCounts.AssembledRaceCount);

        return TypedResults.Ok(status);
    }

    private (IReadOnlyList<QueueStatus> Queues, long ScheduledCount, long FailedCount) FetchQueueStatus()
    {
        var configuredQueues = QueueConfig.AllQueues
            .Select(JobEnqueueExtensions.NormalizeQueue)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        try
        {
            var monitoring = JobStorage.Current?.GetMonitoringApi();
            if (monitoring is null)
                return (UnavailableQueues(configuredQueues), -1, -1);

            // Include configured queues even when they are empty, plus any queue
            // Hangfire has seen at runtime that is not in the static list.
            var enqueuedByQueue = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            foreach (var queueName in configuredQueues)
                enqueuedByQueue[queueName] = 0;

            foreach (var queue in monitoring.Queues())
                enqueuedByQueue[JobEnqueueExtensions.NormalizeQueue(queue.Name)] = queue.Length;

            var queues = enqueuedByQueue
                .Select(entry => new QueueStatus(entry.Key, entry.Value))
                .OrderBy(queue => queue.QueueName, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return (queues, monitoring.ScheduledCount(), monitoring.FailedCount());
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch Hangfire queue status");
            return (UnavailableQueues(configuredQueues), -1, -1);
        }
    }

    private static IReadOnlyList<QueueStatus> UnavailableQueues(IEnumerable<string> queueNames) =>
        queueNames
            .OrderBy(queueName => queueName, StringComparer.OrdinalIgnoreCase)
            .Select(queueName => new QueueStatus(queueName, -1))
            .ToArray();

    private async Task<PostgresTableStatus> FetchTableStatusAsync(string tableName)
    {
        try
        {
            var (rowCount, sizeBytes) = await statusRepository.GetTableStatsAsync(tableName);
            return new PostgresTableStatus(tableName, rowCount, sizeBytes);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch stats for PostgreSQL table {TableName}", tableName);
            return new PostgresTableStatus(tableName, -1, -1);
        }
    }

    private async Task<long?> FetchDatabaseSizeBytesAsync()
    {
        try
        {
            return await statusRepository.GetDatabaseSizeBytesAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch PostgreSQL database size");
            return null;
        }
    }

    private async Task<int?> FetchActiveConnectionsAsync()
    {
        try
        {
            return await statusRepository.GetActiveConnectionsAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch active PostgreSQL connection count");
            return null;
        }
    }

    private async Task<ActivityWindowCountsProjection> FetchActivityWindowCountsAsync()
    {
        try
        {
            var sinceUtc = DateTime.UtcNow.AddHours(-24);
            var counts = await statusRepository.GetActivityWindowCountsAsync(sinceUtc);
            return new ActivityWindowCountsProjection(
                counts.NewActivities,
                counts.ProcessedNewActivities);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch 24-hour activity window counts");
            return new ActivityWindowCountsProjection(-1, -1);
        }
    }
}
