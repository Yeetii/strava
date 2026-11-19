using Microsoft.AspNetCore.Mvc;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Api.Http;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Api.Endpoints.Admin;

public record QueueStatus(
    string QueueName,
    long EnqueuedCount,
    long ScheduledCount,
    long FailedCount);

public record PostgresTableStatus(
    string TableName,
    long RowCount);

public record AdminStatus(
    IReadOnlyList<QueueStatus> Queues,
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

        var queueStatus = FetchQueueStatus();
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
            [queueStatus],
            tableStatuses,
            databaseSizeBytes,
            activeConnections,
            activityWindowCounts.NewActivitiesLast24Hours,
            activityWindowCounts.ProcessedNewActivitiesLast24Hours,
            raceCounts.OrganizerCount,
            raceCounts.AssembledRaceCount);

        return TypedResults.Ok(status);
    }

    private QueueStatus FetchQueueStatus()
    {
        try
        {
            var monitoring = JobStorage.Current?.GetMonitoringApi();
            if (monitoring is null)
                return new QueueStatus("hangfire", -1, -1, -1);
            return new QueueStatus(
                "hangfire",
                monitoring.EnqueuedCount("default"),
                monitoring.ScheduledCount(),
                monitoring.FailedCount());
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch Hangfire queue status");
            return new QueueStatus("hangfire", -1, -1, -1);
        }
    }

    private async Task<PostgresTableStatus> FetchTableStatusAsync(string tableName)
    {
        try
        {
            var rowCount = await statusRepository.GetTableRowCountAsync(tableName);
            return new PostgresTableStatus(tableName, rowCount);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch row count for PostgreSQL table {TableName}", tableName);
            return new PostgresTableStatus(tableName, -1);
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
