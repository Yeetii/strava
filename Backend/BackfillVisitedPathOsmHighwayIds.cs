using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Services.Postgres.Admin;

namespace Backend;

public class BackfillVisitedPathOsmHighwayIds(
    IBackendAdminQueries queries,
    QueueClient queueClient,
    ILogger<BackfillVisitedPathOsmHighwayIds> logger)
{
    private const int DefaultBatchSize = 200;
    private const int MaxBatchSize = 1000;
    private const int DefaultMaxPages = 1000;
    private const int MaxMaxPages = 10000;

    [Function(nameof(BackfillVisitedPathOsmHighwayIds))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "manage/visited-paths/backfill-osm-highway-ids")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(req);
        var continuationToken = req.Query["continuationToken"];

        var (fetchedDocuments, queuedActivities, nextContinuationToken) = await ProcessPageAsync(
            batchSize,
            continuationToken,
            cancellationToken);

        logger.LogInformation(
            "Visited-path OSM backfill queued {QueuedCount} activity messages from {DocumentCount} documents (batchSize={BatchSize}, hasMore={HasMore})",
            queuedActivities,
            fetchedDocuments,
            batchSize,
            !string.IsNullOrWhiteSpace(nextContinuationToken));

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            requestedBatchSize = batchSize,
            fetchedDocuments,
            queuedActivities,
            hasMore = !string.IsNullOrWhiteSpace(nextContinuationToken),
            continuationToken = nextContinuationToken
        }, cancellationToken);
        return response;
    }

    [Function("BackfillVisitedPathOsmHighwayIdsAll")]
    public async Task<HttpResponseData> RunAllPages(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "manage/visited-paths/backfill-osm-highway-ids/all")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(req);
        var maxPages = ParseMaxPages(req);
        var pageCount = 0;
        var totalDocuments = 0;
        var totalQueued = 0;
        string? continuationToken = req.Query["continuationToken"];

        do
        {
            pageCount++;
            var (fetchedDocuments, queuedActivities, nextContinuationToken) = await ProcessPageAsync(
                batchSize,
                continuationToken,
                cancellationToken);

            totalDocuments += fetchedDocuments;
            totalQueued += queuedActivities;
            continuationToken = nextContinuationToken;
        }
        while (!string.IsNullOrWhiteSpace(continuationToken) && pageCount < maxPages);

        var stoppedByLimit = !string.IsNullOrWhiteSpace(continuationToken) && pageCount >= maxPages;
        logger.LogInformation(
            "Visited-path OSM full backfill processed {PageCount} pages, {DocumentCount} documents, queued {QueuedCount} activity messages. StoppedByLimit={StoppedByLimit}",
            pageCount,
            totalDocuments,
            totalQueued,
            stoppedByLimit);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            requestedBatchSize = batchSize,
            maxPages,
            processedPages = pageCount,
            fetchedDocuments = totalDocuments,
            queuedActivities = totalQueued,
            completed = string.IsNullOrWhiteSpace(continuationToken),
            stoppedByLimit,
            continuationToken
        }, cancellationToken);
        return response;
    }

    private async Task<(int FetchedDocuments, int QueuedActivities, string? ContinuationToken)> ProcessPageAsync(
        int batchSize,
        string? continuationToken,
        CancellationToken cancellationToken)
    {
        // Deterministic keyset paging over activity_visited_paths instead of the old Cosmos
        // continuation token. The token is the last (highest) activity_id from the page.
        var activityIds = await queries.GetActivityIdsWithMissingOsmHighwayId(
            continuationToken,
            batchSize,
            cancellationToken);

        var queuedActivities = await QueueActivityCollectionJobs.QueueActivityIdsAsync(
            activityIds,
            queueClient,
            QueueConfig.CalculateVisitedPathsJobs,
            TimeSpan.Zero);

        var nextContinuationToken = activityIds.Count == batchSize ? activityIds[^1] : null;
        return (activityIds.Count, queuedActivities, nextContinuationToken);
    }

    private static int ParseBatchSize(HttpRequestData req)
    {
        if (!int.TryParse(req.Query["batchSize"], out var batchSize))
        {
            return DefaultBatchSize;
        }

        return Math.Clamp(batchSize, 1, MaxBatchSize);
    }

    private static int ParseMaxPages(HttpRequestData req)
    {
        if (!int.TryParse(req.Query["maxPages"], out var maxPages))
        {
            return DefaultMaxPages;
        }

        return Math.Clamp(maxPages, 1, MaxMaxPages);
    }
}
