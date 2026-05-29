using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class BackfillVisitedPathOsmHighwayIds(
    CollectionClient<VisitedPath> visitedPathsCollection,
    ServiceBusClient serviceBusClient,
    ILogger<BackfillVisitedPathOsmHighwayIds> logger)
{
    private const int DefaultBatchSize = 200;
    private const int MaxBatchSize = 1000;
    private const int DefaultMaxPages = 1000;
    private const int MaxMaxPages = 10000;

    private readonly CollectionClient<VisitedPath> _visitedPathsCollection = visitedPathsCollection;
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private readonly ILogger<BackfillVisitedPathOsmHighwayIds> _logger = logger;

    private sealed class VisitedPathActivityProjection
    {
        public required string Id { get; init; }
        public List<string>? ActivityIds { get; init; }
    }

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

        _logger.LogInformation(
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
        _logger.LogInformation(
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

        var query = new QueryDefinition(@"
SELECT c.id, c.activityIds
FROM c
WHERE NOT IS_DEFINED(c.osmHighwayId)
   OR IS_NULL(c.osmHighwayId)
   OR c.osmHighwayId = ''");

        var (items, nextContinuationToken) = await _visitedPathsCollection.ExecuteQueryPageAsync<VisitedPathActivityProjection>(
            query,
            batchSize,
            continuationToken: continuationToken,
            cancellationToken: cancellationToken);

        var activityIds = items
            .SelectMany(item => item.ActivityIds ?? [])
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var queuedActivities = await QueueActivityCollectionJobs.QueueActivityIdsAsync(
            activityIds,
            _serviceBusClient,
            ServiceBusConfig.CalculateVisitedPathsJobs,
            TimeSpan.Zero);

        return (items.Count, queuedActivities, nextContinuationToken);
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
