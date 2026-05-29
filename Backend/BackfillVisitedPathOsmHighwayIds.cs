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

        var queued = await QueueActivityCollectionJobs.QueueActivityIdsAsync(
            activityIds,
            _serviceBusClient,
            ServiceBusConfig.CalculateVisitedPathsJobs,
            TimeSpan.Zero);

        _logger.LogInformation(
            "Visited-path OSM backfill queued {QueuedCount} activity messages from {DocumentCount} documents (batchSize={BatchSize}, hasMore={HasMore})",
            queued,
            items.Count,
            batchSize,
            !string.IsNullOrWhiteSpace(nextContinuationToken));

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            requestedBatchSize = batchSize,
            fetchedDocuments = items.Count,
            queuedActivities = queued,
            hasMore = !string.IsNullOrWhiteSpace(nextContinuationToken),
            continuationToken = nextContinuationToken
        }, cancellationToken);
        return response;
    }

    private static int ParseBatchSize(HttpRequestData req)
    {
        if (!int.TryParse(req.Query["batchSize"], out var batchSize))
        {
            return DefaultBatchSize;
        }

        return Math.Clamp(batchSize, 1, MaxBatchSize);
    }
}
