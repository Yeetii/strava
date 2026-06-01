using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class CleanupExcludedActivityTypes(
    CollectionClient<Activity> activitiesCollection,
    CollectionClient<VisitedPath> visitedPathsCollection,
    CollectionClient<VisitedArea> visitedAreasCollection,
    CollectionClient<SummitedPeak> summitedPeaksCollection,
    ILogger<CleanupExcludedActivityTypes> logger)
{
    [Function(nameof(CleanupExcludedActivityTypes))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "manage/cleanup-excluded-activity-types")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var userId = req.Query["userId"]; // optional — omit to process all users
        var featureParam = req.Query["feature"] ?? "all";
        var dryRun = string.Equals(req.Query["dryRun"], "true", StringComparison.OrdinalIgnoreCase);

        var includePaths = featureParam is "all" or "paths";
        var includePeaks = featureParam is "all" or "peaks";
        var includeAreas = featureParam is "all" or "areas";

        // Project id + userId + sportType. Scope to single partition when userId is provided.
        QueryDefinition activityQuery;
        QueryRequestOptions? activityQueryOptions;
        if (!string.IsNullOrWhiteSpace(userId))
        {
            activityQuery = new QueryDefinition("SELECT c.id, c.userId, c.sportType FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", userId);
            activityQueryOptions = new QueryRequestOptions { PartitionKey = new PartitionKey(userId) };
        }
        else
        {
            activityQuery = new QueryDefinition("SELECT c.id, c.userId, c.sportType FROM c");
            activityQueryOptions = null;
        }

        var activities = await activitiesCollection.ExecuteQueryAsync<ActivityIdProjection>(
            activityQuery, activityQueryOptions, cancellationToken: cancellationToken);

        // Build activityId → userId maps for each feature's excluded set.
        var excludedFromPaths = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPaths.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        var excludedFromPeaks = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPeaks.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        var excludedFromAreas = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromAreas.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        int pathsDeleted = 0, pathsPatched = 0;
        int peaksDeleted = 0, peaksPatched = 0;
        int areasDeleted = 0, areasPatched = 0;

        if (includePaths && excludedFromPaths.Count > 0)
        {
            (pathsDeleted, pathsPatched) = await CleanupContainerAsync(
                visitedPathsCollection, userId, excludedFromPaths, dryRun, cancellationToken);
        }

        if (includePeaks && excludedFromPeaks.Count > 0)
        {
            (peaksDeleted, peaksPatched) = await CleanupContainerAsync(
                summitedPeaksCollection, userId, excludedFromPeaks, dryRun, cancellationToken);
        }

        if (includeAreas && excludedFromAreas.Count > 0)
        {
            (areasDeleted, areasPatched) = await CleanupContainerAsync(
                visitedAreasCollection, userId, excludedFromAreas, dryRun, cancellationToken);
        }

        logger.LogInformation(
            "CleanupExcludedActivityTypes for {Scope} (dryRun={DryRun}): " +
            "paths deleted={PathsDeleted} patched={PathsPatched}; " +
            "peaks deleted={PeaksDeleted} patched={PeaksPatched}; " +
            "areas deleted={AreasDeleted} patched={AreasPatched}",
            string.IsNullOrWhiteSpace(userId) ? "all users" : $"user {userId}", dryRun,
            pathsDeleted, pathsPatched,
            peaksDeleted, peaksPatched,
            areasDeleted, areasPatched);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            userId = string.IsNullOrWhiteSpace(userId) ? "all" : userId,
            dryRun,
            excludedActivities = new
            {
                fromPaths = excludedFromPaths.Count,
                fromPeaks = excludedFromPeaks.Count,
                fromAreas = excludedFromAreas.Count,
            },
            paths = new { deleted = pathsDeleted, patched = pathsPatched },
            peaks = new { deleted = peaksDeleted, patched = peaksPatched },
            areas = new { deleted = areasDeleted, patched = areasPatched },
        }, cancellationToken);
        return response;
    }

    /// <summary>
    /// Removes contributions from excluded activities in a feature container.
    /// When <paramref name="userId"/> is provided the container query is scoped to that
    /// partition; otherwise all partitions are scanned.
    /// </summary>
    private async Task<(int Deleted, int Patched)> CleanupContainerAsync<T>(
        CollectionClient<T> collection,
        string? userId,
        Dictionary<string, string> excludedActivityIdToUserId,
        bool dryRun,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        QueryDefinition query;
        QueryRequestOptions? queryOptions;
        if (!string.IsNullOrWhiteSpace(userId))
        {
            query = new QueryDefinition("SELECT c.id, c.userId, c.activityIds FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", userId);
            queryOptions = new QueryRequestOptions { PartitionKey = new PartitionKey(userId) };
        }
        else
        {
            query = new QueryDefinition("SELECT c.id, c.userId, c.activityIds FROM c");
            queryOptions = null;
        }

        var projections = await collection.ExecuteQueryAsync<ActivityLinkedProjection>(
            query, queryOptions, cancellationToken: cancellationToken);

        int deleted = 0, patched = 0;
        foreach (var proj in projections)
        {
            if (proj.ActivityIds == null || proj.ActivityIds.Count == 0)
                continue;

            var remaining = proj.ActivityIds
                .Where(id => !excludedActivityIdToUserId.ContainsKey(id))
                .ToList();

            if (remaining.Count == proj.ActivityIds.Count)
                continue;

            var docPartitionKey = new PartitionKey(proj.UserId);

            if (dryRun)
            {
                if (remaining.Count == 0) deleted++;
                else patched++;
                continue;
            }

            if (remaining.Count == 0)
            {
                await collection.DeleteDocument(proj.Id, docPartitionKey, cancellationToken);
                deleted++;
            }
            else
            {
                await collection.PatchDocument(
                    proj.Id,
                    docPartitionKey,
                    [PatchOperation.Set("/activityIds", remaining)],
                    cancellationToken: cancellationToken);
                patched++;
            }
        }
        return (deleted, patched);
    }

    private sealed class ActivityIdProjection
    {
        public required string Id { get; init; }
        public required string UserId { get; init; }
        public string? SportType { get; init; }
    }

    private sealed class ActivityLinkedProjection
    {
        public required string Id { get; init; }
        public required string UserId { get; init; }
        public List<string>? ActivityIds { get; init; }
    }
}
