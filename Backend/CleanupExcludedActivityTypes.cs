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
        var userId = req.Query["userId"];
        if (string.IsNullOrWhiteSpace(userId))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync("userId query parameter is required.", cancellationToken);
            return badRequest;
        }

        var featureParam = req.Query["feature"] ?? "all";
        var dryRun = string.Equals(req.Query["dryRun"], "true", StringComparison.OrdinalIgnoreCase);

        var includePaths = featureParam is "all" or "paths";
        var includePeaks = featureParam is "all" or "peaks";
        var includeAreas = featureParam is "all" or "areas";

        // Project id + sportType for all activities of this user (single-partition query)
        var partitionKey = new PartitionKey(userId);
        var activities = await activitiesCollection.ExecuteQueryAsync<ActivityIdProjection>(
            new QueryDefinition("SELECT c.id, c.sportType FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", userId),
            new QueryRequestOptions { PartitionKey = partitionKey },
            cancellationToken: cancellationToken);

        var excludedFromPaths = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPaths.Contains(a.SportType))
            .Select(a => a.Id)
            .ToHashSet(StringComparer.Ordinal);

        var excludedFromPeaks = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPeaks.Contains(a.SportType))
            .Select(a => a.Id)
            .ToHashSet(StringComparer.Ordinal);

        var excludedFromAreas = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromAreas.Contains(a.SportType))
            .Select(a => a.Id)
            .ToHashSet(StringComparer.Ordinal);

        int pathsDeleted = 0, pathsPatched = 0;
        int peaksDeleted = 0, peaksPatched = 0;
        int areasDeleted = 0, areasPatched = 0;

        if (includePaths && excludedFromPaths.Count > 0)
        {
            (pathsDeleted, pathsPatched) = await CleanupContainerAsync(
                visitedPathsCollection, userId, partitionKey, excludedFromPaths, dryRun, cancellationToken);
        }

        if (includePeaks && excludedFromPeaks.Count > 0)
        {
            (peaksDeleted, peaksPatched) = await CleanupContainerAsync(
                summitedPeaksCollection, userId, partitionKey, excludedFromPeaks, dryRun, cancellationToken);
        }

        if (includeAreas && excludedFromAreas.Count > 0)
        {
            (areasDeleted, areasPatched) = await CleanupContainerAsync(
                visitedAreasCollection, userId, partitionKey, excludedFromAreas, dryRun, cancellationToken);
        }

        logger.LogInformation(
            "CleanupExcludedActivityTypes for user {UserId} (dryRun={DryRun}): " +
            "paths deleted={PathsDeleted} patched={PathsPatched}; " +
            "peaks deleted={PeaksDeleted} patched={PeaksPatched}; " +
            "areas deleted={AreasDeleted} patched={AreasPatched}",
            userId, dryRun,
            pathsDeleted, pathsPatched,
            peaksDeleted, peaksPatched,
            areasDeleted, areasPatched);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            userId,
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

    private async Task<(int Deleted, int Patched)> CleanupContainerAsync<T>(
        CollectionClient<T> collection,
        string userId,
        PartitionKey partitionKey,
        HashSet<string> excludedActivityIds,
        bool dryRun,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        var projections = await collection.ExecuteQueryAsync<ActivityLinkedProjection>(
            new QueryDefinition("SELECT c.id, c.activityIds FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", userId),
            new QueryRequestOptions { PartitionKey = partitionKey },
            cancellationToken: cancellationToken);

        int deleted = 0, patched = 0;
        foreach (var proj in projections)
        {
            if (proj.ActivityIds == null || proj.ActivityIds.Count == 0)
                continue;

            var remaining = proj.ActivityIds
                .Where(id => !excludedActivityIds.Contains(id))
                .ToList();

            if (remaining.Count == proj.ActivityIds.Count)
                continue;

            if (dryRun)
            {
                if (remaining.Count == 0) deleted++;
                else patched++;
                continue;
            }

            if (remaining.Count == 0)
            {
                await collection.DeleteDocument(proj.Id, partitionKey, cancellationToken);
                deleted++;
            }
            else
            {
                await collection.PatchDocument(
                    proj.Id,
                    partitionKey,
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
        public string? SportType { get; init; }
    }

    private sealed class ActivityLinkedProjection
    {
        public required string Id { get; init; }
        public List<string>? ActivityIds { get; init; }
    }
}
