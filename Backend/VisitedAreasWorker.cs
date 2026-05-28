using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;

namespace Backend;

public class VisitedAreasWorker(
    ILogger<VisitedAreasWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<VisitedArea> _visitedAreasCollection,
    [FromKeyedServices(FeatureKinds.ProtectedArea)] TiledCollectionClient _protectedAreasCollection,
    AdminBoundariesCollectionClient _adminBoundariesCollection,
    ServiceBusClient serviceBusClient,
    UserSyncStatusService _userSyncStatusService)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private const int AreaTileZoom = 8;
    private const int AdminLevelRegion = 4;
    private const int MaxAdminBoundaryLookupPoints = 120;

    private sealed record VisitedAreaCandidate(
        string AreaId,
        string Name,
        string AreaType,
        string? Wikidata,
        string? WikimediaCommons);

    [Function(nameof(VisitedAreasWorker))]
    public async Task Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(GetRoutePolyline(a)))
            .ToList();

        var nearbyAreas = (await FetchNearbyAreas(activitiesList)).ToList();

        var semaphore = new SemaphoreSlim(4, 4);
        var processingTasks = jobs.Select(async job =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try { await ProcessJob(job, actions, activitiesList, nearbyAreas, cancellationToken); }
            finally { semaphore.Release(); }
        });
        await Task.WhenAll(processingTasks);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        List<Activity> activitiesList,
        List<StoredFeature> nearbyAreas,
        CancellationToken cancellationToken)
    {
        var activityId = job.Body.ToString();
        var hasRealLockToken = ServiceBusCosmosRetryHelper.HasRealLockToken(job);
        try
        {
            var activity = activitiesList.FirstOrDefault(a => a.Id == activityId);

            var routePolyline = activity is null ? null : GetRoutePolyline(activity);

            if (activity == null || string.IsNullOrWhiteSpace(routePolyline))
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
                if (activity != null)
                {
                    await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
                }
                if (hasRealLockToken) await actions.CompleteMessageAsync(job);
                return;
            }

            var activityPoints = GeoSpatialFunctions.DecodePolyline(routePolyline).ToList();
            _logger.LogDebug("Activity {ActivityId} decoded {PointCount} route points", activityId, activityPoints.Count);

            var boundaryLookupPolyline = GetBoundaryLookupPolyline(activity) ?? routePolyline;
            var boundaryLookupPoints = string.Equals(boundaryLookupPolyline, routePolyline, StringComparison.Ordinal)
                ? activityPoints
                : GeoSpatialFunctions.DecodePolyline(boundaryLookupPolyline).ToList();
            _logger.LogDebug(
                "Activity {ActivityId} using {LookupPointCount} points for admin boundary lookup",
                activityId,
                boundaryLookupPoints.Count);

            var nearbyRegions = (await FetchAdminRegionsForActivity(boundaryLookupPoints, cancellationToken)).ToList();

            if (nearbyRegions.Count == 0)
            {
                _logger.LogWarning(
                    "No admin boundary found for activity {ActivityId} after lookup.",
                    activityId);
            }

            foreach (var region in nearbyRegions)
            {
                if (string.IsNullOrWhiteSpace(GetCountryIso2FromRegion(region.Properties)))
                {
                    _logger.LogDebug(
                        "No country could be derived for visited region {RegionId} on activity {ActivityId}. Expected ISO 3166-2 style properties (e.g. RO-BR).",
                        region.LogicalId,
                        activityId);
                }
            }

            var visitedAreas = FindVisitedProtectedAreas(activityPoints, nearbyAreas)
                .Concat(FindVisitedRegionAreas(nearbyRegions))
                .Concat(FindVisitedCountriesFromRegions(nearbyRegions))
                .ToList();

            var visitedCountryCount = visitedAreas.Count(a => string.Equals(a.AreaType, "country", StringComparison.Ordinal));
            _logger.LogInformation(
                "Visited areas summary for activity {ActivityId}: nearby protected={NearbyProtectedCount}, visited admin regions={VisitedRegionCount}, visited countries={VisitedCountryCount}, total visited areas={AreaCount}",
                activityId,
                nearbyAreas.Count,
                nearbyRegions.Count,
                visitedCountryCount,
                visitedAreas.Count);

            if (visitedAreas.Count == 0)
            {
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
                if (hasRealLockToken) await actions.CompleteMessageAsync(job);
                return;
            }

            var partitionKey = new PartitionKey(activity.UserId);
            var documentIds = visitedAreas
                .Select(area => activity.UserId + "-" + area.AreaId)
                .ToList();
            var existingDocs = (await _visitedAreasCollection.GetByIdsAsync(documentIds, cancellationToken))
                .ToDictionary(doc => doc.Id, StringComparer.Ordinal);

            var toCreate = new List<VisitedArea>();
            var toPatch = new List<(string Id, IReadOnlyList<PatchOperation> Operations)>();
            foreach (var area in visitedAreas)
            {
                var documentId = activity.UserId + "-" + area.AreaId;
                if (existingDocs.TryGetValue(documentId, out var existing))
                {
                    if (!existing.ActivityIds.Contains(activity.Id))
                    {
                        toPatch.Add((documentId, [
                            PatchOperation.Add("/activityIds/-", activity.Id)
                        ]));
                    }
                    continue;
                }

                toCreate.Add(new VisitedArea
                {
                    Id = documentId,
                    UserId = activity.UserId,
                    AreaId = area.AreaId,
                    Name = area.Name,
                    AreaType = area.AreaType,
                    Wikidata = area.Wikidata,
                    WikimediaCommons = area.WikimediaCommons,
                    ActivityIds = [activity.Id]
                });
            }

            if (hasRealLockToken) await actions.RenewMessageLockAsync(job);
            _logger.LogInformation(
                "Writing visited areas for activity {ActivityId}: creates={CreateCount}, patches={PatchCount}, documentIds=[{DocumentIds}]",
                activityId,
                toCreate.Count,
                toPatch.Count,
                string.Join(", ", documentIds));
            await _visitedAreasCollection.ExecuteBatch(partitionKey, creates: toCreate, patches: toPatch, cancellationToken: cancellationToken);
            _logger.LogInformation("Batch write complete for activity {ActivityId}", activityId);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
            if (hasRealLockToken) await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            if (hasRealLockToken)
            {
                await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                    ex, actions, job, _serviceBusClient, Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs, _logger, cancellationToken);
                return;
            }

            _logger.LogError(ex,
                "Failed to process visited areas for activity {ActivityId} (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                activityId,
                job.MessageId,
                job.DeliveryCount);
            return;
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedProtectedAreas(
        List<Coordinate> activityPoints,
        IEnumerable<StoredFeature> areas)
    {
        foreach (var area in areas)
        {
            if (ActivityVisitsArea(activityPoints, area.Geometry))
            {
                var areaId = area.LogicalId;
                yield return new VisitedAreaCandidate(
                    areaId,
                    GetPropertyValue(area.Properties, "name") ?? areaId,
                    GetPropertyValue(area.Properties, "areaType") ?? "protected_area",
                    GetPropertyValue(area.Properties, "wikidata"),
                    GetPropertyValue(area.Properties, "wikimedia_commons"));
            }
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedRegionAreas(IEnumerable<StoredFeatureSummary> regions)
    {
        foreach (var region in regions)
        {
            var regionId = region.LogicalId;
            yield return new VisitedAreaCandidate(
                regionId,
                GetPropertyValue(region.Properties, "name") ?? regionId,
                "region",
                GetPropertyValue(region.Properties, "wikidata"),
                GetPropertyValue(region.Properties, "wikimedia_commons"));
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedCountriesFromRegions(IEnumerable<StoredFeatureSummary> regions)
    {
        var visitedCountryIso2Codes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var region in regions)
        {
            var countryIso2 = GetCountryIso2FromRegion(region.Properties);
            if (!string.IsNullOrWhiteSpace(countryIso2))
            {
                visitedCountryIso2Codes.Add(countryIso2);
            }
        }

        foreach (var countryIso2 in visitedCountryIso2Codes.Order(StringComparer.Ordinal))
        {
            yield return new VisitedAreaCandidate(
                $"country:{countryIso2}",
                GetCountryName(countryIso2),
                "country",
                null,
                null);
        }
    }

    private static string? GetPropertyValue(IDictionary<string, dynamic> properties, string key)
    {
        if (properties.TryGetValue(key, out var value))
            return value?.ToString();

        foreach (var kvp in properties)
        {
            if (string.Equals(kvp.Key, key, StringComparison.OrdinalIgnoreCase))
                return kvp.Value?.ToString();
        }

        return null;
    }

    private static string? GetCountryIso2FromRegion(IDictionary<string, dynamic> regionProperties)
    {
        var iso3166_2 = GetPropertyValue(regionProperties, "iso3166-2")
            ?? GetPropertyValue(regionProperties, "ISO3166-2")
            ?? GetPropertyValue(regionProperties, "isO3166-2");

        if (!string.IsNullOrWhiteSpace(iso3166_2))
        {
            var separatorIndex = iso3166_2.IndexOf('-');
            var countryPart = separatorIndex > 0
                ? iso3166_2[..separatorIndex]
                : iso3166_2;

            var normalized = countryPart.Trim().ToUpperInvariant();
            if (normalized.Length == 2)
                return normalized;
        }

        var countryCode = GetPropertyValue(regionProperties, "countryCode");
        if (!string.IsNullOrWhiteSpace(countryCode))
        {
            var normalized = countryCode.Trim().ToUpperInvariant();
            if (normalized.Length == 2)
                return normalized;
        }

        return null;
    }

    private static string GetCountryName(string iso2)
    {
        try
        {
            return new RegionInfo(iso2).EnglishName;
        }
        catch (ArgumentException)
        {
            // Fallback for non-standard or unknown country codes.
            return iso2;
        }
    }

    private static bool ActivityVisitsArea(List<Coordinate> activityPoints, Geometry geometry)
    {
        foreach (var idx in GetSampledIndices(activityPoints.Count))
        {
            if (RouteFeatureMatcher.IsPointInGeometry(activityPoints[idx], geometry))
                return true;
        }
        return false;
    }

    private static IEnumerable<int> GetSampledIndices(int count)
    {
        const int step = 10;
        var visited = new HashSet<int>();

        for (int i = 0; i < count; i += step)
        {
            visited.Add(i);
            yield return i;
        }

        for (int i = 5; i < count; i += step)
        {
            if (visited.Add(i))
                yield return i;
        }

        for (int i = 0; i < count; i++)
        {
            if (visited.Add(i))
                yield return i;
        }
    }

    private async Task<IEnumerable<StoredFeature>> FetchNearbyAreas(IEnumerable<Activity> activities)
    {
        var areaTileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline ?? activity.Polyline;
            if (string.IsNullOrEmpty(polyline))
                continue;

            var points = GeoSpatialFunctions.DecodePolyline(polyline);
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, AreaTileZoom))
                areaTileIndices.Add(tile);
        }

        var areas = (await _protectedAreasCollection.FetchByTiles(areaTileIndices, AreaTileZoom, followPointers: true)).ToList();
        _logger.LogDebug("Found {Count} nearby protected areas", areas.Count);
        return areas;
    }

    private async Task<IEnumerable<StoredFeatureSummary>> FetchAdminRegionsForActivity(
        List<Coordinate> activityPoints,
        CancellationToken cancellationToken)
    {
        if (activityPoints.Count == 0)
            return [];

        var coordinates = SampleCoordinatesForAdminBoundaryLookup(activityPoints)
            .DistinctBy(p => (p.Lat, p.Lng));

        var summaries = (await _adminBoundariesCollection.FindBoundarySummariesContainingAnyPoint(
            coordinates, AdminLevelRegion, cancellationToken)).ToList();

        if (summaries.Count == 0)
        {
            _logger.LogDebug("Found 0 admin region candidates via ST_WITHIN");
            return [];
        }

        if (summaries.Count == 1)
        {
            _logger.LogDebug("Found a single admin region candidate via ST_WITHIN; skipping local geometry validation and full boundary fetch.");
            return summaries;
        }

        var boundaryDocsById = (await _adminBoundariesCollection.GetByIdsAsync(
                summaries.Select(summary => summary.Id),
                cancellationToken))
            .ToDictionary(doc => doc.Id, StringComparer.Ordinal);

        var validatedSummaries = summaries
            .Select(summary => boundaryDocsById.TryGetValue(summary.Id, out var boundaryDoc)
                && ActivityIntersectsBoundary(activityPoints, boundaryDoc.Geometry)
                    ? MergeSummaryWithBoundaryDoc(summary, boundaryDoc)
                    : null)
            .Where(summary => summary is not null)
            .Select(summary => summary!)
            .ToList();

        if (validatedSummaries.Count == 0)
        {
            var mergedCandidates = summaries
                .Select(summary => boundaryDocsById.TryGetValue(summary.Id, out var boundaryDoc)
                    ? MergeSummaryWithBoundaryDoc(summary, boundaryDoc)
                    : summary)
                .ToList();

            _logger.LogWarning(
                "All {CandidateCount} admin region candidates for this activity were rejected by local geometry validation; falling back to ST_WITHIN candidates.",
                summaries.Count);
            return mergedCandidates;
        }

        _logger.LogDebug(
            "Found {CandidateCount} admin region candidates via ST_WITHIN; {ValidatedCount} remained after local geometry validation",
            summaries.Count,
            validatedSummaries.Count);
        return validatedSummaries;
    }

    private static StoredFeatureSummary MergeSummaryWithBoundaryDoc(
        StoredFeatureSummary summary,
        StoredFeature boundaryDoc)
    {
        var mergedProperties = new Dictionary<string, dynamic>(summary.Properties, StringComparer.Ordinal);
        foreach (var (key, value) in boundaryDoc.Properties)
            mergedProperties[key] = value;

        return new StoredFeatureSummary
        {
            Id = summary.Id,
            FeatureId = summary.FeatureId,
            Kind = summary.Kind,
            Properties = mergedProperties
        };
    }

    private static bool ActivityIntersectsBoundary(
        IEnumerable<Coordinate> activityPoints,
        Geometry geometry)
    {
        foreach (var point in activityPoints)
        {
            if (RouteFeatureMatcher.IsPointInGeometry(point, geometry))
                return true;
        }

        return false;
    }

    private static IEnumerable<Coordinate> SampleCoordinatesForAdminBoundaryLookup(List<Coordinate> activityPoints)
    {
        if (activityPoints.Count <= MaxAdminBoundaryLookupPoints)
            return activityPoints;

        var step = (int)Math.Ceiling((double)activityPoints.Count / MaxAdminBoundaryLookupPoints);
        var sampled = activityPoints
            .Where((_, i) => i % step == 0)
            .ToList();

        // Ensure the end of the route is represented even when it does not align with the step.
        var last = activityPoints[^1];
        if (sampled.Count == 0 || sampled[^1] != last)
            sampled.Add(last);

        return sampled;
    }

    private static string? GetRoutePolyline(Activity activity)
    {
        if (!string.IsNullOrWhiteSpace(activity.Polyline))
            return activity.Polyline;

        if (!string.IsNullOrWhiteSpace(activity.SummaryPolyline))
            return activity.SummaryPolyline;

        return null;
    }

    private static string? GetBoundaryLookupPolyline(Activity activity)
    {
        if (!string.IsNullOrWhiteSpace(activity.SummaryPolyline))
            return activity.SummaryPolyline;

        return GetRoutePolyline(activity);
    }
}
