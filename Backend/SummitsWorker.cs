using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;
using System.Diagnostics;

namespace Backend
{
    public class SummitsWorker(ILogger<SummitsWorker> _logger, CollectionClient<StoredFeature> _peaksCollection, CollectionClient<SummitedPeak> _summitedPeaksCollection)
    {
        [Function("SummitsWorker")]
        public async Task Run(
            [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection")] string json,
            [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%ActivitiesContainer%",
            Connection  = "CosmosDBConnection",
            Id = "{activityId}",
            PartitionKey = "{activityId}"
            )] Shared.Models.Activity activity)
        {
            var stopwatch = Stopwatch.StartNew();
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
            {
                _logger.LogInformation("Skipping activity {activityId} since it has no start location", activity.Id);
                return;
            }
            var startLocation = new Coordinate(activity.StartLatLng[1], activity.StartLatLng[0]);
            var activityLength = (int) Math.Ceiling(activity.Distance ?? 0);
            var nearbyPeaks = await _peaksCollection.GeoSpatialFetch(startLocation, activityLength);
            stopwatch.Stop();
            _logger.LogInformation("Fetched {amnPeaks} peaks after: {time}", nearbyPeaks.Count, stopwatch.Elapsed);
            var stopwatch2 = Stopwatch.StartNew();
            var nearbyPoints = nearbyPeaks.Select(peak => (peak.Id, Coordinate.ParseGeoJsonCoordinate(peak.Geometry.Coordinates)));
            var summitedPeakIds = GeoSpatialFunctions.FindPointsIntersectingLine(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline);
            stopwatch2.Stop();
            _logger.LogInformation("Calculated peaks in {time}", stopwatch2.Elapsed);
            if (!summitedPeakIds.Any())
            {
                _logger.LogInformation("Found no peaks for activity {activityId}", activity.Id);
                return;
            }
            _logger.LogInformation("Found {amnPeaks} summited peaks for activity {activityId}", summitedPeakIds.Count(), activity.Id);

            foreach (var peakId in summitedPeakIds){
                var documentId = activity.UserId + "-" + peakId;
                var partitionKey = new PartitionKey(activity.UserId);
                var summitedPeakDocument = await _summitedPeaksCollection.GetByIdMaybe(documentId, partitionKey) 
                    ?? new SummitedPeak{
                        Id = documentId,
                        UserId = activity.UserId,
                        PeakId = peakId,
                        ActivityIds = []
                    };
                summitedPeakDocument.ActivityIds.Add(activity.Id);
                await _summitedPeaksCollection.UpsertDocument(summitedPeakDocument);
            }
        }
    }
}