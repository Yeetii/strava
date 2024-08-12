using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;

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
            )] Activity activity)
        {
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
                throw new ArgumentNullException("activity.StartLatLng");
            var startLocation = new Coordinate(activity.StartLatLng[1], activity.StartLatLng[0]);
            var activityLength = (int) Math.Ceiling(activity.Distance ?? 0);
            var nearbyPeaks = await _peaksCollection.GeoSpatialFetch(startLocation, activityLength);
            var nearbyPoints = nearbyPeaks.Select(peak => (peak.Id, Coordinate.ParseGeoJsonCoordinate(peak.Geometry.Coordinates)));
            var summitedPeakIds = GeoSpatialFunctions.FindPointsIntersectingLine(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline);

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
                await _summitedPeaksCollection.StoreDocument(summitedPeakDocument);
            }
        }
    }
}