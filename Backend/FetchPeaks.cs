using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend
{
    public class FetchPeaks(ILogger<FetchPeaks> _logger, OverpassClient _overpassClient)
    {
        [Function("FetchPeaks")]
        [CosmosDBOutput("%CosmosDb%", "%PeaksContainer%", Connection = "CosmosDBConnection", PartitionKey = "/id")]
        public async Task<IEnumerable<StoredFeature>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "fetchPeaks/{bbox}")] HttpRequestData req, string bbox)
        {
            // Parse bbox string: expected format "minLat,minLon,maxLat,maxLon"
            var parts = bbox.Split(',');
            if (parts.Length != 4)
                throw new ArgumentException("Bounding box must have 4 comma-separated values: minLat,minLon,maxLat,maxLon");
            var southWest = new Coordinate(double.Parse(parts[1]), double.Parse(parts[0]));
            var northEast = new Coordinate(double.Parse(parts[3]), double.Parse(parts[2]));

            var features = await _overpassClient.GetPeaks(southWest, northEast);
            _logger.LogInformation("Fetched {Count} peaks", features.Count());
            var storedFeatures = features.Select(f => new StoredFeature(f)).ToList();
            return storedFeatures;
        }
    }
}