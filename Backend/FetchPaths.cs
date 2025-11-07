using System.Text.Json.Serialization;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Microsoft.Extensions.Logging;
using BAMCIS.GeoJSON;
using Shared.Services;

namespace Backend
{
    public class RootPaths
    {
        [JsonPropertyName("elements")]
        public required List<RawPath> Elements { get; set; }
    }

    public class RawPath
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("geometry")]
        public List<PathNode>? Geometry { get; set; }

        [JsonPropertyName("tags")]
        public Dictionary<string, JsonElement> Tags { get; set; } = [];
    }

    public class PathNode
    {
        [JsonPropertyName("lat")]
        public double Lat { get; set; }

        [JsonPropertyName("lon")]
        public double Lon { get; set; }
    }

    public class FetchPaths(ILogger<FetchPaths> _logger, OverpassClient _overpassClient)
    {
        [Function("FetchPaths")]
        [CosmosDBOutput("%CosmosDb%", "%PathsContainer%", Connection = "CosmosDBConnection", PartitionKey = "/id")]
        public async Task<IEnumerable<StoredFeature>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "fetchPaths/{bbox}")] HttpRequestData req, string bbox)
        {
            // Parse bbox string: expected format "minLat,minLon,maxLat,maxLon"
            var parts = bbox.Split(',');
            if (parts.Length != 4)
                throw new ArgumentException("Bounding box must have 4 comma-separated values: minLat,minLon,maxLat,maxLon");
            var southWest = new Coordinate(double.Parse(parts[1]), double.Parse(parts[0]));
            var northEast = new Coordinate(double.Parse(parts[3]), double.Parse(parts[2]));

            var features = await _overpassClient.GetPaths(southWest, northEast);
            _logger.LogInformation("Fetched {Count} paths", features.Count());
            return features.Select(f => new StoredFeature(f));
        }
    }
}
