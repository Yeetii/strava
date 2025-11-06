using System.Text.Json.Serialization;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Microsoft.Extensions.Logging;
using BAMCIS.GeoJSON;

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

    public class FetchPaths(ILogger<FetchPaths> _logger)
    {
        private const string overpassUri = "https://overpass-api.de/api/interpreter";
        static readonly HttpClient client = new();

        [Function("FetchPaths")]
        [CosmosDBOutput("%CosmosDb%", "%PathsContainer%", Connection = "CosmosDBConnection", PartitionKey = "/id")]
        public async Task<IEnumerable<StoredFeature>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "fetchPaths/{bbox}")] HttpRequestData req, string bbox)
        {
            // Fetch hiking trails and paths with geometry
            string body = @"[out:json][timeout:300];" + "\n" +
            $"            way[\"highway\"=\"path\"]({bbox});" + "\n" +
            @"            out geom;" + "\n";

            var buffer = System.Text.Encoding.UTF8.GetBytes(body);
            var byteContent = new ByteArrayContent(buffer);
            var response = await client.PostAsync(overpassUri, byteContent);
            string rawPaths = await response.Content.ReadAsStringAsync();
            
            // Log the response if it's not JSON
            if (!rawPaths.TrimStart().StartsWith("{"))
            {
                _logger.LogError("Overpass API returned non-JSON response: {Response}", rawPaths);
                throw new Exception($"Overpass API error: {rawPaths}");
            }
            
            RootPaths myDeserializedClass = JsonSerializer.Deserialize<RootPaths>(rawPaths) ?? throw new Exception("Could not deserialize");
            _logger.LogInformation("Fetched {AmtPaths} paths", myDeserializedClass.Elements.Count);

            var paths = myDeserializedClass.Elements
                .Where(x => x.Geometry != null && x.Geometry.Any())
                .Select(x =>
                {
                    // Convert tags to simple dictionary, filtering out nulls
                    var properties = x.Tags
                        .Where(kvp => kvp.Value.ValueKind == JsonValueKind.String)
                        .ToDictionary(
                            kvp => kvp.Key,
                            kvp => (dynamic)kvp.Value.GetString()!
                        );

                    // Convert path geometry to GeoJSON LineString coordinates
                    var coordinates = x.Geometry!.Select(node => new double[] { node.Lon, node.Lat }).ToList();

                    var geometry = new LineString([.. coordinates.Select(coord => new Position(coord[0], coord[1]))]);
                    var feature = new Feature(geometry, properties, null, new FeatureId(x.Id.ToString()));
                    return new StoredFeature(feature);
                });

            return paths;
        }
    }
}
