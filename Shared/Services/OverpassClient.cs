using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using BAMCIS.GeoJSON;
using Shared.Models;


namespace Shared.Services
{
    public class OverpassClient(IHttpClientFactory httpClientFactory)
    {
        readonly HttpClient _client = httpClientFactory.CreateClient("overpassClient");

        private readonly string[] mirrors = ["https://overpass.private.coffee/api/interpreter", "https://overpass-api.de/api/interpreter", "https://maps.mail.ru/osm/tools/overpass/api/interpreter"];


        private async Task<HttpResponseMessage> GetAsyncMultipleMirrors(string query)
        {
            foreach (var mirror in mirrors)
            {
                try
                {
                    var response = await _client.GetAsync($"{mirror}?data={query}");
                    if (response.IsSuccessStatusCode)
                    {
                        return response;
                    }
                }
                catch (Exception) { }
            }
            return await _client.GetAsync($"{mirrors[0]}?data={query}");
        }

        public async Task<IEnumerable<Feature>> GetPeaks(Coordinate southWest, Coordinate northEast)
        {
            // Overpass expects bbox as: minLat,minLon,maxLat,maxLon
            string bbox = $"{southWest.Lat},{southWest.Lng},{northEast.Lat},{northEast.Lng}";
            string query = $"[out:json][timeout:400];node[\"natural\"=\"peak\"][\"name\"]({bbox});out qt;";
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get peaks, status code: {response.StatusCode}, {response.ReasonPhrase}");
            string rawPeaks = await response.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(rawPeaks) || !(rawPeaks.TrimStart().StartsWith("{") || rawPeaks.TrimStart().StartsWith("[")))
            {
                // Log the raw response for debugging
                Console.WriteLine("Overpass response was not valid JSON:");
                Console.WriteLine(rawPeaks);
                throw new Exception("Overpass API did not return valid JSON. Response may be HTML or an error page.");
            }
            RootPeaks rootPeaks = JsonConvert.DeserializeObject<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
            return ConvertRawPeaksToFeatures(rootPeaks.Elements);
        }

        private static IEnumerable<Feature> ConvertRawPeaksToFeatures(IEnumerable<RawPeaks> rawPeaks)
        {
            foreach (var p in rawPeaks)
            {
                // Convert tags to simple dictionary, filtering out nulls
                var properties = p.Tags
                    .Where(kvp => kvp.Value.Type == JTokenType.String)
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => (dynamic)kvp.Value.ToString()
                    );

                var geometry = new Point(new Position(p.Lon, p.Lat));
                var feature = new Feature(geometry, properties, null, new FeatureId(p.Id.ToString()));
                
                yield return feature;
            }
        }

        public async Task<IEnumerable<Feature>> GetPaths(Coordinate southWest, Coordinate northEast)
        {
            string bbox = $"{southWest.Lat},{southWest.Lng},{northEast.Lat},{northEast.Lng}";
            string query = $"[out:json][timeout:400];way[\"highway\"=\"path\"]({bbox});out geom;";
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get paths, status code: {response.StatusCode}, {response.ReasonPhrase}");
            string rawPaths = await response.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(rawPaths) || !(rawPaths.TrimStart().StartsWith("{") || rawPaths.TrimStart().StartsWith("[")))
            {
                Console.WriteLine("Overpass response was not valid JSON:");
                Console.WriteLine(rawPaths);
                throw new Exception("Overpass API did not return valid JSON. Response may be HTML or an error page.");
            }
            RootPaths rootPaths = JsonConvert.DeserializeObject<RootPaths>(rawPaths) ?? throw new Exception("Could not deserialize");
            return ConvertRawPathsToFeatures(rootPaths.Elements);
        }

        private static IEnumerable<Feature> ConvertRawPathsToFeatures(IEnumerable<RawPath> rawPaths)
        {
            foreach (var p in rawPaths)
            {
                if (p.Geometry == null || p.Geometry.Count == 0) continue;
                var properties = p.Tags
                    .Where(kvp => kvp.Value.Type == JTokenType.String)
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => (dynamic)kvp.Value.ToString()
                    );
                var coordinates = p.Geometry.Select(node => new Position(node.Lon, node.Lat)).ToList();
                var geometry = new LineString(coordinates);
                var feature = new Feature(geometry, properties, null, new FeatureId(p.Id.ToString()));
                yield return feature;
            }
        }
    }

    public class RootPeaks
    {
        [JsonProperty("elements")]
        public required List<RawPeaks> Elements { get; set; }
    }
    public class RawPeaks
    {
        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("lat")]
        public double Lat { get; set; }

        [JsonProperty("lon")]
        public double Lon { get; set; }

        [JsonProperty("tags")]
        public Dictionary<string, JToken> Tags { get; set; } = new();
    }
    public class RootPaths
    {
        [JsonProperty("elements")]
        public required List<RawPath> Elements { get; set; }
    }
    public class RawPath
    {
        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("geometry")]
        public List<PathNode>? Geometry { get; set; }

        [JsonProperty("tags")]
        public Dictionary<string, JToken> Tags { get; set; } = new();
    }
    public class PathNode
    {
        [JsonProperty("lat")]
        public double Lat { get; set; }
        [JsonProperty("lon")]
        public double Lon { get; set; }
    }
}