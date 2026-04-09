using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Models;
using System.Globalization;


namespace Shared.Services
{
    public class OverpassClient(IHttpClientFactory httpClientFactory)
    {
        readonly HttpClient _client = httpClientFactory.CreateClient("overpassClient");

        private readonly string[] mirrors = ["https://overpass.openstreetmap.fr/api/interpreter", "https://overpass.private.coffee/api/interpreter", "https://overpass-api.de/api/interpreter", "https://maps.mail.ru/osm/tools/overpass/api/interpreter"];


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

        private static string CreateBoundingBox(Coordinate southWest, Coordinate northEast)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{southWest.Lat},{southWest.Lng},{northEast.Lat},{northEast.Lng}"
            );
        }

        public async Task<IEnumerable<Feature>> GetPeaks(Coordinate southWest, Coordinate northEast)
        {
            // Overpass expects bbox as: minLat,minLon,maxLat,maxLon
            string bbox = CreateBoundingBox(southWest, northEast);
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
            string bbox = CreateBoundingBox(southWest, northEast);
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

        public async Task<IEnumerable<Feature>> GetProtectedAreas(Coordinate southWest, Coordinate northEast)
        {
            string bbox = CreateBoundingBox(southWest, northEast);
            string query = string.Join(string.Empty,
                "[out:json][timeout:400];(",
                $"way[\"boundary\"~\"protected_area|national_park\"][\"name\"]({bbox});",
                $"relation[\"boundary\"~\"protected_area|national_park\"][\"name\"]({bbox});",
                $"way[\"leisure\"=\"nature_reserve\"][\"name\"]({bbox});",
                $"relation[\"leisure\"=\"nature_reserve\"][\"name\"]({bbox});",
                ");out geom;"
            );
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get protected areas, status code: {response.StatusCode}, {response.ReasonPhrase}");

            string rawAreas = await response.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(rawAreas) || !(rawAreas.TrimStart().StartsWith("{") || rawAreas.TrimStart().StartsWith("[")))
            {
                Console.WriteLine("Overpass response was not valid JSON:");
                Console.WriteLine(rawAreas);
                throw new Exception("Overpass API did not return valid JSON. Response may be HTML or an error page.");
            }

            RootProtectedAreas rootProtectedAreas = JsonConvert.DeserializeObject<RootProtectedAreas>(rawAreas)
                ?? throw new Exception("Could not deserialize");
            return ConvertRawProtectedAreasToFeatures(rootProtectedAreas.Elements);
        }

        private static IEnumerable<Feature> ConvertRawProtectedAreasToFeatures(IEnumerable<RawProtectedArea> rawProtectedAreas)
        {
            foreach (var area in rawProtectedAreas)
            {
                var geometry = area.Type switch
                {
                    "way" => BuildPolygonFromPath(area.Geometry),
                    "relation" => BuildGeometryFromRelation(area.Members),
                    _ => null,
                };

                if (geometry == null)
                {
                    continue;
                }

                var properties = area.Tags
                    .Where(kvp => kvp.Value.Type == JTokenType.String)
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => (dynamic)kvp.Value.ToString()
                    );
                properties["areaType"] = GetProtectedAreaType(properties);
                properties["osmType"] = area.Type;

                yield return new Feature(
                    geometry,
                    properties,
                    null,
                    new FeatureId($"{area.Type}:{area.Id}")
                );
            }
        }

        private static Geometry? BuildGeometryFromRelation(IEnumerable<RawProtectedAreaMember>? members)
        {
            if (members == null)
            {
                return null;
            }

            var outerSegments = members
                .Where(member => member.Role == "outer" && member.Geometry != null && member.Geometry.Count >= 2)
                .Select(member => member.Geometry!.Select(node => new Position(node.Lon, node.Lat)));

            var rings = PolygonRingAssembler.AssembleRings(outerSegments);

            var polygons = rings
                .Select(ring => new Polygon([ring], null))
                .ToList();

            return polygons.Count switch
            {
                0 => null,
                1 => polygons[0],
                _ => new MultiPolygon(polygons, null),
            };
        }

        private static Polygon? BuildPolygonFromPath(IEnumerable<PathNode>? nodes)
        {
            var positions = nodes?
                .Select(node => new Position(node.Lon, node.Lat))
                .ToList();

            if (positions == null || positions.Count < 3)
            {
                return null;
            }

            var first = positions[0];
            var last = positions[^1];
            if (first.Longitude != last.Longitude || first.Latitude != last.Latitude)
            {
                positions.Add(new Position(first.Longitude, first.Latitude));
            }

            if (positions.Count < 4)
            {
                return null;
            }

            return new Polygon([new LinearRing(positions, null)], null);
        }

        private static string GetProtectedAreaType(Dictionary<string, dynamic> properties)
        {
            var boundary = properties.TryGetValue("boundary", out var boundaryValue) ? boundaryValue?.ToString() : null;
            var leisure = properties.TryGetValue("leisure", out var leisureValue) ? leisureValue?.ToString() : null;
            var protectClass = properties.TryGetValue("protect_class", out var protectClassValue) ? protectClassValue?.ToString() : null;

            if (boundary == "national_park" || protectClass == "2")
            {
                return "national_park";
            }

            if (leisure == "nature_reserve" || protectClass == "4")
            {
                return "nature_reserve";
            }

            return "protected_area";
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

    public class RootProtectedAreas
    {
        [JsonProperty("elements")]
        public required List<RawProtectedArea> Elements { get; set; }
    }

    public class RawProtectedArea
    {
        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;

        [JsonProperty("geometry")]
        public List<PathNode>? Geometry { get; set; }

        [JsonProperty("members")]
        public List<RawProtectedAreaMember>? Members { get; set; }

        [JsonProperty("tags")]
        public Dictionary<string, JToken> Tags { get; set; } = new();
    }

    public class RawProtectedAreaMember
    {
        [JsonProperty("role")]
        public string Role { get; set; } = string.Empty;

        [JsonProperty("geometry")]
        public List<PathNode>? Geometry { get; set; }
    }
}