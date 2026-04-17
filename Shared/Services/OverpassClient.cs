using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;
using System.Globalization;
using System.Net;


namespace Shared.Services
{
    public class OverpassClient(HttpClient httpClient, ILogger<OverpassClient> logger)
    {
        readonly HttpClient _client = httpClient;
        readonly ILogger<OverpassClient> _logger = logger;
        private const int MaxAttemptsPerMirror = 2;
        private const int DefaultMaxConcurrentRequests = 2;
        private static readonly TimeSpan BaseThrottleDelay = TimeSpan.FromMilliseconds(750);
        private static readonly int MaxConcurrentRequests = GetMaxConcurrentRequests();
        private static readonly SemaphoreSlim RequestSemaphore = new(MaxConcurrentRequests, MaxConcurrentRequests);
        private static readonly HttpStatusCode[] ThrottledStatusCodes = [
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout
        ];

        private readonly string[] mirrors = ["https://overpass.openstreetmap.fr/api/interpreter", "https://overpass.private.coffee/api/interpreter", "https://overpass-api.de/api/interpreter", "https://maps.mail.ru/osm/tools/overpass/api/interpreter", "https://overpass.maprva.org/api/interpreter"];


        private async Task<HttpResponseMessage> GetAsyncMultipleMirrors(string query, CancellationToken cancellationToken = default)
        {
            await RequestSemaphore.WaitAsync(cancellationToken);

            Exception? lastException = null;
            HttpStatusCode? lastStatusCode = null;
            var mirrorsInAttemptOrder = (string[])mirrors.Clone();
            Random.Shared.Shuffle(mirrorsInAttemptOrder);

            try
            {
                foreach (var mirror in mirrorsInAttemptOrder)
                {
                    for (var attempt = 1; attempt <= MaxAttemptsPerMirror; attempt++)
                    {
                        try
                        {
                            var response = await _client.GetAsync($"{mirror}?data={query}", cancellationToken);
                            if (response.IsSuccessStatusCode)
                            {
                                return response;
                            }

                            lastStatusCode = response.StatusCode;

                            if (!IsThrottledStatusCode(response.StatusCode))
                            {
                                response.Dispose();
                                break;
                            }

                            var delay = GetRetryDelay(response, attempt);
                            _logger.LogWarning("Overpass mirror {Mirror} throttled with status {StatusCode} on attempt {Attempt}. Retrying after {DelayMs}ms.", mirror, (int)response.StatusCode, attempt, delay.TotalMilliseconds);
                            response.Dispose();
                            await Task.Delay(delay, cancellationToken);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            lastException = ex;
                            _logger.LogWarning(ex, "Overpass mirror {Mirror} failed on attempt {Attempt}.", mirror, attempt);
                            break;
                        }
                    }
                }

                if (lastException != null)
                    throw lastException;

                throw new HttpRequestException($"All Overpass mirrors failed or throttled. Last status code: {(int?)lastStatusCode}");
            }
            finally
            {
                RequestSemaphore.Release();
            }
        }

        private static int GetMaxConcurrentRequests()
        {
            var configuredValue = Environment.GetEnvironmentVariable("OverpassMaxConcurrency");

            return int.TryParse(configuredValue, out var maxConcurrency) && maxConcurrency > 0
                ? maxConcurrency
                : DefaultMaxConcurrentRequests;
        }

        private static bool IsThrottledStatusCode(HttpStatusCode statusCode)
        {
            return ThrottledStatusCodes.Contains(statusCode);
        }

        private static TimeSpan GetRetryDelay(HttpResponseMessage response, int attempt)
        {
            var retryAfter = response.Headers.RetryAfter;

            if (retryAfter?.Delta is TimeSpan delta)
                return ClampRetryDelay(delta);

            if (retryAfter?.Date is DateTimeOffset date)
                return ClampRetryDelay(date - DateTimeOffset.UtcNow);

            var exponentialDelay = TimeSpan.FromMilliseconds(BaseThrottleDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
            return ClampRetryDelay(exponentialDelay);
        }

        private static TimeSpan ClampRetryDelay(TimeSpan delay)
        {
            if (delay <= TimeSpan.Zero)
                return BaseThrottleDelay;

            var maxDelay = TimeSpan.FromSeconds(8);
            return delay <= maxDelay ? delay : maxDelay;
        }

        private static string CreateBoundingBox(Coordinate southWest, Coordinate northEast)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{southWest.Lat},{southWest.Lng},{northEast.Lat},{northEast.Lng}"
            );
        }

        public async Task<IEnumerable<Feature>> GetPeaks(Coordinate southWest, Coordinate northEast, CancellationToken cancellationToken = default)
        {
            // Overpass expects bbox as: minLat,minLon,maxLat,maxLon
            string bbox = CreateBoundingBox(southWest, northEast);
            string query = $"[out:json][timeout:400];node[\"natural\"=\"peak\"][\"name\"]({bbox});out qt;";
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery, cancellationToken);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get peaks, status code: {response.StatusCode}, {response.ReasonPhrase}");
            string rawPeaks = await response.Content.ReadAsStringAsync(cancellationToken);
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

        public async Task<IEnumerable<Feature>> GetPeaksByIds(IEnumerable<string> peakIds, CancellationToken cancellationToken = default)
        {
            const int maxIdsPerRequest = 100;
            var normalizedIds = peakIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, id))
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            if (normalizedIds.Length == 0)
                return [];

            var features = new List<Feature>();
            foreach (var batch in normalizedIds.Chunk(maxIdsPerRequest))
            {
                string query = $"[out:json][timeout:400];node(id:{string.Join(',', batch)});out body;";
                string encodedQuery = Uri.EscapeDataString(query);

                List<RawPeaks>? rawPeaksBatch = null;
                foreach (var mirror in mirrors)
                {
                    using var response = await _client.GetAsync($"{mirror}?data={encodedQuery}", cancellationToken);
                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogWarning("Overpass mirror {Mirror} returned {StatusCode} when fetching peaks by id.", mirror, (int)response.StatusCode);
                        continue;
                    }

                    string rawPeaks = await response.Content.ReadAsStringAsync(cancellationToken);
                    if (string.IsNullOrWhiteSpace(rawPeaks) || !(rawPeaks.TrimStart().StartsWith("{") || rawPeaks.TrimStart().StartsWith("[")))
                    {
                        _logger.LogWarning("Overpass mirror {Mirror} returned a non-JSON response when fetching peaks by id.", mirror);
                        continue;
                    }

                    RootPeaks rootPeaks = JsonConvert.DeserializeObject<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
                    rawPeaksBatch = rootPeaks.Elements
                        .Where(p => p.Tags.TryGetValue("natural", out var natural)
                            && string.Equals(natural.ToString(), "peak", StringComparison.Ordinal))
                        .ToList();

                    if (rawPeaksBatch.Count > 0)
                        break;
                }

                if (rawPeaksBatch == null || rawPeaksBatch.Count == 0)
                    continue;

                features.AddRange(ConvertRawPeaksToFeatures(rawPeaksBatch));
            }

            return features;
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

        public async Task<IEnumerable<Feature>> GetPaths(Coordinate southWest, Coordinate northEast, CancellationToken cancellationToken = default)
        {
            string bbox = CreateBoundingBox(southWest, northEast);
            string query = $"[out:json][timeout:400];way[\"highway\"=\"path\"]({bbox});out geom;";
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery, cancellationToken);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get paths, status code: {response.StatusCode}, {response.ReasonPhrase}");
            string rawPaths = await response.Content.ReadAsStringAsync(cancellationToken);
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

        public async Task<IEnumerable<Feature>> GetProtectedAreas(Coordinate southWest, Coordinate northEast, CancellationToken cancellationToken = default)
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

            var response = await GetAsyncMultipleMirrors(encodedQuery, cancellationToken);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get protected areas, status code: {response.StatusCode}, {response.ReasonPhrase}");

            string rawAreas = await response.Content.ReadAsStringAsync(cancellationToken);
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
                .Where(member => member.Role == "outer" && member.Geometry != null)
                .Select(member => member.Geometry!
                    .Where(node => node != null)
                    .Select(node => new Position(node!.Lon, node.Lat))
                    .ToList())
                .Where(segment => segment.Count >= 2);

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
                .Where(node => node != null)
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

        public async Task<IEnumerable<Feature>> GetAdminBoundaries(Coordinate southWest, Coordinate northEast, int adminLevel, CancellationToken cancellationToken = default)
        {
            string bbox = CreateBoundingBox(southWest, northEast);
            var levelStr = adminLevel.ToString(CultureInfo.InvariantCulture);
            // Filter out dependent territories / sub-national overseas entities that
            // share admin_level=2 with their parent country.
            var territoryFilter = adminLevel == 2
                ? "[\"border_type\"!~\"territorial|territory|dependency\"]"
                : "";
            string query = string.Join(string.Empty,
                "[out:json][timeout:400];(",
                $"relation[\"boundary\"=\"administrative\"][\"admin_level\"=\"{levelStr}\"][\"name\"]{territoryFilter}({bbox});",
                $"way[\"boundary\"=\"administrative\"][\"admin_level\"=\"{levelStr}\"][\"name\"]{territoryFilter}({bbox});",
                ");out geom;"
            );

            return await ExecuteAdminBoundaryQuery(query, cancellationToken);
        }

        /// <summary>
        /// Fetch full geometry for specific OSM boundary IDs.
        /// Use this to fetch only the boundaries that are not yet in Cosmos
        /// rather than re-downloading all boundaries in a bbox.
        /// </summary>
        public async Task<IEnumerable<Feature>> GetAdminBoundariesByIds(IEnumerable<(string osmType, long osmId)> ids, CancellationToken cancellationToken = default)
        {
            var relationIds = ids.Where(x => x.osmType == "relation").Select(x => x.osmId).ToList();
            var wayIds = ids.Where(x => x.osmType == "way").Select(x => x.osmId).ToList();

            var parts = new List<string>();
            if (relationIds.Count > 0)
                parts.Add($"relation(id:{string.Join(",", relationIds)});");
            if (wayIds.Count > 0)
                parts.Add($"way(id:{string.Join(",", wayIds)});");

            if (parts.Count == 0)
                return [];

            string query = $"[out:json][timeout:400];({string.Join("", parts)});out geom;";
            return await ExecuteAdminBoundaryQuery(query, cancellationToken);
        }

        /// <summary>
        /// Lightweight query that returns only IDs and tags (no geometry) for
        /// admin boundaries in a bounding box.  Used to discover which boundaries
        /// exist in a tile before deciding whether to fetch full geometry.
        /// </summary>
        public async Task<IEnumerable<(string osmType, long osmId, Dictionary<string, string> tags)>> GetAdminBoundaryIds(Coordinate southWest, Coordinate northEast, int adminLevel, CancellationToken cancellationToken = default)
        {
            string bbox = CreateBoundingBox(southWest, northEast);
            var levelStr = adminLevel.ToString(CultureInfo.InvariantCulture);
            var territoryFilter = adminLevel == 2
                ? "[\"border_type\"!~\"territorial|territory|dependency\"]"
                : "";
            // Admin boundaries are always relations in OSM.
            // Querying ways returns individual border segments
            // (e.g. "Border Bulgaria - Greece") which are not actual regions.
            string query = string.Join(string.Empty,
                "[out:json][timeout:120];(",
                $"relation[\"boundary\"=\"administrative\"][\"admin_level\"=\"{levelStr}\"][\"name\"]{territoryFilter}({bbox});",
                ");out tags;"
            );
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery, cancellationToken);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get admin boundary IDs, status code: {response.StatusCode}, {response.ReasonPhrase}");

            string rawAreas = await response.Content.ReadAsStringAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(rawAreas) || !(rawAreas.TrimStart().StartsWith("{") || rawAreas.TrimStart().StartsWith("[")))
                throw new Exception("Overpass API did not return valid JSON for admin boundary IDs.");

            RootProtectedAreas root = JsonConvert.DeserializeObject<RootProtectedAreas>(rawAreas)
                ?? throw new Exception("Could not deserialize admin boundary IDs");

            return root.Elements.Select(e => (
                e.Type,
                e.Id,
                e.Tags
                    .Where(kvp => kvp.Value.Type == JTokenType.String)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToString())
            ));
        }

        private async Task<IEnumerable<Feature>> ExecuteAdminBoundaryQuery(string query, CancellationToken cancellationToken)
        {
            string encodedQuery = Uri.EscapeDataString(query);

            var response = await GetAsyncMultipleMirrors(encodedQuery, cancellationToken);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Could not get admin boundaries, status code: {response.StatusCode}, {response.ReasonPhrase}");

            string rawAreas = await response.Content.ReadAsStringAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(rawAreas) || !(rawAreas.TrimStart().StartsWith("{") || rawAreas.TrimStart().StartsWith("[")))
            {
                Console.WriteLine("Overpass response was not valid JSON:");
                Console.WriteLine(rawAreas);
                throw new Exception("Overpass API did not return valid JSON. Response may be HTML or an error page.");
            }

            RootProtectedAreas root = JsonConvert.DeserializeObject<RootProtectedAreas>(rawAreas)
                ?? throw new Exception("Could not deserialize");
            return ConvertRawAdminBoundariesToFeatures(root.Elements);
        }

        private static IEnumerable<Feature> ConvertRawAdminBoundariesToFeatures(IEnumerable<RawProtectedArea> raw)
        {
            foreach (var area in raw)
            {
                var geometry = area.Type switch
                {
                    "way" => BuildPolygonFromPath(area.Geometry),
                    "relation" => BuildGeometryFromRelation(area.Members),
                    _ => null,
                };
                if (geometry == null) continue;

                var properties = area.Tags
                    .Where(kvp => kvp.Value.Type == JTokenType.String)
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => (dynamic)kvp.Value.ToString()
                    );
                properties["osmType"] = area.Type;

                yield return new Feature(
                    geometry,
                    properties,
                    null,
                    new FeatureId($"{area.Type}:{area.Id}")
                );
            }
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