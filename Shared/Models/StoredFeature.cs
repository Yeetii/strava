using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Services;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json.Serialization;

namespace Shared.Models
{
    public class StoredFeature : IStoredInGrid, IDocument
    {
        public const string PointerFlagProperty = "isPointer";
        public const string PointerFeatureIdProperty = "pointerFeatureId";
        public const string PointerStoredDocumentIdProperty = "storedDocumentId";
        public const string PointerStoredXProperty = "storedX";
        public const string PointerStoredYProperty = "storedY";
        public const string PointerStoredZoomProperty = "storedZoom";

        public required string Id { get; set; }
        public string? FeatureId { get; set; }
        public string? Kind { get; set; }
        public required int X { get; set; }
        public required int Y { get; set; }
        public int Zoom { get; set; }
        public IDictionary<string, dynamic> Properties { get; set; } = new Dictionary<string, dynamic>();
        [JsonConverter(typeof(GeometrySystemTextJsonConverter))]
        public required Geometry Geometry { get; set; }

        [JsonIgnore]
        public string LogicalId => !string.IsNullOrWhiteSpace(FeatureId)
            ? FeatureId
            : NormalizeFeatureId(Kind, Id);

        public static string NormalizeFeatureId(string? kind, string id)
        {
            if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(kind))
                return id;

            var prefix = $"{kind}:";
            if (!id.StartsWith(prefix, StringComparison.Ordinal))
                return id;

            var logicalId = id[prefix.Length..];
            if (kind == FeatureKinds.AdminBoundary)
            {
                var separatorIndex = logicalId.IndexOf(':');
                if (separatorIndex > 0
                    && int.TryParse(logicalId[..separatorIndex], CultureInfo.InvariantCulture, out _)
                    && separatorIndex + 1 < logicalId.Length)
                {
                    return logicalId[(separatorIndex + 1)..];
                }
            }

            return logicalId;
        }

        public static string EnsurePrefixedFeatureId(string kind, string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return id;

            return $"{kind}:{NormalizeFeatureId(kind, id)}";
        }

        public static bool IsPointerDocument(StoredFeature feature)
        {
            if (!feature.Properties.TryGetValue(PointerFlagProperty, out var value))
                return false;

            return bool.TryParse(value?.ToString(), out bool parsed) && parsed;
        }

        public static string? GetPointerStoredDocumentId(StoredFeature feature)
        {
            return feature.Properties.TryGetValue(PointerStoredDocumentIdProperty, out var value)
                ? value?.ToString()
                : null;
        }

        public static StoredFeature CreatePointer(
            string kind,
            string featureId,
            int x,
            int y,
            int zoom,
            int storedX,
            int storedY,
            int storedZoom,
            string storedDocumentId,
            IDictionary<string, dynamic>? additionalProperties = null)
        {
            var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
            var center = new Point(new Position(
                (southWest.Lng + northEast.Lng) / 2,
                (southWest.Lat + northEast.Lat) / 2));

            var properties = new Dictionary<string, dynamic>
            {
                [PointerFlagProperty] = true,
                [PointerFeatureIdProperty] = featureId,
                [PointerStoredDocumentIdProperty] = storedDocumentId,
                [PointerStoredXProperty] = storedX,
                [PointerStoredYProperty] = storedY,
                [PointerStoredZoomProperty] = storedZoom
            };

            if (additionalProperties != null)
            {
                foreach (var (key, value) in additionalProperties)
                {
                    properties[key] = value;
                }
            }

            return new StoredFeature
            {
                Id = $"pointer:{kind}:{zoom}:{x}:{y}:{featureId}",
                FeatureId = featureId,
                Kind = kind,
                X = x,
                Y = y,
                Zoom = zoom,
                Geometry = center,
                Properties = properties
            };
        }

        public StoredFeature()
        {
        }

        [SetsRequiredMembers]
        public StoredFeature(Feature feature, string kind, int zoom = 11)
        {
            var featureId = feature.Id.Value;
            Kind = kind;
            Id = $"{kind}:{featureId}";
            FeatureId = featureId;
            Geometry = feature.Geometry;
            Properties = feature.Properties;
            var coordinate = GeometryCentroidHelper.GetCentroid(feature.Geometry);

            var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coordinate, zoom);
            X = x;
            Y = y;
            Zoom = zoom;
        }

        [SetsRequiredMembers]
        public StoredFeature(Feature feature, string kind, int x, int y, int zoom, bool storePerTile)
        {
            var featureId = feature.Id.Value;

            Kind = kind;
            Id = storePerTile ? $"{kind}:{featureId}:{zoom}:{x}:{y}" : $"{kind}:{featureId}";
            FeatureId = featureId;
            Geometry = feature.Geometry;
            Properties = feature.Properties;
            X = x;
            Y = y;
            Zoom = zoom;
        }

        [SetsRequiredMembers]
        public StoredFeature(string kind, int x, int y, int zoom = 11)
        {
            Kind = kind;
            X = x;
            Y = y;
            Zoom = zoom;
            Id = $"empty-{kind}-{zoom}-{x}-{y}";
            FeatureId = Id;
            Geometry = new Point(new Position(0, 0));
        }

        public Feature ToFeature()
        {
            var propertiesCopy = new Dictionary<string, dynamic>(Properties.Count + 3);
            foreach (var (key, value) in Properties)
                propertiesCopy[key] = NormalizePropertyValue(value);

            propertiesCopy["x"] = X.ToString();
            propertiesCopy["y"] = Y.ToString();
            propertiesCopy["zoom"] = Zoom.ToString();

            return new Feature(
                Geometry,
                propertiesCopy,
                null,
                new FeatureId(LogicalId)
            );
        }

        // When Properties come back from Cosmos via System.Text.Json, values are JsonElement
        // rather than native CLR types.  BAMCIS GeoJSON re-serializes them as nested wrappers,
        // producing e.g. [[]] instead of ["tracedetrail"].  Convert them back to plain objects.
        // When read via Newtonsoft (CollectionClient), nested objects come back as JToken.
        private static dynamic NormalizePropertyValue(dynamic value)
        {
            if (value is System.Text.Json.JsonElement je)
                return ConvertJsonElement(je);
            if (value is Newtonsoft.Json.Linq.JToken jt)
                return ConvertJToken(jt);
            return value;
        }

        private static dynamic ConvertJToken(Newtonsoft.Json.Linq.JToken token) => token.Type switch
        {
            Newtonsoft.Json.Linq.JTokenType.String => token.ToObject<string>()!,
            Newtonsoft.Json.Linq.JTokenType.Integer => token.ToObject<long>(),
            Newtonsoft.Json.Linq.JTokenType.Float => token.ToObject<double>(),
            Newtonsoft.Json.Linq.JTokenType.Boolean => token.ToObject<bool>(),
            Newtonsoft.Json.Linq.JTokenType.Null => null!,
            Newtonsoft.Json.Linq.JTokenType.Array => ((Newtonsoft.Json.Linq.JArray)token).Select(ConvertJToken).ToList(),
            Newtonsoft.Json.Linq.JTokenType.Object => ((Newtonsoft.Json.Linq.JObject)token)
                .Properties()
                .ToDictionary(p => p.Name, p => ConvertJToken(p.Value)),
            _ => token.ToString()
        };

        private static dynamic ConvertJsonElement(System.Text.Json.JsonElement element) => element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => element.GetString()!,
            System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Null => null!,
            System.Text.Json.JsonValueKind.Array => element.EnumerateArray().Select(ConvertJsonElement).ToList(),
            System.Text.Json.JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => ConvertJsonElement(p.Value)),
            _ => element.GetRawText()
        };
    }
}