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
            var propertiesCopy = new Dictionary<string, dynamic>(Properties)
            {
                ["x"] = X.ToString(),
                ["y"] = Y.ToString(),
                ["zoom"] = Zoom.ToString()
            };

            return new Feature(
                Geometry,
                propertiesCopy,
                null,
                new FeatureId(LogicalId)
            );
        }
    }
}