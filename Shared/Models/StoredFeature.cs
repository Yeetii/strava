using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Services;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Shared.Models
{
    public class StoredFeature : IStoredInGrid, IDocument
    {
        public required string Id { get; set; }
        public string? FeatureId { get; set; }
        public required int X { get; set; }
        public required int Y { get; set; }
        public int Zoom { get; set; }
        public IDictionary<string, dynamic> Properties { get; set; } = new Dictionary<string, dynamic>();
        [JsonConverter(typeof(GeometrySystemTextJsonConverter))]
        public required Geometry Geometry { get; set; }

        [JsonIgnore]
        public string LogicalId => FeatureId ?? Id;

        public StoredFeature()
        {
        }

        [SetsRequiredMembers]
        public StoredFeature(Feature feature, int zoom = 11)
        {
            Id = feature.Id.Value;
            FeatureId = feature.Id.Value;
            Geometry = feature.Geometry;
            Properties = feature.Properties;
            var coordinate = GeometryCentroidHelper.GetCentroid(feature.Geometry);

            var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coordinate, zoom);
            X = x;
            Y = y;
            Zoom = zoom;
        }

        [SetsRequiredMembers]
        public StoredFeature(Feature feature, int x, int y, int zoom, bool storePerTile)
        {
            var featureId = feature.Id.Value;

            Id = storePerTile ? $"{featureId}:{zoom}:{x}:{y}" : featureId;
            FeatureId = featureId;
            Geometry = feature.Geometry;
            Properties = feature.Properties;
            X = x;
            Y = y;
            Zoom = zoom;
        }

        [SetsRequiredMembers]
        public StoredFeature(int x, int y, int zoom = 11)
        {
            X = x;
            Y = y;
            Zoom = zoom;
            Id = $"empty-{zoom}-{x}-{y}";
            FeatureId = Id;
            var emptyGeometry = new Point(new Position(0, 0));
            Geometry = emptyGeometry;
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