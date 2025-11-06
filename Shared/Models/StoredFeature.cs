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
        public required int X { get; set; }
        public required int Y { get; set; }
        public IDictionary<string, dynamic> Properties { get; set; } = new Dictionary<string, dynamic>();
        [JsonConverter(typeof(GeometrySystemTextJsonConverter))]
        public required Geometry Geometry { get; set; }

        public StoredFeature()
        {
        }

        [SetsRequiredMembers]
        public StoredFeature(Feature feature)
        {
            Id = feature.Id.Value;
            Geometry = feature.Geometry;
            Properties = feature.Properties;
            // Geometry = System.Text.Json.JsonSerializer.Deserialize<object>(feature.Geometry.ToJson());
            var geometry = feature.Geometry;
            // Get first coordinate from geometry to determine tile
            var coordinate = geometry.Type switch
            {
                GeoJsonType.Point => new Coordinate(((Point)geometry).Coordinates.Longitude, ((Point)geometry).Coordinates.Latitude),
                GeoJsonType.LineString => new Coordinate(((LineString)geometry).Coordinates.First().Longitude, ((LineString)geometry).Coordinates.First().Latitude),
                GeoJsonType.Polygon => new Coordinate(((Polygon)geometry).Coordinates.First().Coordinates.First().Longitude, ((Polygon)geometry).Coordinates.First().Coordinates.First().Latitude),
                _ => throw new NotSupportedException($"Geometry type {geometry.Type} not supported for tile calculation")
            };

            var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coordinate, 11);
            X = x;
            Y = y;
        }

        [SetsRequiredMembers]
        public StoredFeature(int x, int y)
        {
            X = x;
            Y = y;
            Id = "empty-" + x + "-" + y;
            var emptyGeometry = new Point(new Position(0, 0));
            Geometry = emptyGeometry;
        }

        public Feature ToFeature()
        {
            var propertiesCopy = new Dictionary<string, dynamic>(Properties)
            {
                ["x"] = X.ToString(),
                ["y"] = Y.ToString()
            };

            return new Feature(
                Geometry,
                propertiesCopy,
                null,
                new FeatureId(Id)
            );
        }
    }
}