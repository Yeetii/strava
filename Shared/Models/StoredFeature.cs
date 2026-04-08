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
            var coordinate = GetRepresentativeCoordinate(feature.Geometry);

            var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coordinate, 11);
            X = x;
            Y = y;
        }

        private static Coordinate GetRepresentativeCoordinate(Geometry geometry)
        {
            return geometry switch
            {
                Point point => new Coordinate(point.Coordinates.Longitude, point.Coordinates.Latitude),
                LineString lineString => new Coordinate(lineString.Coordinates.First().Longitude, lineString.Coordinates.First().Latitude),
                Polygon polygon => new Coordinate(
                    polygon.Coordinates.First().Coordinates.First().Longitude,
                    polygon.Coordinates.First().Coordinates.First().Latitude),
                MultiPolygon multiPolygon => new Coordinate(
                    multiPolygon.Coordinates.First().Coordinates.First().Coordinates.First().Longitude,
                    multiPolygon.Coordinates.First().Coordinates.First().Coordinates.First().Latitude),
                _ => throw new NotSupportedException($"Geometry type {geometry.Type} not supported for tile calculation")
            };
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