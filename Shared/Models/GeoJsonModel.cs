using System.Text.Json.Serialization;

namespace Shared.Models
{
    public class FeatureCollection
    {
        public string Type { get; set; } = "FeatureCollection";
        public required IEnumerable<Feature> Features { get; set; }
    }
    public class Feature
    {
        public required string Id { get; set; }
        public string Type { get; set; } = "Feature";
        public Dictionary<string, object> Properties { get; set; } = [];
        public required Geometry Geometry { get; set; }
    }

    public class StoredFeature : IStoredInGrid
    {
        public required string Id { get; set; }
        public required int X { get; set; }
        public required int Y { get; set; }
        public Dictionary<string, object> Properties { get; set; } = [];
        public required Geometry Geometry { get; set; }

        public Feature ToFeature()
        {
            Properties.Add("x", X);
            Properties.Add("y", Y);
            return new Feature
            {
                Id = Id,
                Properties = Properties,
                Geometry = Geometry
            };
        }
    }

    public class GeometryType
    {
        public const string Point = "Point";
        public const string MultiPoint = "MultiPoint";
        public const string LineString = "LineString";
        public const string MultiLineString = "MultiLineString";
        public const string Polygon = "Polygon";
        public const string MultiPolygon = "MultiPolygon";
        public const string GeometryCollection = "GeometryCollection";
    }

    public class Geometry
    {
        public required double[] Coordinates { get; set; }
        public required string Type { get; set; }
    }
}