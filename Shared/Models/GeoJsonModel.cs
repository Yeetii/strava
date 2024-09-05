using System.Text.Json.Serialization;

namespace Shared.Models
{
    public class FeatureCollection {
        public string Type { get; set; } = "FeatureCollection";
        public required IEnumerable<Feature> Features { get; set; }
    }
    public class Feature {
        public required string Id {get; set;}
        public string Type { get; set; } = "Feature";
        public Dictionary<string, object> Properties { get; set; } = [];
        public required Geometry Geometry {get; set;}
    }

    public class StoredFeature : IStoredInGrid {
        public required string Id {get; set;}
        public required int X {get; set;}
        public required int Y {get; set;}
        public Dictionary<string, object> Properties { get; set; } = [];
        public required Geometry Geometry {get; set;}

        public Feature ToFeature(){
            return new Feature{
                Id = Id,
                Properties = Properties,
                Geometry = Geometry
            };
        }
    }

    public enum GeometryType{
        Point,
        MultiPoint,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
        GeometryCollection
    }

    public class Geometry
    {
        public required double[] Coordinates { get; set; }
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public required GeometryType Type { get; set; }
    }
}