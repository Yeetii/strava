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

    public class StoredFeature {
        public required string Id {get; set;}
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

    public class Geometry(double[] coordinates)
    {
        public double[] Coordinates { get; set; } = coordinates;
        public string Type { get; set; } = "Point";
    }
}