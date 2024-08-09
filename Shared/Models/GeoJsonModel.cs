namespace Shared.Models
{
    public class FeatureCollection {
        public string Type { get; set; } = "FeatureCollection";
        public required IEnumerable<Feature> Features { get; set; }
    }
    public class Feature {
        public string Type { get; set; } = "Feature";
        public Dictionary<string, string> Properties { get; set; } = [];
        public required Geometry Geometry {get; set;}
    }

    public class StoredFeature {
        public required string Id {get; set;}
        public Dictionary<string, string> Properties { get; set; } = [];
        public required Geometry Geometry {get; set;}

        public Feature ToFeature(){
            var properties = new Dictionary<string, string>{{"id", Id}}.Concat(Properties).ToDictionary();
            return new Feature{
                Properties = properties,
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