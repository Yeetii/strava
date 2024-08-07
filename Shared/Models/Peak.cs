namespace Shared.Models
{
    // Special format needed for CosmosDB spatial queries https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-query-geospatial-intro
    public class Peak {
        public required string Id {get; set;}
        public string? Elevation {get; set;}
        public string? Name {get; set;}
        public string? NameSapmi {get; set;}
        public string? NameAlt {get; set;}
        public required Point Location {get; set;}
    }

    public class Point(double[] coordinates)
    {
        public double[] Coordinates { get; set; } = coordinates;
        public string Type { get; set; } = "Point";
    }
}