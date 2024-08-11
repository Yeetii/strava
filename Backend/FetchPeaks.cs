using System.Text.Json.Serialization;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using System.Text.Json.Nodes;

namespace Backend
{
    public class RootPeaks
    {
        [JsonPropertyName("elements")]
        public required List<RawPeaks> Elements { get; set; }
    }
        public class RawPeaks
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("lat")]
        public double Lat { get; set; }

        [JsonPropertyName("lon")]
        public double Lon { get; set; }

        [JsonPropertyName("tags")]
        public required Tags Tags { get; set; }
    }
    public class Tags
    {
        [JsonPropertyName("ele")]
        public string? Elevation { get; set; }

        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("name:sma")]
        public string? NameSapmi { get; set; }

        [JsonPropertyName("alt_name")]
        public string? NameAlt { get; set; }
    }

    public static class FetchPeaks
    {
        private const string overpassUri = "https://overpass-api.de/api/interpreter";
        static readonly HttpClient client = new();
        [Function("FetchPeaks")]
        [CosmosDBOutput("%OsmDb%", "%PeaksContainer%", Connection = "CosmosDBConnection", PartitionKey = "/id")]
        public static async Task<IEnumerable<StoredFeature>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            const string body = @"[out:json][timeout:25];" + "\n" +
            @"            node[""natural""=""peak""](61.554109444927185,11.88719122576684,65.13687800930502,16.833283564091325);" + "\n" +
            @"            out body;" + "\n" +
            @"            >;" + "\n" +
            @"            out skel qt;";
            var buffer = System.Text.Encoding.UTF8.GetBytes(body);
            var byteContent = new ByteArrayContent(buffer);
            var response = await client.PostAsync(overpassUri, byteContent);
            string rawPeaks = await response.Content.ReadAsStringAsync();
            RootPeaks myDeserializedClass = JsonSerializer.Deserialize<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
            var peaks = myDeserializedClass.Elements.Select(x => 
                {

                    var propertiesDirty = new Dictionary<string, object?>(){
                        {"elevation", x.Tags.Elevation},
                        {"name", x.Tags.Name},
                        {"nameSapmi", x.Tags.NameSapmi},
                        {"nameAlt", x.Tags.NameAlt},
                        {"groups", new Dictionary<string, bool>()}

                    };

                    var properties = propertiesDirty.Where(x => x.Value != null).ToDictionary();

                    return new StoredFeature{
                    Id = x.Id.ToString(),
                    Properties = properties,
                    Geometry= new Geometry([x.Lon, x.Lat])
                    };
                });

            return peaks;
        }
    }
}