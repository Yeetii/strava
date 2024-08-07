using System.Text.Json.Serialization;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;

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
        public static async Task<IEnumerable<Peak>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            const string body = @"[out:json][timeout:25];" + "\n" +
            @"            node[""natural""=""peak""](61.91827102335593,10.88216959652901,64.5578812115091,18.87472330746651);" + "\n" +
            @"            out body;" + "\n" +
            @"            >;" + "\n" +
            @"            out skel qt;";
            var buffer = System.Text.Encoding.UTF8.GetBytes(body);
            var byteContent = new ByteArrayContent(buffer);
            var response = await client.PostAsync(overpassUri, byteContent);
            string rawPeaks = await response.Content.ReadAsStringAsync();
            RootPeaks myDeserializedClass = JsonSerializer.Deserialize<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
            var peaks = myDeserializedClass.Elements.Select(x => 
                new Peak{
                    Id = x.Id.ToString(), 
                    Elevation = x.Tags.Elevation, 
                    Name = x.Tags.Name, 
                    NameSapmi = x.Tags.NameSapmi, 
                    NameAlt = x.Tags.NameAlt, 
                    Location= new Point([x.Lon, x.Lat])
                });

            return peaks;
        }
    }
}