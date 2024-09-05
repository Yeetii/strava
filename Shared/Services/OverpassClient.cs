using System.Text.Json.Serialization;
using System.Text.Json;
using Shared.Models;


namespace Shared.Services
{
    public class OverpassClient(IHttpClientFactory httpClientFactory)
    {
        readonly HttpClient _client = httpClientFactory.CreateClient("overpassClient");

        public async Task<IEnumerable<RawPeaks>> GetPeaks(Coordinate southWest, Coordinate northEast)
        {
            string bbox = $"{northEast.Lat},{southWest.Lng},{southWest.Lat},{northEast.Lng}";
            string query = $"[out:json][timeout:400];node[\"natural\"=\"peak\"][\"name\"]({bbox});out qt;";
            string encodedQuery = Uri.EscapeDataString(query);
            var response = await _client.GetAsync($"?data={encodedQuery}");
            string rawPeaks = await response.Content.ReadAsStringAsync();
            RootPeaks rootPeaks = JsonSerializer.Deserialize<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
            return rootPeaks.Elements;
        }
    }

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
}