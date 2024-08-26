using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;

namespace Backend
{
    public class FetchPeaksWorker(ILogger<FetchPeaksWorker> _logger)
    {
        private const string overpassUri = "https://overpass-api.de/api/interpreter";
        static readonly HttpClient client = new();
        [Function(nameof(FetchPeaksWorker))]
        [CosmosDBOutput("%OsmDb%", "%PeaksContainer%", Connection = "CosmosDBConnection", PartitionKey = "/id")]
        public async Task<IEnumerable<StoredFeature>> Run(
            [ServiceBusTrigger("peaksfetchjobs", Connection = "ServicebusConnection")] PeaksFetchJob fetchJob)
        {
            string bbox = $"{fetchJob.Lat1},{fetchJob.Lon1},{fetchJob.Lat2},{fetchJob.Lon2}";
            string query = $"[out:json][timeout:400];node[\"natural\"=\"peak\"][\"name\"]({bbox});out qt;";

            var buffer = System.Text.Encoding.UTF8.GetBytes(query);
            var byteContent = new ByteArrayContent(buffer);
            var response = await client.PostAsync(overpassUri, byteContent);
            string rawPeaks = await response.Content.ReadAsStringAsync();
            RootPeaks myDeserializedClass = JsonSerializer.Deserialize<RootPeaks>(rawPeaks) ?? throw new Exception("Could not deserialize");
            _logger.LogInformation("Fetched {AmnPeaks} peaks", myDeserializedClass.Elements.Count);
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