using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared.Helpers;

namespace Backend
{
    public class FetchPeaksWorker(ILogger<FetchPeaksWorker> _logger, CollectionClient<StoredFeature> _peaksCollection)
    {
        private const string overpassUri = "https://overpass-api.de/api/interpreter";
        static readonly HttpClient client = new();
        [Function(nameof(FetchPeaksWorker))]
        public async Task Run(
            [ServiceBusTrigger("peaksfetchjobs", Connection = "ServicebusConnection")] PeaksFetchJob fetchJob)
        {
            string bbox = $"{fetchJob.Lat1},{fetchJob.Lon1},{fetchJob.Lat2},{fetchJob.Lon2}";
            string query = $"[out:json][timeout:400];node[\"natural\"=\"peak\"][\"name\"]({bbox});out qt;";

            var buffer = System.Text.Encoding.UTF8.GetBytes(query);
            var byteContent = new ByteArrayContent(buffer);
            var response = await client.PostAsync(overpassUri, byteContent);
            string rawPeaks = await response.Content.ReadAsStringAsync();
            RootPeaks myDeserializedClass = JsonSerializer.Deserialize<RootPeaks>(rawPeaks) ?? throw new JsonException("Could not deserialize");
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
                    var tile = SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(x.Lon, x.Lat), 11);

                    return new StoredFeature
                    {
                        Id = x.Id.ToString(),
                        X = tile.X,
                        Y = tile.Y,
                        Properties = properties,
                        Geometry = new Geometry { Coordinates = [x.Lon, x.Lat], Type = GeometryType.Point }
                    };
                });

            await _peaksCollection.BulkUpsert(peaks);
        }
    }
}