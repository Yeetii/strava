using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Microsoft.Azure.Cosmos;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Backend
{
    public class RefinePeaks(CosmosClient cosmos, ILogger<RefinePeaks> logger)
    {
        private record RefinedPeak(
            string name,
            int elevation,
            string area,
            long? id,
            double lat,
            double lon
        );

        readonly Container Container = cosmos.GetContainer("osm-cosmos", "peaks");
        readonly ILogger Logger = logger;

        [Function("RefinePeaks")]
        public async Task Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            // Path to your local JSON file
            string filePath = "./peaks.json";

            // Ensure the file exists
            if (!File.Exists(filePath))
            {
                return;
            }

            // Read the file asynchronously
            string jsonString = await File.ReadAllTextAsync(filePath);

            // Deserialize the JSON string into a list of StoredFeature objects
            // Adjust the type parameter based on the actual structure of your JSON data
            var featuresList = JsonSerializer.Deserialize<IEnumerable<RefinedPeak>>(jsonString);

            foreach (var peak in featuresList){
                if (peak.id == null) continue;

                try {
                    await Container.PatchItemAsync<StoredFeature>(
                        id: peak.id.ToString(),
                        partitionKey: new PartitionKey(peak.id.ToString()),
                        patchOperations: [
                            PatchOperation.Set("/properties/groups/Jämtlands fjälltoppar", true),
                            PatchOperation.Set($"/properties/groups/{peak.area}", true)
                        ]
                    );
                } catch {
                    Logger.LogError("{peakId} peak not found", peak.id);
                }
                }
        }
    }
}