using Microsoft.Azure.Functions.Worker;
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
            long? id
        );

        readonly Container Container = cosmos.GetContainer("osm-cosmos", "peaks");
        readonly ILogger Logger = logger;

        // TODO: Better solution than trying to time peak fetches, there will be some hours during night at the 1st each month were groups are missing
        [Function("RefinePeaks")]
        public async Task Run([TimerTrigger("6 10 0 1 * *")] TimerInfo myTimer)
        {
            string filePath = "./peaks.json";

            if (!File.Exists(filePath))
            {
                return;
            }
            string jsonString = await File.ReadAllTextAsync(filePath);
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
                } catch (Exception ex) {
                    Logger.LogError(ex, "{PeakId} peak not found", peak.id);
                }
            }
        }
    }
}