using Microsoft.Azure.Functions.Worker;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services.Postgres;

namespace Backend
{
    public class StoreCuratedPeaksGroups(ILogger<StoreCuratedPeaksGroups> _logger, IPeaksGroupRepository peaksGroupRepository)
    {
        private sealed record RefinedPeak(
            string name,
            int elevation,
            string area,
            long? id
        );


        [Function(nameof(StoreCuratedPeaksGroups))]
        public async Task<IEnumerable<PeaksGroup>?> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "peaks/refine")] HttpRequestData req)
        {
            string filePath = "./peaks.json";

            if (!File.Exists(filePath))
            {
                _logger.LogError("File not found: {path}", filePath);
                return default;
            }
            string jsonString = await File.ReadAllTextAsync(filePath);
            var featuresList = JsonSerializer.Deserialize<IEnumerable<RefinedPeak>>(jsonString);

            if (featuresList == null)
            {
                _logger.LogError("Failed to deserialize JSON string");
                return default;
            }


            var groups = new List<PeaksGroup>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                    Name = "Jämtlands fjälltoppar",
                    AmountOfPeaks = featuresList.Count(),
                    PeakIds = featuresList.Select(peak => peak.id.ToString()!).ToArray(),
                    Boundrary = null
                }
            };

            featuresList.GroupBy(peak => peak.area).ToList().ForEach(area =>
            {
                var group = new PeaksGroup()
                {
                    Id = Guid.NewGuid(),
                    ParentId = groups.First().Id,
                    Name = area.Key.EndsWith('s') ? area.Key + " fjälltoppar" : area.Key + "s fjälltoppar",
                    AmountOfPeaks = area.Count(),
                    PeakIds = area.Select(peak => peak.id.ToString()!).ToArray(),
                    Boundrary = null
                };
                groups.Add(group);
            });

            // Replace the implicit CosmosDBOutput write: persist the parent first, then each child
            // with its ParentId through the peaks_groups repository.
            foreach (var group in groups)
                await peaksGroupRepository.Upsert(group);

            return groups;
        }
    }
}
