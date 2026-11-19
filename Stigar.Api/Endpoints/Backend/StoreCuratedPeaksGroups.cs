using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api.Endpoints.Backend
{
    public class StoreCuratedPeaksGroups(ILogger<StoreCuratedPeaksGroups> _logger, IPeaksGroupRepository peaksGroupRepository)
    {
        private sealed record RefinedPeak(
            string name,
            int elevation,
            string area,
            long? id
        );
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/peaks/refine", (HttpContext context, [FromServices] StoreCuratedPeaksGroups endpoint) => endpoint.Handle(context));

        public async Task<IResult> Handle(HttpContext context)
        {
            string filePath = "./peaks.json";

            if (!File.Exists(filePath))
            {
                _logger.LogError("File not found: {path}", filePath);
                return TypedResults.NotFound();
            }
            string jsonString = await File.ReadAllTextAsync(filePath);
            var featuresList = JsonSerializer.Deserialize<IEnumerable<RefinedPeak>>(jsonString);

            if (featuresList == null)
            {
                _logger.LogError("Failed to deserialize JSON string");
                return TypedResults.NotFound();
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

            // Persist each curated group through the peaks_groups repository.
            foreach (var group in groups)
                await peaksGroupRepository.Upsert(group);

            return TypedResults.Ok(groups);
        }
    }
}
