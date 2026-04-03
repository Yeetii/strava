using Shared.Models;

namespace API.Utils;

internal static class SummitedPeakConsolidator
{
    public static List<SummitedPeak> ConsolidateByPeakId(IEnumerable<SummitedPeak> summitedPeaks)
    {
        return summitedPeaks
            .GroupBy(peak => peak.PeakId)
            .Select(group =>
            {
                var first = group.First();

                return new SummitedPeak
                {
                    Id = first.Id,
                    Name = group.Select(peak => peak.Name).FirstOrDefault(name => !string.IsNullOrWhiteSpace(name)) ?? first.Name,
                    UserId = first.UserId,
                    PeakId = first.PeakId,
                    Elevation = group.Select(peak => peak.Elevation).FirstOrDefault(elevation => elevation.HasValue) ?? first.Elevation,
                    ActivityIds = group
                        .SelectMany(peak => peak.ActivityIds)
                        .ToHashSet()
                };
            })
            .ToList();
    }
}