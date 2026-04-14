using Shared.Models;
using Shared.Services;

namespace API.Utils;

internal static class SummitedPeakConsolidator
{
    public static List<SummitedPeak> ConsolidateByPeakId(IEnumerable<SummitedPeak> summitedPeaks)
    {
        return summitedPeaks
            .GroupBy(peak => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, peak.PeakId))
            .Select(group =>
            {
                var first = group.First();
                var peakId = group.Key;

                return new SummitedPeak
                {
                    Id = first.Id,
                    Name = group.Select(peak => peak.Name).FirstOrDefault(name => !string.IsNullOrWhiteSpace(name)) ?? first.Name,
                    UserId = first.UserId,
                    PeakId = peakId,
                    Elevation = group.Select(peak => peak.Elevation).FirstOrDefault(elevation => elevation.HasValue) ?? first.Elevation,
                    ActivityIds = group
                        .SelectMany(peak => peak.ActivityIds)
                        .ToHashSet()
                };
            })
            .ToList();
    }
}