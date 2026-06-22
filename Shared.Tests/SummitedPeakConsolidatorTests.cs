using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class SummitedPeakConsolidatorTests
{
    [Fact]
    public void ConsolidateByPeakId_NormalizesPeakIdsAndMergesActivityIds()
    {
        var summitedPeaks = new[]
        {
            new SummitedPeak
            {
                Id = "summit-1",
                Name = "",
                UserId = "user-1",
                PeakId = "123",
                Elevation = null,
                ActivityIds = ["activity-1"]
            },
            new SummitedPeak
            {
                Id = "summit-2",
                Name = "Peak Name",
                UserId = "user-1",
                PeakId = "peak:123",
                Elevation = 1200,
                ActivityIds = ["activity-1", "activity-2"]
            }
        };

        var consolidated = SummitedPeakConsolidator.ConsolidateByPeakId(summitedPeaks);

        var peak = Assert.Single(consolidated);
        Assert.Equal("123", peak.PeakId);
        Assert.Equal("Peak Name", peak.Name);
        Assert.Equal(1200, peak.Elevation);
        Assert.Equal(["activity-1", "activity-2"], peak.ActivityIds.Order());
    }
}
