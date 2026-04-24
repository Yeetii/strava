using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class RaceCollectionClientTests
{
    [Theory]
    [InlineData("race:gbgtrailrun.se-0", "gbgtrailrun.se", 0)]
    [InlineData("race:gbgtrailrun.se-12", "gbgtrailrun.se", 12)]
    [InlineData("race:exgotland.se-2", "exgotland.se", 2)]
    public void TryParseRaceDocumentSlotIndex_ParsesSuffix(string id, string organizer, int expected)
    {
        Assert.True(RaceCollectionClient.TryParseRaceDocumentSlotIndex(id, organizer, out var n));
        Assert.Equal(expected, n);
    }

    [Theory]
    [InlineData("race:gbgtrailrun.se-0", "other.se")]
    [InlineData("peak:gbgtrailrun.se-0", "gbgtrailrun.se")]
    [InlineData("race:gbgtrailrun.se-x", "gbgtrailrun.se")]
    public void TryParseRaceDocumentSlotIndex_RejectsMismatch(string id, string organizer)
    {
        Assert.False(RaceCollectionClient.TryParseRaceDocumentSlotIndex(id, organizer, out _));
    }

    [Fact]
    public void TryGetHighestRaceSlotIndex_ReturnsMaxFromFeatureIds()
    {
        var key = "gbgtrailrun.se";
        var races = new List<StoredFeature>
        {
            new()
            {
                Id = $"{FeatureKinds.Race}:{key}-1",
                FeatureId = $"{key}-1",
                Kind = FeatureKinds.Race,
                X = 0,
                Y = 0,
                Zoom = 8,
                Geometry = new Point(new Position(0, 0)),
            },
            new()
            {
                Id = $"{FeatureKinds.Race}:{key}-0",
                FeatureId = $"{key}-0",
                Kind = FeatureKinds.Race,
                X = 0,
                Y = 0,
                Zoom = 8,
                Geometry = new Point(new Position(0, 0)),
            },
        };

        Assert.True(RaceCollectionClient.TryGetHighestRaceSlotIndex(key, races, out var max));
        Assert.Equal(1, max);
    }

    [Fact]
    public void TryGetHighestRaceSlotIndex_EmptyList_ReturnsFalse()
    {
        Assert.False(RaceCollectionClient.TryGetHighestRaceSlotIndex("k", [], out _));
    }
}
