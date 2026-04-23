using System.Text;
using RaceTileJob;

namespace RaceTileJob.Tests;

public class RaceTileBuildServiceTests
{
    [Fact]
    public void GetTippecanoeArguments_IncludesPreserveFeatureFlags()
    {
        var input = "/tmp/features.geojson";
        var output = "/tmp/trails.pmtiles";

        var args = RaceTileBuildService.GetTippecanoeArguments(input, output);

        Assert.Contains("--output", args);
        Assert.Contains(output, args);
        Assert.Contains("--layer=trails", args);
        Assert.Contains("--minimum-zoom=0", args);
        Assert.Contains("--maximum-zoom=14", args);
        Assert.Contains("--simplification=10", args);
        Assert.Contains("--cluster-distance=8", args);
        Assert.Contains("--coalesce-smallest-as-needed", args);
        Assert.Contains("--no-tile-size-limit", args);
        Assert.Contains("--no-feature-limit", args);
        Assert.Contains("--force", args);
        Assert.Contains(input, args);
        Assert.Equal(output, args[1]);
        Assert.Equal(input, args[^1]);
    }

    [Fact]
    public void IsValidPmtilesFile_ReturnsFalseForInvalidMagicHeader()
    {
        var temporaryFile = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(temporaryFile, Encoding.UTF8.GetBytes("NOTPMT").Concat(new byte[1024]).ToArray());
            Assert.False(RaceTileBuildService.IsValidPmtilesFile(temporaryFile));
        }
        finally
        {
            File.Delete(temporaryFile);
        }
    }

    [Fact]
    public void IsValidPmtilesFile_ReturnsTrueForMagicHeader()
    {
        var temporaryFile = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(temporaryFile, Encoding.UTF8.GetBytes("PMTiles")
                .Concat(new byte[1024]).ToArray());

            Assert.True(RaceTileBuildService.IsValidPmtilesFile(temporaryFile));
        }
        finally
        {
            File.Delete(temporaryFile);
        }
    }

    [Fact]
    public void ParseTippecanoeFeatureCount_ReturnsCountFromOutput()
    {
        var output = "Wrote 1700 features to /tmp/trails.pmtiles\nSome other info";

        var count = RaceTileBuildService.ParseTippecanoeFeatureCount(output);

        Assert.Equal(1700, count);
    }

    [Fact]
    public void ParseTippecanoeFeatureCount_ReturnsNegativeWhenMissing()
    {
        var output = "No feature count present";

        var count = RaceTileBuildService.ParseTippecanoeFeatureCount(output);

        Assert.Equal(-1, count);
    }

    [Fact]
    public void IsValidPmtilesFile_ReturnsFalseForTooSmallFile()
    {
        var temporaryFile = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(temporaryFile, Encoding.UTF8.GetBytes("PMTiles"));
            Assert.False(RaceTileBuildService.IsValidPmtilesFile(temporaryFile));
        }
        finally
        {
            File.Delete(temporaryFile);
        }
    }
}
