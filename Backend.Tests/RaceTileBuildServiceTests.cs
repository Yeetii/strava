using System.Text;

namespace Backend.Tests;

public class RaceTileBuildServiceTests
{
    [Fact]
    public void GetTippecanoeArguments_ReturnsExpectedArguments()
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
        Assert.Contains("--force", args);
        Assert.Contains(input, args);
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
