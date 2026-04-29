using System.Text;
using Microsoft.Extensions.Configuration;
using PmtilesJob;

namespace PmtilesJob.Tests;

public class PmtilesUtilityServiceTests
{
    [Fact]
    public void Parse_ReturnsFilterOutdoorCommand_WhenFilterSubcommandIsProvided()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["filter-outdoor", "--input", "/tmp/world.pmtiles", "--output", "/tmp/outdoors.pmtiles"],
            configuration);

        Assert.Equal(PmtilesCommandKind.FilterOutdoor, command.Command);
        Assert.Equal("/tmp/world.pmtiles", command.InputPath);
        Assert.Equal("/tmp/outdoors.pmtiles", command.OutputPath);
        Assert.Null(command.MaximumZoom);
        Assert.False(command.ExcludeAllAttributes);
    }

    [Fact]
    public void Parse_AllowsFilterOutdoorPerformanceOptions()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["filter-outdoor", "--input", "/tmp/world.pmtiles", "--output", "/tmp/outdoors.pmtiles", "--max-zoom", "8", "--exclude-all-attributes"],
            configuration);

        Assert.Equal(PmtilesCommandKind.FilterOutdoor, command.Command);
        Assert.Equal(8, command.MaximumZoom);
        Assert.True(command.ExcludeAllAttributes);
    }

    [Fact]
    public void Parse_ReturnsFilterAdminBoundariesCommand_WhenSubcommandIsProvided()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["filter-admin-boundaries", "--input", "/tmp/world.pmtiles", "--output", "/tmp/boundaries.pmtiles"],
            configuration);

        Assert.Equal(PmtilesCommandKind.FilterAdminBoundaries, command.Command);
        Assert.Equal("/tmp/world.pmtiles", command.InputPath);
        Assert.Equal("/tmp/boundaries.pmtiles", command.OutputPath);
    }

    [Fact]
    public void Parse_ReturnsBuildAdminAreasCommand_WhenSubcommandIsProvided()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["build-admin-areas", "--output", "/tmp/regions.pmtiles", "--admin-level", "4"],
            configuration);

        Assert.Equal(PmtilesCommandKind.BuildAdminAreas, command.Command);
        Assert.Equal("/tmp/regions.pmtiles", command.OutputPath);
        Assert.Equal([4], command.AdminLevels);
    }

    [Fact]
    public void Parse_DefaultsBuildAdminAreasToCountriesAndRegions()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["build-admin-areas", "--output", "/tmp/regions.pmtiles"],
            configuration);

        Assert.Equal(PmtilesCommandKind.BuildAdminAreas, command.Command);
        Assert.Equal([2, 4], command.AdminLevels);
    }

    [Fact]
    public void Parse_AllowsMultipleAdminLevelsForBuildAdminAreas()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse(
            ["build-admin-areas", "--output", "/tmp/regions.pmtiles", "--admin-levels", "4,2,4"],
            configuration);

        Assert.Equal(PmtilesCommandKind.BuildAdminAreas, command.Command);
        Assert.Equal([2, 4], command.AdminLevels);
    }

    [Fact]
    public void Parse_ReturnsBuildRaceTilesFromOrganizers_WhenNoSubcommandIsProvided()
    {
        var configuration = new ConfigurationBuilder().Build();

        var command = PmtilesCommandLine.Parse([], configuration);

        Assert.Equal(PmtilesCommandKind.BuildRaceTilesFromOrganizers, command.Command);
    }

    [Fact]
    public void Parse_Throws_WhenFilterOutdoorInputIsMissing()
    {
        var configuration = new ConfigurationBuilder().Build();

        var ex = Assert.Throws<InvalidOperationException>((Action)(() =>
            PmtilesCommandLine.Parse(["filter-outdoor", "--output", "/tmp/outdoors.pmtiles"], configuration)));

        Assert.Contains("--input", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ResolveBinaryPath_PrefersExistingFallback_WhenConfiguredPathIsInvalid()
    {
        var temporaryBinary = Path.GetTempFileName();

        try
        {
            var resolved = PmtilesUtilityService.ResolveBinaryPath("/does/not/exist/tile-join", temporaryBinary);

            Assert.Equal(temporaryBinary, resolved);
        }
        finally
        {
            File.Delete(temporaryBinary);
        }
    }

    [Fact]
    public void ResolveBinaryPath_PreservesConfiguredPath_WhenNoFallbackExists()
    {
        var configuredPath = "/does/not/exist/tile-join";

        var resolved = PmtilesUtilityService.ResolveBinaryPath(configuredPath, "/also/missing/tile-join");

        Assert.Equal(configuredPath, resolved);
    }

    [Fact]
    public void GetTileJoinTemporaryMbtilesPath_UsesMbtilesExtension()
    {
        var temporaryPath = PmtilesUtilityService.GetTileJoinTemporaryMbtilesPath("/tmp/admin-boundaries.pmtiles");

        Assert.EndsWith(".mbtiles", temporaryPath, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("admin-boundaries.", temporaryPath, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GetTippecanoeArguments_IncludesConfiguredTippecanoeFlags()
    {
        var input = "/tmp/features.geojson";
        var output = "/tmp/trails.pmtiles";

        var args = PmtilesUtilityService.GetTippecanoeArguments(input, output);

        Assert.Contains("--output", args);
        Assert.Contains(output, args);
        Assert.Contains("--layer=trails", args);
        Assert.Contains("--minimum-zoom=0", args);
        Assert.Contains("-zg", args);
        Assert.Contains("-r1", args);
        Assert.Contains("--no-tile-size-limit", args);
        Assert.Contains("--no-feature-limit", args);
        Assert.Contains("--drop-densest-as-needed", args);
        Assert.Contains("--force", args);
        Assert.Contains(input, args);
        Assert.Equal(output, args[1]);
        Assert.Equal(input, args[^1]);
    }

    [Fact]
    public void GetTippecanoeArguments_AllowsMultipleNamedLayers()
    {
        var output = "/tmp/admin-areas.pmtiles";

        var args = PmtilesUtilityService.GetTippecanoeArguments(
            [("countries", "/tmp/countries.geojson"), ("regions", "/tmp/regions.geojson")],
            output);

        Assert.Contains("--output", args);
        Assert.Contains(output, args);
        Assert.Contains("--named-layer=countries:/tmp/countries.geojson", args);
        Assert.Contains("--named-layer=regions:/tmp/regions.geojson", args);
        Assert.DoesNotContain(args, static arg => arg.StartsWith("--layer=", StringComparison.Ordinal));
    }

    [Fact]
    public void GetOutdoorMapFilterArguments_IncludesOnlyOutdoorLayers()
    {
        var input = "/tmp/world.pmtiles";
        var output = "/tmp/outdoors.pmtiles";

        var args = PmtilesUtilityService.GetOutdoorMapFilterArguments(input, output);

        Assert.Equal("-o", args[0]);
        Assert.Equal(output, args[1]);
        Assert.Contains("-pg", args);
        Assert.Contains("-l", args);
        Assert.Contains("-j", args);
        foreach (var layer in PmtilesUtilityService.OutdoorMapIncludedLayers)
        {
            Assert.Contains(layer, args);
        }

        var filterJsonIndex = Array.IndexOf(args.ToArray(), "-j");
        Assert.True(filterJsonIndex >= 0);
        var filterJson = args[filterJsonIndex + 1];
        Assert.Contains("\"places\"", filterJson);
        foreach (var kind in PmtilesUtilityService.OutdoorMapIncludedPlaceKinds)
        {
            Assert.Contains($"\"{kind}\"", filterJson);
        }

        Assert.DoesNotContain("\"neighbourhood\"", filterJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("\"suburb\"", filterJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("\"quarter\"", filterJson, StringComparison.OrdinalIgnoreCase);

        foreach (var layer in PmtilesUtilityService.OutdoorMapExcludedLayers)
        {
            Assert.DoesNotContain(layer, args);
        }

        Assert.Equal(input, args[^1]);
    }

    [Fact]
    public void GetOutdoorMapFilterArguments_AllowsMaximumZoomAndAttributeStripping()
    {
        var args = PmtilesUtilityService.GetOutdoorMapFilterArguments(
            "/tmp/world.pmtiles",
            "/tmp/outdoors.pmtiles",
            maximumZoom: 8,
            excludeAllAttributes: true);

        Assert.Contains("-z", args);
        Assert.Contains("8", args);
        Assert.Contains("-X", args);
    }

    [Fact]
    public void GetAdminBoundariesFilterArguments_KeepsOnlyBoundaryLayerAndSupportedBoundaryKinds()
    {
        var input = "/tmp/world.pmtiles";
        var output = "/tmp/admin-boundaries.pmtiles";

        var args = PmtilesUtilityService.GetAdminBoundariesFilterArguments(input, output);

        Assert.Equal("-o", args[0]);
        Assert.Equal(output, args[1]);
        Assert.Contains("-l", args);
        Assert.Contains("boundaries", args);
        Assert.Contains("-j", args);
        Assert.DoesNotContain("roads", args);
        Assert.DoesNotContain("places", args);
        Assert.DoesNotContain("natural", args);

        var filterJsonIndex = Array.IndexOf(args.ToArray(), "-j");
        Assert.True(filterJsonIndex >= 0);
        var filterJson = args[filterJsonIndex + 1];
        Assert.Contains("\"boundaries\"", filterJson);
        foreach (var kind in PmtilesUtilityService.AdminBoundariesIncludedKinds)
        {
            Assert.Contains($"\"{kind}\"", filterJson);
        }

        Assert.DoesNotContain("\"map_unit\"", filterJson);
        Assert.DoesNotContain("\"unrecognized_country\"", filterJson);
        Assert.Equal(input, args[^1]);
    }

    [Fact]
    public void GetTileJoinArguments_ThrowsWhenBothIncludeAndExcludeAreSpecified()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            PmtilesUtilityService.GetTileJoinArguments(
                "/tmp/world.pmtiles",
                "/tmp/outdoors.pmtiles",
                includeLayers: ["natural"],
                excludeLayers: ["buildings"]));

        Assert.Contains("either include layers or exclude layers", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IsValidPmtilesFile_ReturnsFalseForInvalidMagicHeader()
    {
        var temporaryFile = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(temporaryFile, Encoding.UTF8.GetBytes("NOTPMT").Concat(new byte[1024]).ToArray());
            Assert.False(PmtilesUtilityService.IsValidPmtilesFile(temporaryFile));
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

            Assert.True(PmtilesUtilityService.IsValidPmtilesFile(temporaryFile));
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

        var count = PmtilesUtilityService.ParseTippecanoeFeatureCount(output);

        Assert.Equal(1700, count);
    }

    [Fact]
    public void ParseTippecanoeFeatureCount_ReturnsNegativeWhenMissing()
    {
        var output = "No feature count present";

        var count = PmtilesUtilityService.ParseTippecanoeFeatureCount(output);

        Assert.Equal(-1, count);
    }

    [Fact]
    public void IsValidPmtilesFile_ReturnsFalseForTooSmallFile()
    {
        var temporaryFile = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(temporaryFile, Encoding.UTF8.GetBytes("PMTiles"));
            Assert.False(PmtilesUtilityService.IsValidPmtilesFile(temporaryFile));
        }
        finally
        {
            File.Delete(temporaryFile);
        }
    }
}
