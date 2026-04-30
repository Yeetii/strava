using Shared.Services;

namespace Shared.Tests;

public class RaceDistanceKmTests
{
    [Theory]
    [InlineData(509, 511, true)]
    [InlineData(45, 50, false)]
    public void RoughlyEqualSymmetric_UsesDefaultThreePercent(double a, double b, bool expected) =>
        Assert.Equal(expected, RaceDistanceKm.RoughlyEqualSymmetric(a, b));

    [Fact]
    public void WithinRelativeOfReference_QuarterBand()
    {
        Assert.True(RaceDistanceKm.WithinRelativeOfReference(100, 110, 0.25));
        Assert.False(RaceDistanceKm.WithinRelativeOfReference(100, 130, 0.25));
        Assert.False(RaceDistanceKm.WithinRelativeOfReference(0, 10, 0.25));
    }

    [Theory]
    [InlineData("33 km", 33.0)]
    [InlineData("50 km, 33 km", 50.0)]
    [InlineData("0,8 km, 5,3 km", 0.8)]
    public void TryParsePrimarySegmentKilometers(string? input, double expected) =>
        Assert.Equal(expected, RaceDistanceKm.TryParsePrimarySegmentKilometers(input));

    [Fact]
    public void TryParsePrimarySegmentKilometers_NullInput_ReturnsNull() =>
        Assert.Null(RaceDistanceKm.TryParsePrimarySegmentKilometers(null));

    [Fact]
    public void ParseCommaSeparatedKilometers_ParsesTokens()
    {
        var list = RaceDistanceKm.ParseCommaSeparatedKilometers("50 km, 33 km, 21 km");
        Assert.Equal(new[] { 50.0, 33.0, 21.0 }, list);
    }

    [Fact]
    public void ParseCommaSeparatedKilometers_ParsesDecimalCommas()
    {
        var list = RaceDistanceKm.ParseCommaSeparatedKilometers("0,8 km, 5,3 km, 10,6 km");
        Assert.Equal(new[] { 0.8, 5.3, 10.6 }, list);
    }

    [Fact]
    public void ParseCommaSeparatedKilometers_BareNumericToken()
    {
        var list = RaceDistanceKm.ParseCommaSeparatedKilometers("13.5, 21 km");
        Assert.Equal(new[] { 13.5, 21.0 }, list);
    }

    [Fact]
    public void ParseCommaSeparatedKilometers_MarathonMilesAndCapitalMForMiles()
    {
        var list = RaceDistanceKm.ParseCommaSeparatedKilometers("Marathon, 10 km, 6 mi, 10M");
        Assert.Equal(42.0, list[0]);
        Assert.Equal(10.0, list[1]);
        Assert.InRange(list[2], 9.655, 9.657);
        Assert.InRange(list[3], 16.092, 16.094);
    }

    [Fact]
    public void TryParsePrimarySegmentKilometers_MarathonMilesCapitalM()
    {
        Assert.Equal(42.0, RaceDistanceKm.TryParsePrimarySegmentKilometers("Marathon"));
        Assert.InRange(RaceDistanceKm.TryParsePrimarySegmentKilometers("6 mi")!.Value, 9.655, 9.657);
        Assert.InRange(RaceDistanceKm.TryParsePrimarySegmentKilometers("10M")!.Value, 16.092, 16.094);
    }

    [Theory]
    [InlineData("400m", 0.4)]
    [InlineData("500 m", 0.5)]
    [InlineData("1000 meters", 1.0)]
    [InlineData("750 metres", 0.75)]
    [InlineData("800 metre", 0.8)]
    public void TryParseCommaListToken_MetresSuffix(string token, double expectedKm)
    {
        Assert.True(RaceDistanceKm.TryParseCommaListTokenKilometers(token, out var km));
        Assert.Equal(expectedKm, km, 9);
    }

    [Theory]
    [InlineData("marathon", 42)]
    [InlineData("Half Marathon", 21)]
    public void TryParseMarathonKeyword(string token, double km)
    {
        Assert.True(RaceDistanceKm.TryParseMarathonKeyword(token, out var parsed));
        Assert.Equal(km, parsed);
    }
}
