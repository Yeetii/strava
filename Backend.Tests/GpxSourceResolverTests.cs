using Backend.Scrapers;

namespace Backend.Tests;

public class GpxSourceResolverTests
{
    private static readonly Uri Origin = new("https://mmctrail.no/100m");

    [Theory]
    [InlineData("https://www.dropbox.com/sh/abc/x?dl=0", GpxSourceKind.Dropbox)]
    [InlineData("https://www.dropbox.com/sh/abc/x?dl=0#tracks%2Froute.gpx", GpxSourceKind.Dropbox)]
    [InlineData("https://dl.dropboxusercontent.com/s/abc/file.gpx", GpxSourceKind.Dropbox)]
    [InlineData("https://drive.google.com/uc?export=download&id=1", GpxSourceKind.GoogleDrive)]
    [InlineData("https://mmctrail.no/files/route.gpx", GpxSourceKind.InternalGpx)]
    [InlineData("https://www.mmctrail.no/files/route.gpx", GpxSourceKind.InternalGpx)]
    [InlineData("https://tracedetrail.fr/gpx/foo.gpx", GpxSourceKind.ExternalGpx)]
    [InlineData("https://app.racedaymap.com/vgl-trail-2025/abc123/gpx/10km.gpx", GpxSourceKind.RaceDayMap)]
    public void Resolve_classifies_by_host_and_path(string gpx, string expected)
    {
        var u = new Uri(gpx);
        Assert.Equal(expected, GpxSourceResolver.Resolve(u, Origin));
    }

    [Fact]
    public void Resolve_returns_null_for_null_gpx_url()
    {
        Assert.Null(GpxSourceResolver.Resolve(null, Origin));
    }
}
