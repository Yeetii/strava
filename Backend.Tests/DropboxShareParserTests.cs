using System.IO.Compression;
using System.Text;
using Backend.Scrapers;
using Microsoft.Extensions.Logging.Abstractions;

namespace Backend.Tests;

public class DropboxShareParserTests
{
    [Fact]
    public void WithDl1_replaces_dl0_preserving_other_query()
    {
        var u = new Uri("https://www.dropbox.com/scl/fo/abc/xyz?rlkey=secret&dl=0&e=1");
        var r = DropboxShareParser.WithDl1(u);
        Assert.Contains("dl=1", r.Query, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("dl=0", r.Query, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("rlkey=secret", r.Query, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ToSharedFolderEntryUri_preserves_folder_query_and_adds_fragment()
    {
        var folder = new Uri("https://www.dropbox.com/sh/npbmlzm6saso1bc/AADPKBDsvV2ynm89zKN7DpT8a?rlkey=abc&dl=0");
        var entry = DropboxShareParser.ToSharedFolderEntryUri(folder, "tracks/MMC 100K 2024.gpx");
        Assert.StartsWith("https://www.dropbox.com/sh/", entry.GetLeftPart(UriPartial.Path), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("rlkey=abc", entry.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MMC%20100K%202024.gpx", entry.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WithDl1_appends_dl_when_absent()
    {
        var u = new Uri("https://www.dropbox.com/s/abc/file.gpx");
        var r = DropboxShareParser.WithDl1(u);
        Assert.Contains("dl=1", r.Query, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("https://www.dropbox.com/scl/fo/x/y", true)]
    [InlineData("https://www.dropbox.com/sh/abcd/xyz", true)]
    [InlineData("https://www.dropbox.com/s/abc/file.gpx", false)]
    [InlineData("https://www.dropbox.com/scl/fi/x/y", false)]
    [InlineData("https://drive.google.com/drive/folders/abc", false)]
    public void IsDropboxSharedFolder_classification(string url, bool folder)
    {
        var u = new Uri(url);
        Assert.Equal(folder, DropboxShareParser.IsDropboxSharedFolder(u));
    }

    [Fact]
    public void ExtractGpxFromZip_returns_only_gpx_entries()
    {
        using var ms = new MemoryStream();
        using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            var g = zip.CreateEntry("tracks/a.gpx");
            using (var w = new StreamWriter(g.Open(), Encoding.UTF8))
                w.Write("<?xml version=\"1.0\"?><gpx><trk><trkseg><trkpt lat=\"1\" lon=\"2\"/></trkseg></trk></gpx>");
            var d = zip.CreateEntry("readme.txt");
            using (var w = new StreamWriter(d.Open(), Encoding.UTF8))
                w.Write("no");
        }

        var list = DropboxShareParser.ExtractGpxFromZip(ms.ToArray());
        Assert.Single(list);
        Assert.Contains("<gpx>", list[0].GpxXml, StringComparison.Ordinal);
    }

}
