using Backend;

namespace Backend.Tests;

public class RaceHtmlScraperTests
{
    // ── ExtractGpxUrlsFromHtml ────────────────────────────────────────────────

    [Fact]
    public void ExtractGpxUrlsFromHtml_FindsLinksFromHrefAndEscapedScriptUrls()
    {
        const string html = """
            <html>
              <body>
                <a href="/courses/utmb-50k.gpx">Download GPX</a>
                <script>
                  window.courseData = {"gpx":"https:\/\/utmb.world\/downloads\/ccc-course.gpx?download=1"};
                </script>
              </body>
            </html>
            """;

        var urls = RaceHtmlScraper.ExtractGpxUrlsFromHtml(html, new Uri("https://utmb.world/races/ccc"));

        Assert.Contains(urls, uri => uri.AbsoluteUri == "https://utmb.world/courses/utmb-50k.gpx");
        Assert.Contains(urls, uri => uri.AbsoluteUri == "https://utmb.world/downloads/ccc-course.gpx?download=1");
    }

    // ── ExtractCourseLinksFromHtml ────────────────────────────────────────────

    [Fact]
    public void ExtractCourseLinksFromHtml_FindsLinksByHrefKeyword()
    {
        const string html = """
            <html>
              <body>
                <a href="/course/info">Race information</a>
                <a href="/about">About us</a>
                <a href="/lopp/10k">10K race</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractCourseLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/course/info");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/lopp/10k");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("about"));
    }

    [Fact]
    public void ExtractCourseLinksFromHtml_FindsLinksByLinkText()
    {
        const string html = """
            <html>
              <body>
                <a href="/page1">Läs mer om loppet</a>
                <a href="/page2">See more info</a>
                <a href="/page3">Contact us</a>
                <a href="/page4">Info om banan</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractCourseLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page1");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page2");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page4");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("page3"));
    }

    [Fact]
    public void ExtractCourseLinksFromHtml_ReturnsEmptyForBlankInput()
    {
        Assert.Empty(RaceHtmlScraper.ExtractCourseLinksFromHtml("", new Uri("https://example.com/")));
    }

    // ── ExtractGpxLinksFromHtml ───────────────────────────────────────────────

    [Fact]
    public void ExtractGpxLinksFromHtml_FindsGpxByExtensionAndByLinkText()
    {
        const string html = """
            <html>
              <body>
                <a href="/routes/race.gpx">Download route</a>
                <a href="/download">Download GPX file</a>
                <a href="/info">Race information</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/routes/race.gpx");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/download");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("info"));
    }

    [Fact]
    public void ExtractGpxLinksFromHtml_DeduplicatesResults()
    {
        const string html = """
            <html>
              <body>
                <a href="/race.gpx">Download GPX</a>
                <a href="/race.gpx">GPX fil</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Single(links, u => u.AbsoluteUri == "https://example.com/race.gpx");
    }

    // ── ExtractDownloadLinksFromHtml ──────────────────────────────────────────

    [Fact]
    public void ExtractDownloadLinksFromHtml_FindsSwedishAndEnglishDownloadLinks()
    {
        const string html = """
            <html>
              <body>
                <a href="/dl1">Ladda ner</a>
                <a href="/dl2">Hämta filen</a>
                <a href="/dl3">Download file</a>
                <a href="/about">About us</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractDownloadLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl1");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl2");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl3");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("about"));
    }

    // ── ExtractRaceSiteUrl ────────────────────────────────────────────────────

    [Fact]
    public void ExtractRaceSiteUrl_FindsSiteDelaCourseLink()
    {
        const string html = """
            <html>
              <body>
                <a href="https://utmb.world/races/120K">UTMB</a>
                <a href="https://myrace.com/">Site de la course</a>
                <a href="/info">Info</a>
              </body>
            </html>
            """;

        var result = RaceHtmlScraper.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/some-event"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRaceSiteUrl_IsCaseInsensitive()
    {
        const string html = """<a href="https://myrace.com/">SITE DE LA COURSE</a>""";
        var result = RaceHtmlScraper.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRaceSiteUrl_ReturnsNullWhenNoneFound()
    {
        const string html = "<html><body><a href='/info'>Info</a></body></html>";
        Assert.Null(RaceHtmlScraper.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x")));
    }

    [Fact]
    public void ExtractRaceSiteUrl_ReturnsNullForBlankInput()
    {
        Assert.Null(RaceHtmlScraper.ExtractRaceSiteUrl("", new Uri("https://tracedetrail.fr/en/event/x")));
    }

    // ── ExtractRunagainEventLinks ─────────────────────────────────────────────

    [Fact]
    public void ExtractRunagainEventLinks_FindsEventHrefs()
    {
        const string html = """
            <html>
              <body>
                <a href="/event/ultra-trail-bergen-2025">Ultra Trail Bergen</a>
                <a href="/event/oslo-maraton-50k">Oslo Maraton 50K</a>
                <a href="/find-event?race_type=Stiløp&p=2">Next page</a>
                <a href="/about">About</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractRunagainEventLinks(html, new Uri("https://runagain.com/"));

        Assert.Equal(2, links.Count);
        Assert.Contains(links, u => u.AbsoluteUri == "https://runagain.com/event/ultra-trail-bergen-2025");
        Assert.Contains(links, u => u.AbsoluteUri == "https://runagain.com/event/oslo-maraton-50k");
    }

    [Fact]
    public void ExtractRunagainEventLinks_DeduplicatesDuplicateLinks()
    {
        const string html = """
            <html>
              <body>
                <a href="/event/my-race">My Race</a>
                <a href="/event/my-race">My Race (again)</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractRunagainEventLinks(html, new Uri("https://runagain.com/"));
        Assert.Single(links);
    }

    [Fact]
    public void ExtractRunagainEventLinks_ReturnsEmptyForBlankInput()
    {
        Assert.Empty(RaceHtmlScraper.ExtractRunagainEventLinks("", new Uri("https://runagain.com/")));
    }

    [Fact]
    public void ExtractRunagainEventLinks_ReturnsEmptyWhenNoEventLinks()
    {
        const string html = """<a href="/find-event?race_type=Stiløp&p=2">Next</a>""";
        Assert.Empty(RaceHtmlScraper.ExtractRunagainEventLinks(html, new Uri("https://runagain.com/")));
    }

    // ── ExtractRunagainSiteUrl ────────────────────────────────────────────────

    [Fact]
    public void ExtractRunagainSiteUrl_FindsNorwegianHjemmesideLink()
    {
        const string html = """
            <html>
              <body>
                <a href="https://ultratrail.no/">Hjemmeside</a>
                <a href="/event/other">Other event</a>
              </body>
            </html>
            """;

        var result = RaceHtmlScraper.ExtractRunagainSiteUrl(html, new Uri("https://runagain.com/event/ultra-trail-2025"));
        Assert.Equal("https://ultratrail.no/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRunagainSiteUrl_FindsEnglishOfficialWebsiteLink()
    {
        const string html = """
            <a href="https://myrace.com/">Official website</a>
            """;

        var result = RaceHtmlScraper.ExtractRunagainSiteUrl(html, new Uri("https://runagain.com/event/my-race"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRunagainSiteUrl_IgnoresSameHostLinks()
    {
        const string html = """
            <a href="https://runagain.com/about">Nettside</a>
            """;

        var result = RaceHtmlScraper.ExtractRunagainSiteUrl(html, new Uri("https://runagain.com/event/my-race"));
        Assert.Null(result);
    }

    [Fact]
    public void ExtractRunagainSiteUrl_ReturnsNullWhenNoneFound()
    {
        const string html = """<a href="https://example.com/">Contact us</a>""";
        Assert.Null(RaceHtmlScraper.ExtractRunagainSiteUrl(html, new Uri("https://runagain.com/event/x")));
    }

    [Fact]
    public void ExtractRunagainSiteUrl_ReturnsNullForBlankInput()
    {
        Assert.Null(RaceHtmlScraper.ExtractRunagainSiteUrl("", new Uri("https://runagain.com/event/x")));
    }
}
