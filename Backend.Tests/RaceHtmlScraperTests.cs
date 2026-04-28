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

    [Fact]
    public void ExtractGpxLinksFromHtml_FindsDropboxFolder_when_anchor_has_no_gpx_word()
    {
        const string html = """
            <html><body>
              <p>GPX from the <a href="https://www.dropbox.com/sh/npbmlzm6saso1bc/AADPKBDsvV2ynm89zKN7DpT8a?dl=0">download area</a>.</p>
            </body></html>
            """;

        var links = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, new Uri("https://mmctrail.no/100m"));

        Assert.Contains(links, u => u.Host.Contains("dropbox", StringComparison.OrdinalIgnoreCase));
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
    public void ExtractCourseLinksFromHtml_FindsLinksWithUppercaseMDistancePattern()
    {
        const string html = """
            <html>
              <body>
                <a href="/lopp/100M">100M race</a>
                <a href="/lopp/50k">50K race</a>
              </body>
            </html>
            """;

        var links = RaceHtmlScraper.ExtractCourseLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/lopp/100M");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/lopp/50k");
    }

    [Fact]
    public void ExtractDistanceFromUrl_ParsesUppercaseMileMarker()
    {
        var distance = RaceHtmlScraper.ExtractDistanceFromUrl(new Uri("https://example.com/race/50M"));
        Assert.Equal("80.5 km", distance);
    }

    [Fact]
    public void ExtractRaceSiteUrl_IsCaseInsensitive()
    {
        const string html = """<a href="https://myrace.com/">SITE DE LA COURSE</a>""";
        var result = RaceHtmlScraper.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRaceSiteUrl_HandlesHrefWithoutScheme()
    {
        const string html = """<a href="www.ungetuem.at">Site de la course</a>""";
        var result = RaceHtmlScraper.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x"));
        Assert.Equal("https://www.ungetuem.at/", result?.AbsoluteUri);
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

    [Fact]
    public void ExtractElevationGain_IgnoresDistanceLineBeforeSwedishElevationKeyword()
    {
        const string html = """
            <div>Längd: 42 195 meter</div>
            <div>Höjdmeter: Total stigning</div>
            """;

        var gain = RaceHtmlScraper.ExtractElevationGain(html);

        Assert.Null(gain);
    }

    [Fact]
    public void ExtractElevationGain_DiscardWhenGreaterThanHalfDistance()
    {
        const string html = """
            <div>Distance: 40 km</div>
            <div>Elevation gain: 23000 m</div>
            """;

        var gain = RaceHtmlScraper.ExtractElevationGain(html);

        Assert.Null(gain);
    }

    [Fact]
    public void ExtractElevationGain_ReturnsReasonableValueWhenWithinHalfDistance()
    {
        const string html = """
            <div>Distance: 40 km</div>
            <div>Elevation gain: 3000 m</div>
            """;

        var gain = RaceHtmlScraper.ExtractElevationGain(html);

        Assert.Equal(3000, gain);
    }

    [Fact]
    public void ExtractElevationGain_ParsesSignedValueWithoutUnit()
    {
        const string html = """
            <div>Elevation Gain: <span style="font-weight:bold">+607</span></div>
            """;

        var gain = RaceHtmlScraper.ExtractElevationGain(html);

        Assert.Equal(607, gain);
    }

    [Fact]
    public void ExtractDate_ParsesSwedishDateWithTimeSuffix()
    {
        const string html = """
            <div>5 september 2026 kl. 10:00</div>
            """;

        var date = RaceHtmlScraper.ExtractDate(html);

        Assert.Equal("2026-09-05", date);
    }

    [Fact]
    public void ExtractDate_ParsesTimePrefixedWeekdayOrdinalDate()
    {
        // Format: "12:00, Friday September 11th" — no year, weekday prefix, ordinal suffix.
        // The year is inferred as the nearest upcoming September 11.
        const string html = """
            <div>12:00, Friday September 11th</div>
            """;

        var date = RaceHtmlScraper.ExtractDate(html);

        Assert.NotNull(date);
        Assert.Matches(@"^\d{4}-09-11$", date);
    }

    [Fact]
    public void ExtractDate_ParsesWeekdayOrdinalDateWithoutTime()
    {
        // Format: "Saturday October 4th" — no time prefix, no year.
        const string html = """
            <div>Saturday October 4th</div>
            """;

        var date = RaceHtmlScraper.ExtractDate(html);

        Assert.NotNull(date);
        Assert.Matches(@"^\d{4}-10-04$", date);
    }

    [Fact]
    public void ExtractEventName_PicksJsonLdNameOverTitle()
    {
        const string html = """
            <html>
              <head>
                <script type="application/ld+json">
                  {"@context":"https://schema.org","@type":"Event","name":"Eco Trail Festival"}
                </script>
                <meta property="og:title" content="Website | Eco Trail Festival" />
              </head>
            </html>
            """;

        var name = RaceHtmlScraper.ExtractEventName(new Uri("https://ecotrail.example/"), html);

        Assert.Equal("Eco Trail Festival", name);
    }

    [Fact]
    public void ExtractStylesheetUrls_ResolvesRelativeAndAbsoluteUrls()
    {
        const string html = """
            <link rel="stylesheet" href="/css/site.css" />
            <link rel="stylesheet" href="https://cdn.example.com/styles.css" />
            <link rel="stylesheet" href="javascript:void(0);" />
            """;

        var urls = RaceHtmlScraper.ExtractStylesheetUrls(html, new Uri("https://example.com/race"));

        Assert.Contains(urls, u => u.AbsoluteUri == "https://example.com/css/site.css");
        Assert.Contains(urls, u => u.AbsoluteUri == "https://cdn.example.com/styles.css");
        Assert.DoesNotContain(urls, u => u.AbsoluteUri.StartsWith("javascript:"));
    }

    [Fact]
    public void ExtractProminentImage_PrefersOgImageOverLogo()
    {
        const string html = """
            <meta property="og:image" content="https://example.com/images/hero.jpg" />
            <img src="/images/logo.svg" alt="Event logo" />
            """;

        var image = RaceHtmlScraper.ExtractProminentImage(html, new Uri("https://example.com/race"));

        Assert.Equal("https://example.com/images/hero.jpg", image?.AbsoluteUri);
    }

    [Fact]
    public void ExtractLogo_FindsImageWithLogoAltText()
    {
        const string html = """
            <img src="/assets/logo.png" alt="Race logo" />
            <link rel="icon" href="/favicon.ico" />
            """;

        var logo = RaceHtmlScraper.ExtractLogo(html, new Uri("https://example.com/race"));

        Assert.Equal("https://example.com/assets/logo.png", logo?.AbsoluteUri);
    }

    [Fact]
    public void ExtractLogo_FallsBackToFaviconWhenNoLogoImagePresent()
    {
        const string html = """
            <img src="/assets/banner.png" alt="Banner image" />
            <link rel="icon" href="/favicon.ico" />
            """;

        var logo = RaceHtmlScraper.ExtractLogo(html, new Uri("https://example.com/race"));

        Assert.Equal("https://example.com/favicon.ico", logo?.AbsoluteUri);
    }

    [Fact]
    public void ExtractPrice_RecognisesEuroWordAsEur()
    {
        const string html = "<div>Startavgift: 35 Euro</div>";
        var price = RaceHtmlScraper.ExtractPrice(html, new Uri("https://example.com/"));

        Assert.NotNull(price);
        Assert.Equal("35", price!.Amount);
        Assert.Equal("EUR", price.Currency);
    }

    [Fact]
    public void ExtractPrice_ParsesEuroRangeIntoAmountRangeAndEurCurrency()
    {
        const string html = "<div>Startavgift: 60€ - 90€</div>";
        var price = RaceHtmlScraper.ExtractPrice(html, new Uri("https://example.com/"));

        Assert.NotNull(price);
        Assert.Equal("60-90", price!.Amount);
        Assert.Equal("EUR", price.Currency);
    }
}
