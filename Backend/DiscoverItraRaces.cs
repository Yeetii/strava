using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net;

namespace Backend;

public class DiscoverItraRaces(
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverItraRaces> logger)
{
    // Country is the only filter that scopes results server-side.
    // Iterating over every country code keeps each request under the global ~300-race cap.
    private static readonly string[] CountryCodes =
    [
        "AF","AL","DZ","AD","AO","AG","AR","AM","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BT",
        "BO","BA","BW","BR","BN","BG","BF","BI","CV","KH","CM","CA","CF","TD","CL","CN","CO","KM","CG","CD",
        "CR","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","SZ","ET","FJ","FI","FR",
        "GA","GM","GE","DE","GH","GR","GD","GT","GN","GW","GY","HT","HN","HU","IS","IN","ID","IR","IQ","IE",
        "IL","IT","JM","JP","JO","KZ","KE","KI","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MG",
        "MW","MY","MV","ML","MT","MH","MR","MU","MX","FM","MD","MC","MN","ME","MA","MZ","MM","NA","NR","NP",
        "NL","NZ","NI","NE","NG","MK","NO","OM","PK","PW","PA","PG","PY","PE","PH","PL","PT","QA","RO","RU",
        "RW","KN","LC","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SK","SI","SB","SO","ZA","SS","ES",
        "LK","SD","SR","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TO","TT","TN","TR","TM","TV","UG","UA",
        "AE","GB","US","UY","UZ","VU","VE","VN","YE","ZM","ZW","RE","GP","MQ","GF","NC","PF","YT","PM","WF",
        "TF","CK","NU","TK","HK","MO","PS","XK","GI","IM","JE","GG","AX","FO","GL","SJ","BM","KY","VG","VI",
        "PR","GU","MP","AS","UM","MF","BL","CW","SX","BQ","AW","AC","TA","IO","SH","FK","GS"
    ];

    [Function(nameof(DiscoverItraRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        await discoveryService.EnqueueDiscoveryMessageAsync(new RaceDiscoveryMessage("itra"), delay: null, cancellationToken);
    }

    public async Task<bool> ProcessPageAsync(int page, CancellationToken cancellationToken)
    {
        if (page < 1 || page > TotalPages)
        {
            logger.LogWarning("ITRA discovery page {Page} is outside valid range 1..{TotalPages}", page, TotalPages);
            return false;
        }

        var country = CountryCodes[page - 1];
        logger.LogInformation("ITRA: discovering country {Country} on page {Page}/{TotalPages}", country, page, TotalPages);

        var jobs = await FetchJobsAsync(country, cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("itra", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);

        return page < TotalPages;
    }

    private static int TotalPages => CountryCodes.Length;

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(string country, CancellationToken cancellationToken)
    {
        try
        {
            var cookieContainer = new System.Net.CookieContainer();
            using var handler = new HttpClientHandler
            {
                CookieContainer = cookieContainer,
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };

            using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(90) };
            client.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36");
            client.DefaultRequestHeaders.Accept.ParseAdd("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8");
            client.DefaultRequestHeaders.AcceptLanguage.ParseAdd("en-US,en;q=0.9");
            client.DefaultRequestHeaders.Add("sec-fetch-site", "none");
            client.DefaultRequestHeaders.Add("sec-fetch-mode", "navigate");
            client.DefaultRequestHeaders.Add("sec-fetch-dest", "document");
            client.DefaultRequestHeaders.Add("upgrade-insecure-requests", "1");

            logger.LogInformation("ITRA: fetching calendar page for antiforgery token");
            using var initRequest = new HttpRequestMessage(HttpMethod.Get, ItraDiscoveryAgent.CalendarUrl);
            using var initResponse = await client.SendAsync(initRequest, cancellationToken);
            if (!initResponse.IsSuccessStatusCode)
            {
                logger.LogWarning("ITRA: calendar page returned {Status}", initResponse.StatusCode);
                return [];
            }

            var initialHtml = await initResponse.Content.ReadAsStringAsync(cancellationToken);
            var token = ItraDiscoveryAgent.ExtractRequestVerificationToken(initialHtml);
            if (string.IsNullOrWhiteSpace(token))
            {
                logger.LogWarning("ITRA: failed to extract antiforgery token from calendar page (html length={Length})", initialHtml.Length);
                return [];
            }
            logger.LogInformation("ITRA: got antiforgery token, starting discovery for country {Country}", country);

            List<KeyValuePair<string, string>> fields =
            [
                new("__RequestVerificationToken", token),
                new("Input.Longitude", "0"),
                new("Input.Latitude", "0"),
                new("Input.NorthEastLat", "90"),
                new("Input.NorthEastLng", "180"),
                new("Input.SouthWestLat", "-90"),
                new("Input.SouthWestLng", "-180"),
                new("Input.MinDistance", ""),
                new("Input.MaxDistance", ""),
                new("Input.MinElevationGain", ""),
                new("Input.MaxElevationGain", ""),
                new("Input.MinElevationLoss", ""),
                new("Input.MaxElevationLoss", ""),
                new("Input.MinItraPts", "0"),
                new("Input.MaxItraPts", "7"),
                new("Input.ItraPtsValue", "0"),
                new("Input.NationalLeagues", "false"),
                new("Input.NationalLeague", "false"),
                new("Input.DateValue", ""),
                new("Input.DateStart", ""),
                new("Input.DateEnd", ""),
                new("Input.resultcount", "0"),
                new("Input.RaceValue", ""),
                new("Input.Country", country),
                new("ZoomLevel", "2"),
                new("type", ""),
                new("countMap", "1"),
            ];

            using var request = new HttpRequestMessage(HttpMethod.Post, ItraDiscoveryAgent.CalendarUrl)
            {
                Content = new FormUrlEncodedContent(fields)
            };
            request.Headers.Referrer = ItraDiscoveryAgent.CalendarUrl;

            using var response = await client.SendAsync(request, cancellationToken);
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                throw new HttpRequestException($"ITRA calendar returned 429 for country {country}", null, response.StatusCode);

            response.EnsureSuccessStatusCode();

            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            var page = ItraDiscoveryAgent.ParseCalendarPage(html, ItraDiscoveryAgent.CalendarUrl);

            var jobs = page.ToArray();
            logger.LogInformation("ITRA: discovered {Count} races for country {Country}", jobs.Length, country);

            var enrichedJobs = await ItraDiscoveryAgent.EnrichEventPageDetailsAsync(jobs, client, cancellationToken,
                onProgress: (done, total) =>
                {
                    if (done == -1)
                        logger.LogWarning("ITRA enrichment: bot-blocked (captcha), stopping enrichment");
                    else if (done % 100 == 0)
                        logger.LogInformation("ITRA enrichment: {Done}/{Total}", done, total);
                });
            return enrichedJobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "ITRA: failed to initialise calendar session");
            return [];
        }
    }
}
