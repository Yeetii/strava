using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Backend;

public class DiscoverRunagainRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverRunagainRaces> logger)
{
    private static readonly Uri ApiUrl = new("https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs");
    private const string FirestoreBatchUrl = "https://firestore.googleapis.com/v1/projects/nestelop-production/databases/(default)/documents:batchGet";
    private const string FirestoreDocPrefix = "projects/nestelop-production/databases/(default)/documents/RunCollection/";
    private static readonly string[] RaceTypes = ["Stiløp", "Backyard ultra", "Ultra", "Motbakke"];
    private static readonly string[] TerrainTypes = ["Terreng"];

    [Function(nameof(DiscoverRunagainRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var jobs = await FetchJobsAsync(httpClient, cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("runagain", jobs, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var seenUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var jobs = new List<ScrapeJob>();
        var oneYearAgoTimestamp = DateTimeOffset.UtcNow.AddYears(-1).ToUnixTimeSeconds();

        var raceTypeFilter = string.Join(", ", RaceTypes.Select(t => $"'{t}'"));
        var terrainFilter = string.Join(", ", TerrainTypes.Select(t => $"'{t}'"));
        var filter = $"(race_type IN [{raceTypeFilter}] OR terrain_type IN [{terrainFilter}]) AND timestamp >= {oneYearAgoTimestamp}";
        const int pageSize = 500;

        for (int offset = 0; ; offset += pageSize)
        {
            string json;
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, ApiUrl)
                {
                    Content = new StringContent(
                        JsonSerializer.Serialize(new { search = "", filter, offset, sort = "date", limit = pageSize }),
                        System.Text.Encoding.UTF8,
                        "application/json")
                };
                var response = await httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                json = await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "RunAgain: failed to fetch search results (offset {Offset})", offset);
                break;
            }

            var page = RaceScrapeDiscovery.ParseRunagainSearchResults(json);
            if (page.Count == 0) break;

            foreach (var job in page)
            {
                if (job.RunagainUrl is not null && seenUrls.Add(job.RunagainUrl.AbsoluteUri))
                    jobs.Add(job);
            }

            if (page.Count < pageSize) break;
        }

        logger.LogInformation("RunAgain: discovered {Count} unique events", jobs.Count);
        await EnrichWebsiteUrlsAsync(httpClient, jobs, cancellationToken);
        return jobs;
    }

    private async Task EnrichWebsiteUrlsAsync(HttpClient httpClient, List<ScrapeJob> jobs, CancellationToken cancellationToken)
    {
        var guidToIndices = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        for (int i = 0; i < jobs.Count; i++)
        {
            if (jobs[i].ExternalIds?.TryGetValue("runagain", out var guid) == true && !string.IsNullOrWhiteSpace(guid))
            {
                if (!guidToIndices.TryGetValue(guid, out var list))
                    guidToIndices[guid] = list = [];
                list.Add(i);
            }
        }

        if (guidToIndices.Count == 0) return;

        var allGuids = guidToIndices.Keys.ToList();
        int enriched = 0;

        for (int batch = 0; batch < allGuids.Count; batch += 100)
        {
            var batchGuids = allGuids.Skip(batch).Take(100).ToList();
            var documentPaths = batchGuids.Select(g => $"{FirestoreDocPrefix}{g}").ToList();

            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, FirestoreBatchUrl)
                {
                    Content = new StringContent(
                        JsonSerializer.Serialize(new
                        {
                            documents = documentPaths,
                            mask = new { fieldPaths = new[] { "link", "registration", "organizer", "more_information", "start_fee", "currency", "county", "date" } }
                        }),
                        System.Text.Encoding.UTF8,
                        "application/json")
                };
                var response = await httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                var json = await response.Content.ReadAsStringAsync(cancellationToken);

                using var doc = JsonDocument.Parse(json);
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    if (!item.TryGetProperty("found", out var found)) continue;
                    if (!found.TryGetProperty("name", out var nameEl)) continue;

                    var docName = nameEl.GetString() ?? "";
                    var guid = docName.Replace(FirestoreDocPrefix, "");
                    if (!guidToIndices.TryGetValue(guid, out var indices)) continue;
                    if (!found.TryGetProperty("fields", out var fields)) continue;

                    string? websiteUrl = null;
                    if (fields.TryGetProperty("link", out var linkEl) && linkEl.TryGetProperty("stringValue", out var linkVal))
                        websiteUrl = linkVal.GetString();
                    if (string.IsNullOrWhiteSpace(websiteUrl) && fields.TryGetProperty("registration", out var regEl) && regEl.TryGetProperty("stringValue", out var regVal))
                        websiteUrl = regVal.GetString();

                    Uri? parsedUrl = null;
                    if (!string.IsNullOrWhiteSpace(websiteUrl)
                        && Uri.TryCreate(websiteUrl, UriKind.Absolute, out var url)
                        && url.Scheme is "http" or "https")
                        parsedUrl = url;

                    static string? GetStringField(JsonElement f, string name) =>
                        f.TryGetProperty(name, out var el) && el.TryGetProperty("stringValue", out var v) ? v.GetString() : null;

                    var organizer = GetStringField(fields, "organizer");
                    var description = GetStringField(fields, "more_information");
                    var startFee = GetStringField(fields, "start_fee");
                    var currency = GetStringField(fields, "currency");
                    var county = GetStringField(fields, "county");
                    var firestoreDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(GetStringField(fields, "date"));

                    foreach (var idx in indices)
                    {
                        var j = jobs[idx];
                        jobs[idx] = j with
                        {
                            WebsiteUrl = parsedUrl ?? j.WebsiteUrl,
                            Organizer = string.IsNullOrWhiteSpace(organizer) ? j.Organizer : organizer,
                            Description = string.IsNullOrWhiteSpace(description) ? j.Description : description,
                            StartFee = string.IsNullOrWhiteSpace(startFee) ? j.StartFee : startFee,
                            Currency = string.IsNullOrWhiteSpace(currency) ? j.Currency : currency,
                            County = string.IsNullOrWhiteSpace(county) ? j.County : county,
                            Date = string.IsNullOrWhiteSpace(j.Date) ? firestoreDate : j.Date,
                        };
                        if (parsedUrl is not null) enriched++;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "RunAgain: failed to enrich website URLs from Firestore (batch {Batch})", batch / 100);
            }
        }

        logger.LogInformation("RunAgain: enriched {Count}/{Total} events with organizer website URLs", enriched, jobs.Count);
    }
}
