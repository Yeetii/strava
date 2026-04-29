using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.IO;
using Azure.Messaging.ServiceBus;
using Backend.Scrapers;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class ScrapeRaceMistralWorker
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly BlobOrganizerStore _organizerClient;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ILogger<ScrapeRaceMistralWorker> _logger;
    private readonly string _agentId;
    private readonly string _apiKey;
    private readonly string _apiBaseUrl;
    private readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

    public ScrapeRaceMistralWorker(
        IHttpClientFactory httpClientFactory,
        BlobOrganizerStore organizerClient,
        ServiceBusClient serviceBusClient,
        IConfiguration configuration,
        ILogger<ScrapeRaceMistralWorker> logger)
    {
        _httpClientFactory = httpClientFactory;
        _organizerClient = organizerClient;
        _serviceBusClient = serviceBusClient;
        _logger = logger;
        _agentId = configuration.GetValue<string>("MistralStudioAgentId") ?? throw new Exception("MistralStudioAgentId is not configured");
        _apiKey = configuration.GetValue<string>("MistralStudioApiKey") ?? throw new Exception("MistralStudioApiKey is not configured");
        _apiBaseUrl = configuration.GetValue<string>("MistralStudioApiUrl")?.TrimEnd('/') ?? "https://api.mistral.ai/v1/conversations";
    }

    [Function(nameof(ScrapeRaceMistralWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.MistralScrapeJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var rawBody = message.Body.ToString();
        ManualMistralScrapeJob? job;
        try
        {
            job = JsonSerializer.Deserialize<ManualMistralScrapeJob>(rawBody, _jsonOptions);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "ManualMistralScrapeWorker received invalid JSON message");
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "InvalidJson", deadLetterErrorDescription: ex.Message);
            return;
        }

        if (job is null || string.IsNullOrWhiteSpace(job.Url))
        {
            _logger.LogWarning("ManualMistralScrapeWorker received missing or empty Url");
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "MissingUrl");
            return;
        }

        if (!Uri.TryCreate(job.Url.Trim(), UriKind.Absolute, out var url) || (url.Scheme is not ("http" or "https")))
        {
            _logger.LogWarning("ManualMistralScrapeWorker received invalid Url: {Url}", job.Url);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "InvalidUrl", deadLetterErrorDescription: job.Url);
            return;
        }

        var organizerKey = BlobOrganizerStore.DeriveOrganizerKey(url);
        var discovery = new SourceDiscovery
        {
            DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
            Name = job.Name,
            SourceUrls = [url.AbsoluteUri],
        };

        try
        {
            await _organizerClient.WriteDiscoveryAsync(organizerKey, url.AbsoluteUri, "manual-mistral", [discovery], cancellationToken);

            var result = await ScrapeWithMistralAsync(url, job.Name, cancellationToken);
            if (result is null)
            {
                _logger.LogWarning("ManualMistralScrapeWorker did not receive a valid scrape result for {OrganizerKey}", organizerKey);
            }
            else
            {
                await _organizerClient.WriteScraperOutputAsync(organizerKey, "mistral", ToScraperOutput(result), cancellationToken);
                _logger.LogInformation("ManualMistralScrapeWorker wrote {RouteCount} routes for {OrganizerKey}", result.Routes.Count, organizerKey);
            }

            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, message, _serviceBusClient, ServiceBusConfig.MistralScrapeJobs, _logger, cancellationToken);
            return;
        }
    }

    private async Task<RaceScraperResult?> ScrapeWithMistralAsync(Uri url, string? fallbackName, CancellationToken cancellationToken)
    {
        var httpClient = _httpClientFactory.CreateClient();
        var html = await TryFetchStringAsync(httpClient, url, cancellationToken);
        if (html is null)
            return null;

        var assistantResponse = await QueryMistralAsync(html, url, fallbackName, cancellationToken);
        if (assistantResponse is null)
            return null;

        var routeCandidates = assistantResponse.GetRouteCandidates();
        var routes = new List<ScrapedRoute>();

        foreach (var candidate in routeCandidates)
        {
            routes.Add(await BuildRouteAsync(candidate, url, httpClient, cancellationToken));
        }

        // Mistral often omits GPX; HTML may link Dropbox as "Downloads" / "download area" (no "gpx" in anchor text).
        if (!routes.Any(r => r.Coordinates.Count >= 2))
        {
            routes.Clear();
            await AppendRoutesFromHtmlGpxDiscoveryAsync(routes, html, url, httpClient, fallbackName, cancellationToken);
        }

        var websiteUrl = TryParseUri(assistantResponse.WebsiteUrl, url) ?? url;
        return new RaceScraperResult(routes,
            WebsiteUrl: websiteUrl,
            ImageUrl: TryParseUri(assistantResponse.ImageUrl, url),
            LogoUrl: TryParseUri(assistantResponse.LogoUrl, url),
            ExtractedName: assistantResponse.ExtractedName ?? fallbackName,
            ExtractedDate: assistantResponse.ExtractedDate,
            StartFee: assistantResponse.StartFee,
            Currency: assistantResponse.Currency);
    }

    private async Task AppendRoutesFromHtmlGpxDiscoveryAsync(
        List<ScrapedRoute> routes,
        string html,
        Uri pageUrl,
        HttpClient httpClient,
        string? fallbackName,
        CancellationToken cancellationToken)
    {
        foreach (var gpxUrl in RaceHtmlScraper.ExtractGpxLinksFromHtml(html, pageUrl))
        {
            if (DropboxShareParser.IsDropboxSharedFolder(gpxUrl))
            {
                var zipBytes = await DropboxShareParser.TryDownloadSharedFolderZipAsync(httpClient, gpxUrl, cancellationToken);
                if (zipBytes is null) continue;
                foreach (var (entryPath, gpxXml) in DropboxShareParser.ExtractGpxFromZip(zipBytes))
                {
                    var fileLabel = Path.GetFileNameWithoutExtension(entryPath.Replace('\\', '/').Split('/').Last());
                    var parsed = GpxParser.TryParseRoute(gpxXml, fileLabel);
                    if (parsed is null) continue;
                    var routeName = string.IsNullOrWhiteSpace(fallbackName)
                        ? parsed.Name
                        : $"{fallbackName.TrimEnd()} — {fileLabel}";
                    var entryUri = DropboxShareParser.ToSharedFolderEntryUri(gpxUrl, entryPath);
                    routes.Add(new ScrapedRoute(
                        Coordinates: parsed.Coordinates,
                        SourceUrl: pageUrl,
                        Name: routeName.Trim(),
                        Distance: null,
                        ElevationGain: null,
                        GpxUrl: entryUri,
                        ImageUrl: null,
                        LogoUrl: null,
                        Date: null,
                        StartFee: null,
                        Currency: null,
                        GpxSource: GpxSourceResolver.Resolve(entryUri, pageUrl)));
                }
            }
            else
            {
                routes.Add(await BuildRouteAsync(new MistralAgentRouteCandidate
                {
                    GpxUrl = gpxUrl.AbsoluteUri,
                    SourceUrl = pageUrl.AbsoluteUri,
                    Name = fallbackName
                }, pageUrl, httpClient, cancellationToken));
            }
        }
    }

    private async Task<ScrapedRoute> BuildRouteAsync(
        MistralAgentRouteCandidate candidate,
        Uri pageUrl,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var sourceUrl = TryParseUri(candidate.SourceUrl, pageUrl) ?? pageUrl;
        var gpxUrl = TryParseUri(candidate.GpxUrl, pageUrl);
        IReadOnlyList<Coordinate> coordinates = [];

        if (gpxUrl is not null)
        {
            var gpxContent = await TryFetchStringAsync(httpClient, gpxUrl, cancellationToken);
            if (gpxContent is not null)
            {
                var parsed = GpxParser.TryParseRoute(gpxContent, candidate.Name ?? "Unnamed route");
                if (parsed is not null)
                {
                    coordinates = parsed.Coordinates;
                }
            }
        }

        return new ScrapedRoute(
            Coordinates: coordinates,
            SourceUrl: sourceUrl,
            Name: candidate.Name,
            Distance: candidate.Distance,
            ElevationGain: candidate.ElevationGain,
            GpxUrl: gpxUrl,
            ImageUrl: null,
            LogoUrl: null,
            Date: candidate.Date,
            StartFee: candidate.StartFee,
            Currency: candidate.Currency,
            GpxSource: gpxUrl is not null ? GpxSourceResolver.Resolve(gpxUrl, pageUrl) : null);
    }

    private async Task<string?> TryFetchStringAsync(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        try
        {
            var fetchUri = DropboxShareParser.IsDropboxSharedFile(url)
                ? DropboxShareParser.WithDl1(url)
                : url;
            using var request = new HttpRequestMessage(HttpMethod.Get, fetchUri);
            request.Headers.UserAgent.ParseAdd("(https://peakshunters.erikmagnusson.com)");
            using var response = await httpClient.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Failed to fetch {Url}: {StatusCode}", fetchUri, response.StatusCode);
                return null;
            }
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Failed to fetch {Url}", url);
            return null;
        }
    }

    private Uri BuildAgentResponseUri()
    {
        var baseUrl = _apiBaseUrl.TrimEnd('/');
        if (baseUrl.EndsWith("/v1/conversations", StringComparison.OrdinalIgnoreCase) || baseUrl.EndsWith("/conversations", StringComparison.OrdinalIgnoreCase))
            return new Uri(baseUrl);

        if (baseUrl.Contains("/v1/conversations/", StringComparison.OrdinalIgnoreCase) || baseUrl.Contains("/conversations/", StringComparison.OrdinalIgnoreCase))
            return new Uri(baseUrl);

        if (baseUrl.EndsWith("/v1/agents", StringComparison.OrdinalIgnoreCase) || baseUrl.EndsWith("/agents", StringComparison.OrdinalIgnoreCase))
            return new Uri($"{baseUrl}/{_agentId}/responses");

        return new Uri($"{baseUrl}/v1/conversations");
    }

    private async Task<MistralAgentScrapeResponse?> QueryMistralAsync(string html, Uri pageUrl, string? fallbackName, CancellationToken cancellationToken)
    {
        var httpClient = _httpClientFactory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, BuildAgentResponseUri());
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _logger.LogInformation("Sending Mistral request to {RequestUri}", request.RequestUri);

        var instructions = $"You are a structured web scraping assistant.\n" +
            "Extract metadata from the event page and return valid JSON only.\n" +
            "Do not include explanations or markdown.\n" +
            "Return keys: websiteUrl, imageUrl, logoUrl, extractedName, extractedDate, startFee, currency, routes.\n" +
            "Each route item must include sourceUrl, name, distance, gpxUrl, date, elevationGain, startFee, currency.\n" +
            "If a field cannot be extracted, return null.\n" +
            $"PageUrl: {pageUrl}\n" +
            $"FallbackEventName: {fallbackName ?? ""}\n" +
            "HTML:\n" + html;

        var payload = new
        {
            agent_id = _agentId,
            agent_version = 0,
            inputs = new[]
            {
                new
                {
                    role = "user",
                    content = pageUrl.AbsoluteUri
                }
            }
        };

        request.Content = new StringContent(JsonSerializer.Serialize(payload, _jsonOptions), Encoding.UTF8, "application/json");

        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning("Mistral agent returned {StatusCode} for {Url}", response.StatusCode, pageUrl);
            return null;
        }

        var raw = await response.Content.ReadAsStringAsync(cancellationToken);
        var mistResponse = JsonSerializer.Deserialize<MistralAgentResponse>(raw, _jsonOptions);
        if (mistResponse is null)
            return null;

        var textPayload = ExtractResponseText(mistResponse);
        if (string.IsNullOrWhiteSpace(textPayload))
            return null;

        var jsonPayload = ExtractJsonObject(textPayload);
        if (string.IsNullOrWhiteSpace(jsonPayload))
            return null;

        try
        {
            var result = JsonSerializer.Deserialize<MistralAgentScrapeResponse>(jsonPayload, _jsonOptions);
            if (result is not null)
                return result;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse Mistral output JSON for {Url}", pageUrl);
        }

        return null;
    }

    private string ExtractResponseText(MistralAgentResponse response)
    {
        if (response.Outputs is null)
            return string.Empty;

        foreach (var output in response.Outputs)
        {
            if (output.Type is not null && output.Type.Contains("output_text", StringComparison.OrdinalIgnoreCase))
            {
                return ExtractJsonContent(output.Content);
            }
        }

        return response.Outputs.Count > 0 ? ExtractJsonContent(response.Outputs[0].Content) : string.Empty;
    }

    private static string ExtractJsonContent(JsonElement content)
    {
        if (content.ValueKind == JsonValueKind.String)
            return content.GetString() ?? string.Empty;

        if (content.ValueKind == JsonValueKind.Array)
            return string.Concat(content.EnumerateArray().Select(element => element.ValueKind == JsonValueKind.String ? element.GetString() : element.GetRawText()));

        return content.GetRawText();
    }

    private static string ExtractJsonObject(string text)
    {
        var startIndex = text.IndexOf('{');
        if (startIndex < 0)
            return string.Empty;

        int depth = 0;
        for (int i = startIndex; i < text.Length; i++)
        {
            if (text[i] == '{') depth++;
            else if (text[i] == '}')
            {
                depth--;
                if (depth == 0)
                    return text[startIndex..(i + 1)];
            }
        }

        return string.Empty;
    }

    private static Uri? TryParseUri(string? candidate, Uri baseUrl)
    {
        if (string.IsNullOrWhiteSpace(candidate))
            return null;

        if (Uri.TryCreate(candidate.Trim(), UriKind.Absolute, out var absolute))
            return absolute;

        if (Uri.TryCreate(baseUrl, candidate.Trim(), out var relative))
            return relative;

        return null;
    }

    private ScraperOutput ToScraperOutput(RaceScraperResult result)
    {
        return new ScraperOutput
        {
            ScrapedAtUtc = DateTime.UtcNow.ToString("o"),
            WebsiteUrl = result.WebsiteUrl?.AbsoluteUri,
            ImageUrl = result.ImageUrl?.AbsoluteUri,
            LogoUrl = result.LogoUrl?.AbsoluteUri,
            ExtractedName = result.ExtractedName,
            ExtractedDate = result.ExtractedDate,
            StartFee = result.StartFee,
            Currency = result.Currency,
            Routes = result.Routes.Count > 0
                ? result.Routes.Select(r => new ScrapedRouteOutput
                {
                    Coordinates = r.Coordinates.Count >= 2
                        ? r.Coordinates.Select(c => new[] { c.Lng, c.Lat }).ToList()
                        : null,
                    SourceUrl = r.SourceUrl?.AbsoluteUri,
                    Name = r.Name,
                    Distance = r.Distance,
                    ElevationGain = r.ElevationGain,
                    GpxUrl = r.GpxUrl?.AbsoluteUri,
                    ImageUrl = r.ImageUrl?.AbsoluteUri,
                    LogoUrl = r.LogoUrl?.AbsoluteUri,
                    Date = r.Date,
                    StartFee = r.StartFee,
                    Currency = r.Currency,
                    GpxSource = r.GpxSource,
                }).ToList()
                : null,
        };
    }

    private sealed record ManualMistralScrapeJob(string Url, string? Name = null);

    private sealed class MistralAgentResponse
    {
        public List<MistralAgentResponseOutput>? Outputs { get; set; }
    }

    private sealed class MistralAgentResponseOutput
    {
        public string? Type { get; set; }
        public JsonElement Content { get; set; }
    }

    private sealed class MistralAgentScrapeResponse
    {
        public string? WebsiteUrl { get; set; }
        public string? ImageUrl { get; set; }
        public string? LogoUrl { get; set; }
        public string? ExtractedName { get; set; }
        public string? ExtractedDate { get; set; }
        public string? StartFee { get; set; }
        public string? Currency { get; set; }

        [JsonPropertyName("routes")]
        public List<MistralAgentRouteCandidate>? Routes { get; set; }

        [JsonPropertyName("routeCandidates")]
        public List<MistralAgentRouteCandidate>? RouteCandidates { get; set; }

        public IReadOnlyList<MistralAgentRouteCandidate> GetRouteCandidates()
        {
            if (Routes is { Count: > 0 })
                return Routes;
            if (RouteCandidates is { Count: > 0 })
                return RouteCandidates;
            return Array.Empty<MistralAgentRouteCandidate>();
        }
    }

    private sealed class MistralAgentRouteCandidate
    {
        public string? SourceUrl { get; set; }
        public string? Name { get; set; }
        public string? Distance { get; set; }
        public string? GpxUrl { get; set; }
        public string? Date { get; set; }
        public double? ElevationGain { get; set; }
        public string? StartFee { get; set; }
        public string? Currency { get; set; }
    }
}
