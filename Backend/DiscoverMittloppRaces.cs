using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net;

namespace Backend;

public class DiscoverMittloppRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverMittloppRaces> logger)
{
    [Function(nameof(DiscoverMittloppRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        await discoveryService.EnqueueDiscoveryMessageAsync(new RaceDiscoveryMessage("mittlopp"), delay: null, cancellationToken);
    }

    public async Task<bool> ProcessPageAsync(int page, CancellationToken cancellationToken)
    {
        var result = await FetchPageJobsAsync(page, cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("mittlopp", result.Jobs, cancellationToken);
        return result.HasNextPage;
    }

    private async Task<(IReadOnlyCollection<ScrapeJob> Jobs, bool HasNextPage)> FetchPageJobsAsync(int page, CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var pageUrl = BuildCalendarPageUrl(page);

        using var response = await httpClient.GetAsync(pageUrl, cancellationToken);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            throw new HttpRequestException($"Mittlopp calendar returned 429 for page {page}", null, response.StatusCode);

        response.EnsureSuccessStatusCode();

        var html = await response.Content.ReadAsStringAsync(cancellationToken);
        var jobs = MittloppDiscoveryAgent.ParseCalendarPage(html, pageUrl).ToArray();
        var nextPageUrl = MittloppDiscoveryAgent.ExtractNextPageUrl(html, pageUrl);
        var enriched = await EnrichJobsAsync(httpClient, jobs, cancellationToken);

        logger.LogInformation("Mittlopp: discovered {Count} retained events on page {Page}", enriched.Count, page);
        return (enriched, nextPageUrl is not null && page < 100);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> EnrichJobsAsync(HttpClient httpClient, IReadOnlyCollection<ScrapeJob> jobs, CancellationToken cancellationToken)
    {
        if (jobs.Count == 0)
            return jobs;

        using var semaphore = new SemaphoreSlim(4);
        var tasks = jobs.Select(async job =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return await EnrichJobsForSeedAsync(httpClient, job, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        });

        var enriched = (await Task.WhenAll(tasks)).SelectMany(x => x);
        return enriched.Where(MittloppDiscoveryAgent.ShouldKeepJob).ToArray();
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> EnrichJobsForSeedAsync(HttpClient httpClient, ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.MittloppEventUrl is null)
            return [job];

        try
        {
            var hubHtml = await httpClient.GetStringAsync(job.MittloppEventUrl, cancellationToken);
            var enriched = MittloppDiscoveryAgent.EnrichFromEventHubPage(job, hubHtml, job.MittloppEventUrl);
            var variants = MittloppDiscoveryAgent.ExtractRegistrationVariants(hubHtml, job.MittloppEventUrl);
            if (variants.Count == 0)
                return [enriched];

            var expanded = new List<ScrapeJob>(variants.Count);
            foreach (var variant in variants)
            {
                try
                {
                    var variantHtml = await httpClient.GetStringAsync(variant.Url, cancellationToken);
                    expanded.Add(MittloppDiscoveryAgent.EnrichFromRegistrationVariantPage(enriched, variantHtml, variant.Url));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogDebug(ex, "Mittlopp: failed to enrich variant {VariantUrl}", variant.Url);
                }
            }

            return expanded.Count > 0 ? expanded : [enriched];
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Mittlopp: failed to enrich {EventUrl}", job.MittloppEventUrl);
            return [job];
        }
    }

    private static Uri BuildCalendarPageUrl(int page)
        => page <= 1
            ? MittloppDiscoveryAgent.CalendarUrl
            : new UriBuilder(MittloppDiscoveryAgent.CalendarUrl) { Query = $"page={page}" }.Uri;
}
