using System.Net;
using System.Web;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetOrganizerRedirectCandidates(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration,
    IHttpClientFactory httpClientFactory,
    ILogger<GetOrganizerRedirectCandidates> logger)
{
    private static readonly TimeSpan RedirectProbeTimeout = TimeSpan.FromSeconds(10);

    [OpenApiOperation(tags: ["Admin"], Summary = "Get organizer redirect candidates based on similar ids and source URL mismatches.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "testRedirects", In = ParameterLocation.Query, Type = typeof(bool), Required = false, Description = "Visit similar organizer URLs and report confirmed redirects.")]
    [OpenApiParameter(name: "autoCreateRedirects", In = ParameterLocation.Query, Type = typeof(bool), Required = false, Description = "Automatically create redirects for confirmed cases whose target organizer already exists.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(OrganizerRedirectCandidateReport))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetOrganizerRedirectCandidates))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/organizers/redirect-candidates")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var query = HttpUtility.ParseQueryString(req.Url.Query);
        var testRedirects = ParseBool(query["testRedirects"]);
        var autoCreateRedirects = ParseBool(query["autoCreateRedirects"]);
        if (autoCreateRedirects)
            testRedirects = true;

        var inputs = new List<OrganizerRedirectCandidateInput>();
        await foreach (var metadata in organizerStore.StreamMetadataWithoutGeometriesAsync(maxConcurrency: 64, cancellationToken))
            inputs.Add(OrganizerRedirectCandidateInput.FromMetadata(metadata));

        var baseReport = OrganizerRedirectCandidateFinder.FindCandidates(inputs);
        var report = baseReport;

        if (testRedirects)
        {
            var knownOrganizerIds = inputs
                .Select(input => input.Id)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var probeTargets = OrganizerRedirectProbeHelper.BuildProbeTargets(baseReport, inputs);
            var observations = await ProbeRedirectsAsync(probeTargets, cancellationToken);

            var autoCreatedRedirectSources = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (autoCreateRedirects)
            {
                foreach (var observation in OrganizerRedirectProbeHelper.GetAutoCreatableRedirects(observations, knownOrganizerIds))
                {
                    try
                    {
                        await organizerStore.SetRedirectAsync(observation.SourceOrganizerKey, observation.TargetOrganizerKey, cancellationToken);
                        autoCreatedRedirectSources.Add(observation.SourceOrganizerKey);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        logger.LogWarning(ex, "Failed to auto-create redirect {SourceOrganizerKey} -> {TargetOrganizerKey}", observation.SourceOrganizerKey, observation.TargetOrganizerKey);
                    }
                }
            }

            report = new OrganizerRedirectCandidateReport(
                OrganizerRedirectProbeHelper.BuildConfirmedRedirects(observations, knownOrganizerIds, autoCreatedRedirectSources),
                baseReport.TotalOrganizers,
                baseReport.SlugToBareHostCandidates,
                baseReport.VerySimilarIdCandidates);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(report, cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }

    private async Task<List<OrganizerRedirectProbeObservation>> ProbeRedirectsAsync(
        IReadOnlyList<OrganizerRedirectProbeTarget> probeTargets,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var observations = new System.Collections.Concurrent.ConcurrentBag<OrganizerRedirectProbeObservation>();

        await Parallel.ForEachAsync(
            probeTargets,
            new ParallelOptions { MaxDegreeOfParallelism = 30, CancellationToken = cancellationToken },
            async (target, ct) =>
            {
                try
                {
                    using var probeCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    probeCancellation.CancelAfter(RedirectProbeTimeout);

                    using var request = new HttpRequestMessage(HttpMethod.Get, target.Url);
                    using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, probeCancellation.Token);
                    var finalUri = response.RequestMessage?.RequestUri;
                    if (finalUri is null)
                        return;

                    var finalOrganizerKey = BlobOrganizerStore.DeriveOrganizerKey(finalUri);
                    if (string.Equals(finalOrganizerKey, target.OrganizerKey, StringComparison.OrdinalIgnoreCase))
                        return;

                    observations.Add(new OrganizerRedirectProbeObservation(
                        target.OrganizerKey,
                        finalOrganizerKey,
                        target.Url,
                        finalUri.AbsoluteUri));
                }
                catch (OperationCanceledException ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogDebug(ex, "Redirect probe timed out for {OrganizerKey} ({Url})", target.OrganizerKey, target.Url);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogDebug(ex, "Redirect probe failed for {OrganizerKey} ({Url})", target.OrganizerKey, target.Url);
                }
            });

        return observations.ToList();
    }

    private static bool ParseBool(string? raw)
        => bool.TryParse(raw, out var parsed) && parsed;
}