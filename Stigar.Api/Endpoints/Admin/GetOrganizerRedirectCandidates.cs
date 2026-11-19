using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Core.Services;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class GetOrganizerRedirectCandidates(
    IOrganizerStore organizerStore,
    IConfiguration configuration,
    IHttpClientFactory httpClientFactory,
    ILogger<GetOrganizerRedirectCandidates> logger)
{
    private static readonly TimeSpan RedirectProbeTimeout = TimeSpan.FromSeconds(10);

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/organizers/redirect-candidates",
            (HttpContext context, [FromServices] GetOrganizerRedirectCandidates endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var testRedirects = ParseBool(context.Request.Query["testRedirects"].ToString());
        var autoCreateRedirects = ParseBool(context.Request.Query["autoCreateRedirects"].ToString());
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

        return TypedResults.Ok(report);
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

                    var finalOrganizerKey = OrganizerUrlRules.DeriveOrganizerKey(finalUri);
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
