using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class PostRaceEvent(
    BlobOrganizerStore organizerClient,
    ServiceBusClient serviceBusClient,
    IConfiguration configuration,
    ILogger<PostRaceEvent> logger)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(PostRaceEventRequest))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostRaceEventResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(PostRaceEvent))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/events")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        PostRaceEventRequest? body;
        try
        {
            body = await req.ReadFromJsonAsync<PostRaceEventRequest>(cancellationToken);
        }
        catch
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Invalid JSON body", cancellationToken);
            return badReq;
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Url))
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Missing required field: url", cancellationToken);
            return badReq;
        }

        if (!Uri.TryCreate(body.Url.Trim(), UriKind.Absolute, out var parsedUrl) || parsedUrl.Scheme is not ("http" or "https"))
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Invalid URL — must be an absolute http(s) URL", cancellationToken);
            return badReq;
        }

        var organizerKey = BlobOrganizerStore.DeriveOrganizerKey(parsedUrl);
        var discovery = new SourceDiscovery
        {
            DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
            Name = body.Name,
            SourceUrls = [parsedUrl.AbsoluteUri],
        };

        await organizerClient.WriteDiscoveryAsync(organizerKey, parsedUrl.AbsoluteUri, "manual", [discovery], cancellationToken);
        logger.LogInformation("Manual discovery: wrote organizer {OrganizerKey} from {Url}", organizerKey, parsedUrl);

        // Enqueue a scrape message so the scraper picks it up.
        if (body.Scrape != false)
        {
            var sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);
            await sender.SendMessageAsync(new ServiceBusMessage(organizerKey), cancellationToken);
            logger.LogInformation("Manual discovery: enqueued scrape for {OrganizerKey}", organizerKey);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new PostRaceEventResponse(organizerKey, parsedUrl.AbsoluteUri), cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}

public record PostRaceEventRequest(string Url, string? Name = null, bool? Scrape = true);
public record PostRaceEventResponse(string EventKey, string CanonicalUrl);
