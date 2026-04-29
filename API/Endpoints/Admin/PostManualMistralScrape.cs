using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
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

public class PostManualMistralScrape(
    ServiceBusClient serviceBusClient,
    IConfiguration configuration,
    ILogger<PostManualMistralScrape> logger)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Queue a manual Mistral scrape job for a race event URL.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(PostManualMistralScrapeRequest))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostManualMistralScrapeResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(PostManualMistralScrape))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/scrape/mistral")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        PostManualMistralScrapeRequest? body;
        try
        {
            body = await req.ReadFromJsonAsync<PostManualMistralScrapeRequest>(cancellationToken);
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

        var requestMessage = new ServiceBusMessage(JsonSerializer.Serialize(new ManualMistralScrapeQueueMessage(parsedUrl.AbsoluteUri, body.Name), new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }))
        {
            ContentType = "application/json"
        };

        var sender = serviceBusClient.CreateSender(ServiceBusConfig.MistralScrapeJobs);
        await sender.SendMessageAsync(requestMessage, cancellationToken);
        logger.LogInformation("Enqueued manual mistral scrape job for {OrganizerKey}", organizerKey);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new PostManualMistralScrapeResponse(organizerKey, parsedUrl.AbsoluteUri), cancellationToken);
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

public record PostManualMistralScrapeRequest(string Url, string? Name = null);
public record PostManualMistralScrapeResponse(string EventKey, string CanonicalUrl);
public record ManualMistralScrapeQueueMessage(string Url, string? Name = null);
