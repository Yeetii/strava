using System.Net;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;

namespace API.Endpoints.Admin;

public record PostRaceDiscoveryRequest(string Agent, int? PageStart = null, int? PageEnd = null);
public record PostRaceDiscoveryResponse(string Agent, int PageStart, int PageEnd, int MessagesEnqueued);

public class PostRaceDiscovery(
    ServiceBusClient serviceBusClient,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Queue one or more race discovery messages.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(PostRaceDiscoveryRequest))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostRaceDiscoveryResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(PostRaceDiscovery))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/discovery")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        PostRaceDiscoveryRequest? body;
        try
        {
            body = await req.ReadFromJsonAsync<PostRaceDiscoveryRequest>(cancellationToken);
        }
        catch
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Invalid JSON body", cancellationToken);
            return badReq;
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Agent))
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Missing required field: agent", cancellationToken);
            return badReq;
        }

        var agent = body.Agent.Trim().ToLowerInvariant();
        var pageStart = Math.Max(1, body.PageStart ?? 1);
        var pageEnd = Math.Max(pageStart, body.PageEnd ?? pageStart);
        if (pageEnd - pageStart > 49)
        {
            var badReq = req.CreateResponse(HttpStatusCode.BadRequest);
            await badReq.WriteStringAsync("Refusing to enqueue more than 50 discovery pages in one request", cancellationToken);
            return badReq;
        }

        var sender = serviceBusClient.CreateSender(ServiceBusConfig.RaceDiscoveryJobs);
        for (var page = pageStart; page <= pageEnd; page++)
        {
            var payload = new { agent, page };
            var message = new ServiceBusMessage(BinaryData.FromString(JsonSerializer.Serialize(payload)))
            {
                ContentType = "application/json",
                MessageId = $"admin-discovery:{agent}:{page}:{Guid.NewGuid():N}",
                Subject = $"discovery:{agent}:{page}"
            };
            await sender.SendMessageAsync(message, cancellationToken);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new PostRaceDiscoveryResponse(agent, pageStart, pageEnd, pageEnd - pageStart + 1), cancellationToken);
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
