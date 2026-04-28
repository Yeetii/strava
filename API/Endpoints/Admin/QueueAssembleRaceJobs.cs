using System.Linq;
using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class QueueAssembleRaceJobs(
    RaceOrganizerClient raceOrganizerClient,
    ServiceBusClient serviceBusClient,
    IConfiguration configuration,
    ILogger<QueueAssembleRaceJobs> logger)
{
    private readonly RaceOrganizerClient _raceOrganizerClient = raceOrganizerClient;
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.AssembleRace);
    private readonly IConfiguration _configuration = configuration;
    private readonly ILogger _logger = logger;

    [OpenApiOperation(tags: ["Admin"], Summary = "Queue assemble jobs for all race organizers.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of organizer assemble jobs queued.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(QueueAssembleRaceJobs))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/assemble")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var ids = await _raceOrganizerClient.GetAllIds();
        _logger.LogInformation("Queuing {Count} assemble jobs for race organizers", ids.Count);

        const int batchSize = 100;
        for (int i = 0; i < ids.Count; i += batchSize)
        {
            var messages = ids.Skip(i).Take(batchSize).Select(id => new ServiceBusMessage(id)).ToList();
            await _sender.SendMessagesAsync(messages);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(ids.Count);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = _configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}
