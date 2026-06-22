using System.Net;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using UserDocument = Shared.Models.User;

namespace API.Endpoints.Admin;

public record RefetchUserActivitiesResponse(string UserId, bool Queued);

public class RefetchUserActivities(
    CollectionClient<UserDocument> usersCollection,
    ServiceBusClient serviceBusClient,
    IConfiguration configuration)
{
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ActivitiesFetchJobs);

    [OpenApiOperation(tags: ["Admin"], Summary = "Queue a full Strava activities refetch for a user.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(RefetchUserActivitiesResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound)]
    [Function(nameof(RefetchUserActivities))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/users/{userId}/activities/refetch")] HttpRequestData req,
        string userId,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var user = await usersCollection.GetByIdMaybe(userId, new PartitionKey(userId), cancellationToken);
        if (user == null)
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        var fetchJob = new ActivitiesFetchJob { UserId = userId, Page = 1 };
        await _sender.SendMessageAsync(new ServiceBusMessage(JsonSerializer.Serialize(fetchJob)), cancellationToken);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new RefetchUserActivitiesResponse(userId, Queued: true), cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
        {
            return false;
        }

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}
