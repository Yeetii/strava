using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class RemoveUser(
    CollectionClient<Shared.Models.User> usersCollection,
    CollectionClient<SummitedPeak> summitedPeaksCollection,
    CollectionClient<Activity> activitiesCollection,
    CollectionClient<Session> sessionsCollection)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
    [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "All data belonging to user has been removed")]
    [Function(nameof(RemoveUser))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "delete", Route = "manage/users/{userId}")] HttpRequestData req,
        string userId)
    {
        await summitedPeaksCollection.DeleteDocumentsByKey("userId", userId, userId);
        await activitiesCollection.DeleteDocumentsByKey("userId", userId, userId);
        await sessionsCollection.DeleteDocumentsByKey("userId", userId);
        await usersCollection.DeleteDocument(userId, new PartitionKey(userId));
        return req.CreateResponse(HttpStatusCode.NoContent);
    }
}
