using System.Net;
using Shared.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Microsoft.Azure.Cosmos;

namespace API.Endpoints
{
    public class RemoveUser(CollectionClient<Shared.Models.User> _usersCollection,
        CollectionClient<SummitedPeak> _summitedPeaksCollection,
        CollectionClient<Activity> _activitiesCollection,
        CollectionClient<Session> _sessionsCollection)
    {
        [OpenApiOperation(tags: ["User management"])]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.NoContent, contentType: "text/plain", bodyType: typeof(string), Description = "All data belonging to user has been removed")]
        [Function(nameof(RemoveUser))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "delete", Route = "{userId}")] HttpRequestData req, string userId)
        {
            await _summitedPeaksCollection.DeleteDocumentsByKey("userId", userId, userId);
            await _activitiesCollection.DeleteDocumentsByKey("userId", userId, userId);
            await _sessionsCollection.DeleteDocumentsByKey("userId", userId);
            await _usersCollection.DeleteDocument(userId, new PartitionKey(userId));
            return req.CreateResponse(HttpStatusCode.NoContent);
        }
    }
}
