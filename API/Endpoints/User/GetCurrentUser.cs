using System.Net;
using System.Text.Json.Serialization;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.User;

public class GetCurrentUser(
    UserAuthenticationService userAuthService,
    CollectionClient<SummitedPeak> summitedPeaksCollection,
    CollectionClient<VisitedPath> visitedPathsCollection,
    CollectionClient<VisitedArea> visitedAreasCollection)
{
    [OpenApiOperation(tags: ["User management"], Summary = "Get the authenticated user's profile and Strava sync status.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(CurrentUserResponse))]
    [Function(nameof(GetCurrentUser))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "user")] HttpRequestData req)
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var summitedPeaksCountTask = summitedPeaksCollection.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", user.Id));
        var visitedPathsCountTask = visitedPathsCollection.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", user.Id));
        var visitedAreasCountTask = visitedAreasCollection.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", user.Id));

        await Task.WhenAll(summitedPeaksCountTask, visitedPathsCountTask, visitedAreasCountTask);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new CurrentUserResponse
        {
            UserId = user.Id,
            Username = user.UserName,
            FirstName = user.FirstName,
            LastName = user.LastName,
            SyncStatus = user.SyncStatus ?? UserSyncStatusService.CreateDefaultStatus(),
            FoundTotals = new FoundTotalsResponse
            {
                SummitedPeaks = summitedPeaksCountTask.Result.FirstOrDefault(),
                VisitedPaths = visitedPathsCountTask.Result.FirstOrDefault(),
                VisitedAreas = visitedAreasCountTask.Result.FirstOrDefault(),
            },
        });
        return response;
    }

    public class CurrentUserResponse
    {
        [JsonPropertyName("userId")]
        public required string UserId { get; set; }

        [JsonPropertyName("username")]
        public string? Username { get; set; }

        [JsonPropertyName("firstName")]
        public string? FirstName { get; set; }

        [JsonPropertyName("lastName")]
        public string? LastName { get; set; }

        [JsonPropertyName("syncStatus")]
        public required StravaSyncStatus SyncStatus { get; set; }

        [JsonPropertyName("foundTotals")]
        public required FoundTotalsResponse FoundTotals { get; set; }
    }

    public class FoundTotalsResponse
    {
        [JsonPropertyName("summitedPeaks")]
        public int SummitedPeaks { get; set; }

        [JsonPropertyName("visitedPaths")]
        public int VisitedPaths { get; set; }

        [JsonPropertyName("visitedAreas")]
        public int VisitedAreas { get; set; }
    }
}