using System.Net;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Spatial;

namespace API.Endpoints.User;

public class GetCurrentUser(
    UserAuthenticationService userAuthService,
    IActivitySummitRepository summitsRepository,
    IActivityVisitedPathRepository visitedPathsRepository,
    IActivityVisitedAreaRepository visitedAreasRepository)
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

        var foundTotals = new FoundTotalsResponse
        {
            SummitedPeaks = await summitsRepository.CountByUser(user.Id),
            VisitedPaths = await visitedPathsRepository.CountByUser(user.Id),
            VisitedAreas = await visitedAreasRepository.CountByUser(user.Id),
        };

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new CurrentUserResponse
        {
            UserId = user.Id,
            Username = user.UserName,
            FirstName = user.FirstName,
            LastName = user.LastName,
            SyncStatus = user.SyncStatus ?? UserSyncStatusService.CreateDefaultStatus(),
            FoundTotals = foundTotals,
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
