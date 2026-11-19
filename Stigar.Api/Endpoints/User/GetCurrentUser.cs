using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using System.Text.Json.Serialization;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Api.Endpoints.User;

public class GetCurrentUser(
    UserAuthenticationService userAuthService,
    IActivitySummitRepository summitsRepository,
    IActivityVisitedPathRepository visitedPathsRepository,
    IActivityVisitedAreaRepository visitedAreasRepository)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/user", (HttpContext context, [FromServices] GetCurrentUser endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        string? sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return TypedResults.Unauthorized();

        var foundTotals = new FoundTotalsResponse
        {
            SummitedPeaks = await summitsRepository.CountByUser(user.Id),
            VisitedPaths = await visitedPathsRepository.CountByUser(user.Id),
            VisitedAreas = await visitedAreasRepository.CountByUser(user.Id),
        };

        return TypedResults.Ok(new CurrentUserResponse
        {
            UserId = user.Id,
            Username = user.UserName,
            FirstName = user.FirstName,
            LastName = user.LastName,
            SyncStatus = user.SyncStatus ?? UserSyncStatusService.CreateDefaultStatus(),
            FoundTotals = foundTotals,
        });
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
