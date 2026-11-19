using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Hangfire;
using Shared.Jobs;
using System.Net;
using Shared.Models;
using System.Text.Json.Serialization;
using Shared.Services.StravaClient;
using Shared.Services;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Services.Postgres;

namespace API.Endpoints;

public class PostLogin(
    AuthenticationApi _authenticationApi,
    IUserRepository _usersRepository,
    ISessionRepository _sessionsRepository,
    IBackgroundJobClient jobs,
    ILogger<PostLogin> _logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/{authCode}/login",
            (HttpContext context, [FromServices] PostLogin endpoint, string authCode) => endpoint.Handle(context, authCode, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string authCode, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(authCode))
            return TypedResults.BadRequest("No tokens provided");

        var tokenResponse = await _authenticationApi.TokenExcange(authCode);
        var refreshToken = tokenResponse.RefreshToken;

        if (refreshToken == null || tokenResponse.Athlete == null)
            return TypedResults.BadRequest("Auth token exchange did not provide refresh token or athlete");

        var sessionId = Guid.NewGuid();
        context.Response.Cookies.Append("session", sessionId.ToString(), new CookieOptions
        {
            MaxAge = TimeSpan.FromDays(30),
            SameSite = SameSiteMode.None,
            Secure = true,
            Path = "/"
        });

        var userId = tokenResponse.Athlete.Id.ToString();
        _logger.LogInformation("Logging in user {userId} ", userId);
        var userExist = await _usersRepository.GetById(userId);

        var syncStatus = userExist?.SyncStatus ?? UserSyncStatusService.CreateDefaultStatus();

        var user = new Shared.Models.User
        {
            Id = userId,
            UserName = tokenResponse.Athlete.Username,
            FirstName = tokenResponse.Athlete.Firstname,
            LastName = tokenResponse.Athlete.Lastname,
            RefreshToken = refreshToken,
            AccessToken = tokenResponse.AccessToken,
            TokenExpiresAt = tokenResponse.ExpiresAt,
            StravaScope = tokenResponse.Scope,
            SyncStatus = syncStatus,
        };
        var shouldQueueInitialActivitiesFetch = ShouldQueueInitialActivitiesFetch(userExist);

        await _usersRepository.Upsert(user);

        if (shouldQueueInitialActivitiesFetch)
        {
            _logger.LogInformation("Queueing initial activities fetch for user {userId}", userId);
            jobs.EnqueueActivitiesFetch(new ActivitiesFetchJob { UserId = userId });
        }

        await _sessionsRepository.Upsert(new Session
        {
            Id = sessionId.ToString(),
            UserId = userId,
            CreatedAtUtc = DateTime.UtcNow
        });

        return TypedResults.Ok(new PostLoginResponse
        {
            UserId = tokenResponse.Athlete.Id.ToString(),
            Username = tokenResponse.Athlete.Username,
            FirstName = tokenResponse.Athlete.Firstname,
            LastName = tokenResponse.Athlete.Lastname,
            SyncStatus = syncStatus,
        });
    }

    public static bool ShouldQueueInitialActivitiesFetch(Shared.Models.User? existingUser)
    {
        if (existingUser == null)
            return true;

        var syncStatus = existingUser.SyncStatus;
        return syncStatus == null
            || (!syncStatus.TotalActivitiesOnStrava.HasValue && syncStatus.SyncedActivities == 0);
    }

    public class PostLoginResponse
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
    }
}
