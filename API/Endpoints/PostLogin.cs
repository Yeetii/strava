using System.Net;
using Shared.Models;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
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
    QueueClient queueClient,
    ILogger<PostLogin> _logger)
{
    private readonly QueueSender _activitiesFetchSender = queueClient.CreateSender(QueueConfig.ActivitiesFetchJobs);

    [OpenApiOperation(tags: ["User management"])]
    [OpenApiParameter(name: "authCode", In = ParameterLocation.Path)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostLoginResponse), Description = "The login response containing the username")]
    [Function(nameof(PostLogin))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "{authCode}/login")] HttpRequestData req, string authCode)
    {
        var response = req.CreateResponse();

        if (string.IsNullOrEmpty(authCode))
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync("No tokens provided");
            return response;
        }

        var tokenResponse = await _authenticationApi.TokenExcange(authCode);
        var refreshToken = tokenResponse.RefreshToken;

        if (refreshToken == null || tokenResponse.Athlete == null)
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync("Auth token exchange did not provide refresh token or athlete");
            return response;
        }

        response.StatusCode = HttpStatusCode.OK;
        var sessionId = Guid.NewGuid();
        var cookie = new HttpCookie("session", sessionId.ToString())
        {
            MaxAge = TimeSpan.FromDays(30).TotalSeconds,
            SameSite = SameSite.ExplicitNone,
            Secure = true,
            Path = "/"
        };
        response.Cookies.Append(cookie);

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
            await _activitiesFetchSender.SendMessageAsync(new QueueMessage(System.Text.Json.JsonSerializer.Serialize(new ActivitiesFetchJob { UserId = userId })));
        }

        await _sessionsRepository.Upsert(new Session
        {
            Id = sessionId.ToString(),
            UserId = userId,
            CreatedAtUtc = DateTime.UtcNow
        });

        await response.WriteAsJsonAsync(new PostLoginResponse
        {
            UserId = tokenResponse.Athlete.Id.ToString(),
            Username = tokenResponse.Athlete.Username,
            FirstName = tokenResponse.Athlete.Firstname,
            LastName = tokenResponse.Athlete.Lastname,
            SyncStatus = syncStatus,
        });

        return response;
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
