using System.Net;
using Azure.Messaging.ServiceBus;
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

namespace API.Endpoints;

public class PostLogin(
    AuthenticationApi _authenticationApi,
    CollectionClient<Shared.Models.User> _usersCollection,
    ServiceBusClient serviceBusClient,
    ILogger<PostLogin> _logger)
{
    private readonly ServiceBusSender _activitiesFetchSender = serviceBusClient.CreateSender(ServiceBusConfig.ActivitiesFetchJobs);

    [OpenApiOperation(tags: ["User management"])]
    [OpenApiParameter(name: "authCode", In = ParameterLocation.Path)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostLoginResponse), Description = "The login response containing the username")]
    [Function(nameof(PostLogin))]
    public async Task<ReturnBindings> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "{authCode}/login")] HttpRequestData req, string authCode)
    {
        var isLocal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT")); // set to "Development" locally
        var response = req.CreateResponse();
        var outputs = new ReturnBindings() { Response = response };

        if (string.IsNullOrEmpty(authCode))
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync("No tokens provided");
            return outputs;
        }

        var tokenResponse = await _authenticationApi.TokenExcange(authCode);
        var refreshToken = tokenResponse.RefreshToken;

        if (refreshToken == null || tokenResponse.Athlete == null)
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync("Auth token exchange did not provide refresh token or athlete");
            return outputs;
        }

        response.StatusCode = HttpStatusCode.OK;
        var sessionId = Guid.NewGuid();
        var cookie = new HttpCookie("session", sessionId.ToString())
        {
            MaxAge = TimeSpan.FromDays(30).TotalSeconds,
            SameSite = SameSite.ExplicitNone,
            Secure = true,
            Domain = isLocal ? "localhost" : "erikmagnusson.com",
            Path = "/"
        };
        response.Cookies.Append(cookie);

        var userId = tokenResponse.Athlete.Id.ToString();
        _logger.LogInformation("Logging in user {userId} ", userId);
        var userExist = await _usersCollection.GetByIdMaybe(userId, new Microsoft.Azure.Cosmos.PartitionKey(userId));

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
        outputs.User = user;

        if (userExist == null)
        {
            _logger.LogInformation("Creating new user {userId}, sending activities fetch jobs", userId);
            await _usersCollection.UpsertDocument(user, priority: CosmosWritePriority.High);
            await _activitiesFetchSender.SendMessageAsync(new ServiceBusMessage(System.Text.Json.JsonSerializer.Serialize(new ActivitiesFetchJob { UserId = userId })));
            outputs.User = null;
        }

        outputs.Session = new Session
        {
            Id = sessionId.ToString(),
            UserId = userId,
            CreatedAtUtc = DateTime.UtcNow
        };

        await response.WriteAsJsonAsync(new PostLoginResponse
        {
            UserId = tokenResponse.Athlete.Id.ToString(),
            Username = tokenResponse.Athlete.Username,
            FirstName = tokenResponse.Athlete.Firstname,
            LastName = tokenResponse.Athlete.Lastname,
            SyncStatus = syncStatus,
        });

        return outputs;
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

    public class ReturnBindings
    {
        [HttpResult]
        public required HttpResponseData Response { get; set; }
        [CosmosDBOutput(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer, Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
        public Shared.Models.User? User { get; set; }
        [CosmosDBOutput(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer, Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
        public Session? Session { get; set; }
    }
}
