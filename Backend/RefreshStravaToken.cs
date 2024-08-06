using System.Configuration;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Shared.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;


namespace Dashboard.RefreshStravaToken
{

    public class TokenResponse {
        [JsonPropertyName("access_token")]
        public string? AccessToken {get; set;}
        [JsonPropertyName("expires_at")]
        public long ExpiresAt {get; set;}
    }

    public class RefreshStravaToken
    {
        private static readonly HttpClient client = new();
        private readonly string ClientSecret; 

        public RefreshStravaToken(IConfiguration configuration)
        {
            ClientSecret = configuration.GetValue<string>("StravaClientSecret") ?? throw new ConfigurationErrorsException("No strava client secret found");
        }

        [Function("RefreshStravaToken")]
        public async Task<ReturnType> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "{userId}/accessToken")] HttpRequestData req, string userId,
            [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%UsersContainer%",
            Connection  = "CosmosDBConnection",
            Id = "{userId}",
            PartitionKey = "{userId}")] User user,
            ILogger log)
        {
            if (user.AccessToken != null && user.TokenExpiresAt > new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
            {
                var tokenStillWorksResponse = req.CreateResponse(System.Net.HttpStatusCode.OK);
                tokenStillWorksResponse.WriteString(user.AccessToken);
                return new ReturnType{Result = tokenStillWorksResponse};
            }

            var RefreshToken = user.RefreshToken;

            if (RefreshToken == null)
            {
                var notFoundResult = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                notFoundResult.WriteString($"Refresh token not found for user {userId}");
                return new ReturnType{Result = notFoundResult};
            }

            var postBodyValues = new Dictionary<string, string>
            {
                { "client_id", "26280" },
                { "client_secret", ClientSecret },
                { "refresh_token", RefreshToken },
                { "grant_type", "refresh_token" }
            };

            var content = new FormUrlEncodedContent(postBodyValues);   
            var response = await client.PostAsync("https://www.strava.com/api/v3/oauth/token", content);
            var tokenResponse = await response.Content.ReadFromJsonAsync<TokenResponse>();

            user.AccessToken = tokenResponse.AccessToken;
            user.TokenExpiresAt = tokenResponse.ExpiresAt;

            var result = req.CreateResponse(System.Net.HttpStatusCode.OK);
            result.WriteString(tokenResponse.AccessToken);

            return new ReturnType{Result = result, WriteToUser = user};
        }

        public class ReturnType
        {
            [HttpResult]
            public required HttpResponseData Result { get; set;}
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? WriteToUser { get; set;}
        }
    }
}