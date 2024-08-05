using System.Configuration;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using API.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Cosmos;
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
            PartitionKey = "{userId}")] API.Models.User user,
            ILogger log)
        {
            // TODO: Move function to backend, should include logic to check expiration on already stored access token
            // Should only have to call function with a user id and a fresh token should then be returned, without spamming strava

            var RefreshToken = user.RefreshToken;

            if (RefreshToken == null)
             return new ReturnType{Result = Results.NotFound($"Refresh token not found for user {userId}")};

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

            return new ReturnType{Result = Results.Ok(tokenResponse.AccessToken), WriteToUser = user};
        }

        public class ReturnType
        {
            [HttpResult]
            public required IResult Result { get; set;}
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public API.Models.User? WriteToUser { get; set;}
        }
    }
}