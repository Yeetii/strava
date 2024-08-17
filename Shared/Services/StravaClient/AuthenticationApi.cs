using System.Configuration;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public class AuthenticationApi(HttpClient _httpClient, IConfiguration configuration)
{
    readonly string ClientSecret = configuration.GetValue<string>("StravaClientSecret") ?? throw new ConfigurationErrorsException("No strava client secret found");
    const string BaseUrl = "https://www.strava.com/api/v3/oauth/token";

    public async Task<TokenResponse> RefreshToken(string refreshToken)
    {
        var postBodyValues = new Dictionary<string, string>
        {
            { "client_id", "26280" },
            { "client_secret", ClientSecret },
            { "refresh_token", refreshToken },
            { "grant_type", "refresh_token" }
        };

        return await CommonTokenExchange(postBodyValues);
    }

    public async Task<TokenResponse> TokenExcange(string authToken)
    {
        var postBodyValues = new Dictionary<string, string>
        {
            { "client_id", "26280" },
            { "client_secret", ClientSecret },
            { "code", authToken },
            { "grant_type", "authorization_code" }
        };

        return await CommonTokenExchange(postBodyValues);
    }

    private async Task<TokenResponse> CommonTokenExchange(Dictionary<string, string> content)
    {
        var encodedContent = new FormUrlEncodedContent(content);
        var response = await _httpClient.PostAsync(BaseUrl, encodedContent);
        
        var tokenResponse = await response.Content.ReadFromJsonAsync<TokenResponse>()
            ?? throw new JsonException("Could not parse token response");

        return tokenResponse;
    }
}