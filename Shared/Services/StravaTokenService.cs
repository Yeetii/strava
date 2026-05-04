using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Services.StravaClient;
using Shared.Services.StravaClient.Model;

namespace Shared.Services;

public class StravaTokenService(
    AuthenticationApi _authApi,
    CollectionClient<Models.User> _usersCollection)
{
    public async Task<string?> GetValidAccessToken(Models.User user)
    {
        if (user.RefreshToken == null)
            return null;

        if (user.AccessToken != null && user.TokenExpiresAt > DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            return user.AccessToken;

        TokenResponse tokenResponse;
        try
        {
            tokenResponse = await _authApi.RefreshToken(user.RefreshToken);
        }
        catch
        {
            return null;
        }

        user.AccessToken = tokenResponse.AccessToken;
        user.TokenExpiresAt = tokenResponse.ExpiresAt;
        await _usersCollection.PatchDocument(
            user.Id,
            new PartitionKey(user.Id),
            [
                PatchOperation.Set("/accessToken", tokenResponse.AccessToken),
                PatchOperation.Set("/tokenExpiresAt", tokenResponse.ExpiresAt)
            ]);

        return tokenResponse.AccessToken;
    }
}
