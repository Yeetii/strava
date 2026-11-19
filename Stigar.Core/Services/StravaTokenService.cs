using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.StravaClient;
using Stigar.Core.Services.StravaClient.Model;

namespace Stigar.Core.Services;

public class StravaTokenService(
    AuthenticationApi _authApi,
    IUserRepository _usersRepository)
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
        if (!string.IsNullOrWhiteSpace(tokenResponse.Scope))
            user.StravaScope = tokenResponse.Scope;

        await _usersRepository.Upsert(user);

        return tokenResponse.AccessToken;
    }
}
