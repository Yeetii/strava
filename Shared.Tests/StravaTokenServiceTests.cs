using System.Net;
using System.Text;
using Microsoft.Extensions.Configuration;
using Moq;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.StravaClient;

namespace Shared.Tests;

public class StravaTokenServiceTests
{
    [Fact]
    public async Task GetValidAccessToken_RefreshesExpiredTokenAndPersistsViaUserRepository()
    {
        var authApi = CreateAuthApi("""
            {"token_type":"Bearer","expires_at":1893456000,"expires_in":21600,"refresh_token":"new-refresh","access_token":"new-access","scope":"read,activity:read_all"}
            """);
        var repo = new Mock<IUserRepository>();
        var service = new StravaTokenService(authApi, repo.Object);
        var user = CreateUser();

        var token = await service.GetValidAccessToken(user);

        Assert.Equal("new-access", token);
        Assert.Equal("new-access", user.AccessToken);
        Assert.Equal(1893456000, user.TokenExpiresAt);
        Assert.Equal("read,activity:read_all", user.StravaScope);
        repo.Verify(r => r.Upsert(user, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetValidAccessToken_ValidCachedToken_DoesNotRefreshOrWrite()
    {
        var authApi = CreateAuthApi("{}");
        var repo = new Mock<IUserRepository>();
        var service = new StravaTokenService(authApi, repo.Object);
        var user = CreateUser();
        user.AccessToken = "still-valid";
        user.TokenExpiresAt = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds();

        var token = await service.GetValidAccessToken(user);

        Assert.Equal("still-valid", token);
        repo.Verify(r => r.Upsert(It.IsAny<User>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetValidAccessToken_RefreshFails_ReturnsNullWithoutWrite()
    {
        var handler = new StubHandler(HttpStatusCode.BadRequest, "{}");
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["StravaClientSecret"] = "secret" })
            .Build();
        var authApi = new Shared.Services.StravaClient.AuthenticationApi(new HttpClient(handler), config);
        var repo = new Mock<IUserRepository>();
        var service = new StravaTokenService(authApi, repo.Object);
        var user = CreateUser();

        var token = await service.GetValidAccessToken(user);

        Assert.Null(token);
        repo.Verify(r => r.Upsert(It.IsAny<User>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static AuthenticationApi CreateAuthApi(string tokenJson)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["StravaClientSecret"] = "secret" })
            .Build();
        return new AuthenticationApi(new HttpClient(new StubHandler(HttpStatusCode.OK, tokenJson)), config);
    }

    private static User CreateUser() => new()
    {
        Id = "user-1",
        RefreshToken = "old-refresh",
        AccessToken = "old-access",
        TokenExpiresAt = 1,
        SyncStatus = new StravaSyncStatus { ProcessedActivities = new StravaProcessedActivityCounts() }
    };

    private sealed class StubHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });
    }
}
