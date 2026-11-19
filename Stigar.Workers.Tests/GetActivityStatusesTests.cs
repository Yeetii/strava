using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Moq;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Api.Endpoints.User;

namespace Stigar.Workers.Tests;

public class GetActivityStatusesTests
{
    [Fact]
    public async Task Handle_WithoutValidSession_ReturnsUnauthorized()
    {
        var authService = CreateUserAuthenticationService("session-123", null);
        var activitiesRepository = new Mock<IActivityRepository>();
        var endpoint = new GetActivityStatuses(authService, activitiesRepository.Object);
        var context = new DefaultHttpContext();

        var result = await endpoint.Handle(context);

        Assert.Equal(StatusCodes.Status401Unauthorized, Assert.IsAssignableFrom<IStatusCodeHttpResult>(result).StatusCode);
    }

    [Fact]
    public void ParseLimit_UsesDefaultAndBounds()
    {
        Assert.Equal(10, GetActivityStatuses.ParseLimit(null));
        Assert.Equal(10, GetActivityStatuses.ParseLimit("abc"));
        Assert.Equal(1, GetActivityStatuses.ParseLimit("0"));
        Assert.Equal(100, GetActivityStatuses.ParseLimit("1000"));
        Assert.Equal(15, GetActivityStatuses.ParseLimit("15"));
    }

    private static UserAuthenticationService CreateUserAuthenticationService(string? sessionId, Stigar.Core.Models.User? user)
    {
        var sessions = new Mock<ISessionRepository>();
        sessions
            .Setup(r => r.GetById(sessionId!, It.IsAny<CancellationToken>()))
            .ReturnsAsync(sessionId == null ? null : new Session { Id = sessionId, UserId = user?.Id ?? "user-1" });

        var users = new Mock<IUserRepository>();
        users
            .Setup(r => r.GetById(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(user);

        return new UserAuthenticationService(users.Object, sessions.Object);
    }

}
