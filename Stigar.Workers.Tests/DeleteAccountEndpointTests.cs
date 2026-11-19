using Hangfire.Common;
using Hangfire.States;
using Hangfire;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Moq;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Api.Endpoints.User;

namespace Stigar.Workers.Tests;

public class DeleteAccountEndpointTests
{
    [Fact]
    public async Task Handle_WithValidSession_QueuesDeleteJobAndClearsSessionCookie()
    {
        var user = new Stigar.Core.Models.User { Id = "user-123" };
        var authService = CreateUserAuthenticationService("session-123", user);
        var jobs = new Mock<IBackgroundJobClient>();
        var context = CreateContext("session-123");

        var endpoint = new DeleteAccount(authService, jobs.Object);
        var result = await endpoint.Handle(context, CancellationToken.None);

        Assert.Equal(StatusCodes.Status204NoContent, Assert.IsAssignableFrom<IStatusCodeHttpResult>(result).StatusCode);
        jobs.Verify(j => j.Create(It.Is<Job>(job =>
            job.Method.Name == "ExecuteAsync" && job.Queue == QueueConfig.AccountDeleteJobs.ToLowerInvariant()),
            It.IsAny<IState>()), Times.Once);
        Assert.Contains("session=", context.Response.Headers["Set-Cookie"].ToString());
    }

    [Fact]
    public async Task Handle_WithoutValidSession_ReturnsUnauthorized()
    {
        var authService = CreateUserAuthenticationService("session-123", null);
        var jobs = new Mock<IBackgroundJobClient>();
        var context = CreateContext(null);

        var endpoint = new DeleteAccount(authService, jobs.Object);
        var result = await endpoint.Handle(context, CancellationToken.None);

        Assert.Equal(StatusCodes.Status401Unauthorized, Assert.IsAssignableFrom<IStatusCodeHttpResult>(result).StatusCode);
        jobs.VerifyNoOtherCalls();
    }

    private static DefaultHttpContext CreateContext(string? sessionId)
    {
        var context = new DefaultHttpContext();
        if (sessionId is not null)
            context.Request.Headers["Cookie"] = $"session={sessionId}";
        return context;
    }

    private static UserAuthenticationService CreateUserAuthenticationService(string? sessionId, Stigar.Core.Models.User? user)
    {
        var sessions = new Mock<ISessionRepository>();
        sessions
            .Setup(r => r.GetById(sessionId!, It.IsAny<CancellationToken>()))
            .ReturnsAsync(sessionId == null ? null : new Session { Id = sessionId, UserId = user?.Id ?? "user-123" });

        var users = new Mock<IUserRepository>();
        users
            .Setup(r => r.GetById(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(user);

        return new UserAuthenticationService(users.Object, sessions.Object);
    }
}
