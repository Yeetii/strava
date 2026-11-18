using System.Net;
using System.Security.Claims;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using API.Endpoints.User;

namespace Backend.Tests;

public class GetActivityStatusesTests
{
    [Fact]
    public async Task Run_WithoutValidSession_ReturnsUnauthorized()
    {
        var authService = CreateUserAuthenticationService("session-123", null);
        var activitiesRepository = new Mock<IActivityRepository>();
        var endpoint = new GetActivityStatuses(authService, activitiesRepository.Object);
        var request = new TestHttpRequestData(new Mock<FunctionContext>().Object, new Uri("https://localhost/activityStatuses"), "GET");

        var response = await endpoint.Run(request);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public void BuildQuery_UsesProjectionAndDescendingOrder()
    {
        var query = GetActivityStatuses.BuildQuery("user-1", 5);

        Assert.Contains("SELECT TOP 5", query.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.id", query.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.name", query.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.startDateLocal", query.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.processingStatus", query.QueryText, StringComparison.Ordinal);
        Assert.Contains("ORDER BY c.startDateLocal DESC", query.QueryText, StringComparison.Ordinal);
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

    private static UserAuthenticationService CreateUserAuthenticationService(string? sessionId, Shared.Models.User? user)
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

    private sealed class TestHttpRequestData : HttpRequestData
    {
        public override HttpHeadersCollection Headers { get; } = new();
        public override Uri Url { get; }
        public override IEnumerable<ClaimsIdentity> Identities { get; } = Array.Empty<ClaimsIdentity>();
        public override string Method { get; }
        public override IReadOnlyCollection<IHttpCookie> Cookies { get; } = Array.Empty<IHttpCookie>();
        public override Stream Body { get; } = new MemoryStream();

        public TestHttpRequestData(FunctionContext functionContext, Uri url, string method)
            : base(functionContext)
        {
            Url = url;
            Method = method;
        }

        public override HttpResponseData CreateResponse()
        {
            return new TestHttpResponseData(FunctionContext);
        }
    }

    private sealed class TestHttpResponseData : HttpResponseData
    {
        public override HttpStatusCode StatusCode { get; set; }
        public override HttpHeadersCollection Headers { get; set; } = new();
        public override Stream Body { get; set; } = new MemoryStream();
        public override HttpCookies Cookies { get; } = new TestHttpCookies();

        public TestHttpResponseData(FunctionContext functionContext)
            : base(functionContext)
        {
        }
    }

    private sealed class TestHttpCookies : HttpCookies
    {
        public override void Append(string name, string value)
        {
        }

        public override void Append(IHttpCookie cookie)
        {
        }

        public override IHttpCookie CreateNew()
        {
            return new HttpCookie(string.Empty, string.Empty);
        }
    }
}
