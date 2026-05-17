using System.Net;
using System.Security.Claims;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Models;
using Shared.Services;
using API.Endpoints.User;

namespace Backend.Tests;

public class GetActivityStatusesTests
{
    [Fact]
    public async Task Run_WithoutValidSession_ReturnsUnauthorized()
    {
        var authService = CreateUserAuthenticationService("session-123", null);
        var activitiesCollection = CreateActivitiesCollection();
        var endpoint = new GetActivityStatuses(authService, activitiesCollection);
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

    private static CollectionClient<Activity> CreateActivitiesCollection()
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddFilter(_ => false));
        return new CollectionClient<Activity>(new Mock<Container>().Object, loggerFactory);
    }

    private static UserAuthenticationService CreateUserAuthenticationService(string? sessionId, Shared.Models.User? user)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddFilter(_ => false));

        var sessionsContainer = new Mock<Container>();
        if (sessionId == null)
        {
            sessionsContainer
                .Setup(c => c.ReadItemAsync<Session>(It.IsAny<string>(), It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new CosmosException("Not found", HttpStatusCode.NotFound, 0, string.Empty, 0));
        }
        else
        {
            var session = new Session { Id = sessionId, UserId = user?.Id ?? "user-1" };
            var sessionResponse = Mock.Of<ItemResponse<Session>>(r => r.Resource == session);
            sessionsContainer
                .Setup(c => c.ReadItemAsync<Session>(sessionId, It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
                .ReturnsAsync(sessionResponse);
        }

        var usersContainer = new Mock<Container>();
        if (user == null)
        {
            usersContainer
                .Setup(c => c.ReadItemAsync<Shared.Models.User>(It.IsAny<string>(), It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new CosmosException("Not found", HttpStatusCode.NotFound, 0, string.Empty, 0));
        }
        else
        {
            var userResponse = Mock.Of<ItemResponse<Shared.Models.User>>(r => r.Resource == user);
            usersContainer
                .Setup(c => c.ReadItemAsync<Shared.Models.User>(user.Id, It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
                .ReturnsAsync(userResponse);
        }

        return new UserAuthenticationService(
            new CollectionClient<Shared.Models.User>(usersContainer.Object, loggerFactory),
            new CollectionClient<Session>(sessionsContainer.Object, loggerFactory));
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
