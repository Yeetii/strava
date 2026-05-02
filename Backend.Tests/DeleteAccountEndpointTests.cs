using System.Net;
using System.Security.Claims;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Models;
using Shared.Services;
using API.Endpoints.User;

namespace Backend.Tests;

public class DeleteAccountEndpointTests
{
    [Fact]
    public async Task Run_WithValidSession_QueuesDeleteJobAndClearsSessionCookie()
    {
        var user = new Shared.Models.User { Id = "user-123" };
        var authService = CreateUserAuthenticationService("session-123", user);

        var functionContext = new Mock<FunctionContext>().Object;
        var request = new TestHttpRequestData(functionContext, new Uri("https://localhost/account"), "DELETE");
        request.AddCookie(new HttpCookie("session", "session-123"));

        var function = new DeleteAccount(authService);
        var outputs = await function.Run(request);

        Assert.Equal(HttpStatusCode.NoContent, outputs.Response.StatusCode);
        Assert.NotNull(outputs.AccountDeleteJob);
        Assert.Equal("user-123", outputs.AccountDeleteJob!.UserId);

        var sessionCookie = Assert.Single(((TestHttpCookies)outputs.Response.Cookies).Items);
        Assert.Equal("session", sessionCookie.Name);
        Assert.Equal(string.Empty, sessionCookie.Value);
        Assert.Equal(0, sessionCookie.MaxAge);
    }

    [Fact]
    public async Task Run_WithoutValidSession_ReturnsUnauthorized()
    {
        var authService = CreateUserAuthenticationService("session-123", null);

        var functionContext = new Mock<FunctionContext>().Object;
        var request = new TestHttpRequestData(functionContext, new Uri("https://localhost/account"), "DELETE");

        var function = new DeleteAccount(authService);
        var outputs = await function.Run(request);

        Assert.Equal(HttpStatusCode.Unauthorized, outputs.Response.StatusCode);
        Assert.Null(outputs.AccountDeleteJob);
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
            var session = new Session { Id = sessionId, UserId = user?.Id ?? "user-123" };
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
        private readonly List<IHttpCookie> _cookies = new();
        public override IReadOnlyCollection<IHttpCookie> Cookies => _cookies;
        public override Stream Body { get; } = new MemoryStream();

        public TestHttpRequestData(FunctionContext functionContext, Uri url, string method)
            : base(functionContext)
        {
            Url = url;
            Method = method;
        }

        public void AddCookie(HttpCookie cookie) => _cookies.Add(cookie);

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
        private readonly List<IHttpCookie> _cookies = new();

        public override void Append(string name, string value)
        {
            _cookies.Add(new HttpCookie(name, value));
        }

        public override void Append(IHttpCookie cookie)
        {
            _cookies.Add(cookie);
        }

        public override IHttpCookie CreateNew()
        {
            return new HttpCookie(string.Empty, string.Empty);
        }

        public IReadOnlyCollection<IHttpCookie> Items => _cookies;
    }
}
