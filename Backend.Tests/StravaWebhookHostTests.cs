using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Shared.Hosting;

namespace Backend.Tests;

/// <summary>
/// Regression tests for the plain-host HTTP envelope on the StravaWebhook
/// endpoint, the only endpoint whose OutputBindings carry an IActionResult
/// [HttpResult]. Azure Functions semantics require: GET verification answers
/// 403 for a bad verify token, 200 with the hub.challenge echo for a good
/// one, and POST events answer 200 "EVENT_RECEIVED". Each response must be
/// written exactly once and terminate cleanly — never an aborted transfer.
/// </summary>
public class StravaWebhookHostTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["stravaVerifyToken"] = "test-verify-token";
        builder.Services.AddCors(options => options.AddDefaultPolicy(policy =>
            policy.WithOrigins("https://trailscope.example")
                .AllowAnyHeader().AllowAnyMethod().AllowCredentials()));
        var app = builder.Build();
        app.UseCors();
        app.MapFunctions(typeof(BackendServices).Assembly);
        app.Urls.Add("http://127.0.0.1:0");
        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses;
        _app = app;
        _client = new HttpClient { BaseAddress = new Uri(addresses.Single()) };
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task Verification_BadToken_AnswersForbidden()
    {
        var response = await _client.GetAsync(
            "/api/StravaWebhook?hub.mode=subscribe&hub.verify_token=wrong-token&hub.challenge=abc");

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task Verification_GoodToken_EchoesChallenge()
    {
        var response = await _client.GetAsync(
            "/api/StravaWebhook?hub.mode=subscribe&hub.verify_token=test-verify-token&hub.challenge=abc123");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("{\"hub.challenge\": \"abc123\"}", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Verification_GoodToken_WithOriginHeader_Completes()
    {
        // Strava never sends Origin, but an allowed origin must still receive
        // its CORS headers from the single UseCors authority, even though the
        // endpoint wrote the body directly.
        var request = new HttpRequestMessage(HttpMethod.Get,
            "/api/StravaWebhook?hub.mode=subscribe&hub.verify_token=test-verify-token&hub.challenge=abc123");
        request.Headers.Add("Origin", "https://trailscope.example");

        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("https://trailscope.example",
            Assert.Single(response.Headers.GetValues("Access-Control-Allow-Origin")));
        Assert.Equal("{\"hub.challenge\": \"abc123\"}", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Verification_DisallowedOrigin_ReceivesNoCorsHeaders()
    {
        // The body-less 403 path is where the mapper used to reflect an
        // arbitrary Origin back with credentials. Only the configured UseCors
        // policy may emit CORS headers.
        var request = new HttpRequestMessage(HttpMethod.Get,
            "/api/StravaWebhook?hub.mode=subscribe&hub.verify_token=wrong-token&hub.challenge=abc");
        request.Headers.Add("Origin", "https://evil.example");

        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.False(response.Headers.TryGetValues("Access-Control-Allow-Origin", out _));
        Assert.False(response.Headers.TryGetValues("Access-Control-Allow-Credentials", out _));
    }

    [Fact]
    public async Task EventPost_AnswersEventReceived()
    {
        const string json =
            """{"object_type":"activity","aspect_type":"create","object_id":"123","owner_id":"456","updates":{}}""";

        var response = await _client.PostAsync(
            "/api/StravaWebhook", new StringContent(json));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("EVENT_RECEIVED", await response.Content.ReadAsStringAsync());
    }
}
