using a2n.Hangfire.Dashboard;
using Stigar.Api;
using Stigar.Api.Http;
using Stigar.Api.Utils;
using Stigar.Core.Geo.SummitsCalculator;
using Hangfire;
using System.Net.Http.Headers;
using Npgsql;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Stigar.Core.Constants;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.GarminClient;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;
using Stigar.Core.Services.Shards;
using Stigar.Core.Services.StravaClient;
using Stigar.Core.Jobs;

// ASP.NET Core (Kestrel) host for the API container. Endpoints are mapped as
// native minimal APIs by EndpointMappings; the DI graph below is shared with
// the Workers host through the core assembly.

var builder = WebApplication.CreateBuilder(args);

// Minimal API results (TypedResults.Ok / Results.Json) serialize with these options.
// Without them GeoJSON enums become numbers, FeatureId becomes an object and geometry
// coordinates are omitted, so a Configure<JsonSerializerOptions> call is not enough.
builder.Services.ConfigureHttpJsonOptions(options => GeoJsonJsonOptions.Configure(options.SerializerOptions));

var configuration = builder.Configuration;

// Shared DI graph: Postgres/repositories, shard services, HTTP clients,
// authentication and calculators. See StigarSharedServiceCollectionExtensions.
builder.Services.AddStigarSharedServices(configuration);

// API-host-only registrations.
builder.Services.AddSingleton(serviceProvider =>
{
    var featureClient = serviceProvider.GetRequiredService<ShardFeatureClient>();
    var zoomIndexService = serviceProvider.GetRequiredService<HighwayZoomIndexService>();
    var tileLogger = serviceProvider.GetRequiredService<ILogger<BlobTileService>>();
    var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
    return new BlobTileService(featureClient, zoomIndexService, tileLogger, shardZoom);
});
builder.Services.AddHttpClient("garminProxyClient", client =>
{
    var garminProxyBaseUrl = configuration.GetValue<string>("GarminProxyBaseUrl")
        ?? "http://localhost:7073/api/";
    client.BaseAddress = new Uri(garminProxyBaseUrl);
    var functionsKey = configuration.GetValue<string>("LIFEDASH_FUNCTIONS_KEY");
    if (!string.IsNullOrWhiteSpace(functionsKey))
        client.DefaultRequestHeaders.TryAddWithoutValidation("x-functions-key", functionsKey);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
    var stravaClient = httpClientFactory.CreateClient("stravaClient");
    return new RoutesApi(stravaClient);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
    var garminProxyClient = httpClientFactory.CreateClient("garminProxyClient");
    return new GarminCoursesApi(garminProxyClient);
});
// Hangfire's dashboard reads every submitted form value in one shot when a
// batch command (e.g. retry/delete many jobs) is dispatched. ASP.NET Core's
// default ValueCountLimit of 1024 turns that into an unhandled 500.
builder.Services.Configure<FormOptions>(options => options.ValueCountLimit = 10000);

// All queues run on Hangfire/Redis. Storage is registered once per host;
// producers enqueue strongly-typed jobs via IBackgroundJobClient.
HangfireInfrastructure.AddHangfireStorage(builder.Services, configuration);
builder.Services.AddHangfireDashboardUI();
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(
        configuration[AppConfig.RedisConnectionString]
        ?? configuration.GetConnectionString(AppConfig.RedisConnectionString)
        ?? "redis:6379,abortConnect=false"));
builder.Services.AddSingleton(serviceProvider =>
    serviceProvider.GetRequiredService<IConnectionMultiplexer>().GetDatabase());
builder.Services.AddScoped<UserSyncService>();

var defaultOrigins = new[]
{
    "https://peakshunters.erikmagnusson.com",
    "https://stigar.erikmagnusson.com",
};

var configuredOrigins = configuration.GetValue<string>("AllowedOrigins")?
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    ?? [];

var allowedOrigins = configuredOrigins.Length > 0
    ? configuredOrigins.Union(defaultOrigins, StringComparer.OrdinalIgnoreCase).ToArray()
    : defaultOrigins;

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials()
            .WithExposedHeaders("Content-Range", "Content-Length", "ETag", "Accept-Ranges");
    });

    options.AddPolicy(Stigar.Api.Endpoints.Tiles.GetTrailsPmtiles.CorsPolicy, policy =>
    {
        // This is a public, anonymous tile endpoint with an open origin matcher.
        // AllowCredentials() must stay off: wildcard origins plus credentials is
        // rejected by browsers and would let any site read credentialed responses.
        policy.SetIsOriginAllowed(_ => true)
            .AllowAnyHeader()
            .WithMethods("GET", "HEAD", "OPTIONS")
            .WithExposedHeaders("Content-Range", "Content-Length", "ETag", "Accept-Ranges");
    });
});
builder.Services.AddOpenApi();
builder.Services.AddApiEndpoints();

var app = builder.Build();


app.Use((context, next) =>
{
    if (context.Request.Headers.ContainsKey("Access-Control-Request-Private-Network") ||
        context.Request.Headers.ContainsKey("Origin"))
    {
        context.Response.Headers.Append("Access-Control-Allow-Private-Network", "true");
    }

    return next(context);
});
app.UseCors();
app.MapOpenApi();

// Hangfire dashboard is tailnet-only: Funnel traffic carries a strip-proof
// header and is rejected outright; everything else needs the admin key via
// HTTP Basic auth. Fail closed when AdminApiKey is not configured.
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments(
            HangfireDashboardGuard.DashboardPath, StringComparison.OrdinalIgnoreCase))
    {
        if (HangfireDashboardGuard.IsFunnelRequest(context) ||
            !HangfireDashboardGuard.IsAuthorized(context, configuration["AdminApiKey"]))
        {
            context.Response.Headers.WWWAuthenticate = "Basic realm=\"Hangfire\"";
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }
    }

    await next(context);
});
// The a2n filter applies the same gate to the dashboard pages, its Blazor
// circuit and its SignalR hub; the middleware above covers every /hangfire
// request as well, including static assets. Both must stay in place.
app.UseHangfireDashboardUI(HangfireDashboardGuard.DashboardPath, new DashboardUIOptions
{
    Authorization = [new HangfireDashboardAuthorizationFilter(configuration)],
    DashboardTitle = "Peakshunters Jobs",
});

app.MapApiEndpoints();

app.MapGet("/health", () => Results.Ok("ok"));

app.Run();
