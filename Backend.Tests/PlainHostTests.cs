using API.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Backend.Tests;

/// <summary>
/// Guards the native minimal-API host: every endpoint is registered explicitly,
/// behind /api/, with no duplicate route/verb pairs, and no Functions trigger
/// attributes or reflection mappers remain.
/// </summary>
public class PlainHostTests
{
    [Fact]
    public async Task MapApiEndpoints_PreservesRoutePrefixAndRoutes()
    {
        await using var app = BuildApp();

        var routes = RouteTexts(app);

        Assert.Contains("/api/activities", routes);
        Assert.Contains("/api/StravaWebhook", routes);
        Assert.Contains("/api/manage/status", routes);
        Assert.DoesNotContain("/api/connectSignalR", routes);
        // The public PMTiles route is intentionally outside the /api prefix.
        Assert.All(routes.Where(route => route != "/tiles/trails.pmtiles"),
            route => Assert.StartsWith("/api/", route));
    }

    [Fact]
    public async Task MappedEndpoints_HaveNoRouteAndVerbCollisions()
    {
        await using var app = BuildApp();

        var pairs = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .SelectMany(endpoint => endpoint.Metadata
                .OfType<Microsoft.AspNetCore.Routing.IHttpMethodMetadata>()
                .SelectMany(meta => meta.HttpMethods)
                .Select(method => $"{method.ToUpperInvariant()} {endpoint.RoutePattern.RawText}"))
            .ToList();

        var duplicates = pairs.GroupBy(pair => pair)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToList();
        Assert.True(duplicates.Count == 0,
            $"Duplicate route/verb pairs: {string.Join(", ", duplicates)}");
    }

    [Fact]
    public void NoAzureFunctionsTriggerAttributesRemain()
    {
        var assemblies = new[]
        {
            typeof(API.Endpoints.User.GetActivities).Assembly,
            typeof(BackendServices).Assembly,
        };

        foreach (var assembly in assemblies)
        {
            var stale = assembly.GetTypes()
                .SelectMany(type => type.GetMethods(
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.DeclaredOnly))
                .Where(method => method.GetCustomAttributes(inherit: false)
                    .Any(attribute => attribute.GetType().FullName is
                        "Microsoft.Azure.Functions.Worker.FunctionAttribute" or
                        "Microsoft.Azure.Functions.Worker.HttpTriggerAttribute" or
                        "Microsoft.Azure.Functions.Worker.QueueTriggerAttribute" or
                        "Microsoft.Azure.Functions.Worker.TimerTriggerAttribute"))
                .Select(method => $"{method.DeclaringType?.Name}.{method.Name}")
                .ToList();

            Assert.True(stale.Count == 0,
                $"{assembly.GetName().Name} still has Functions triggers: {string.Join(", ", stale)}");
        }
    }

    private static WebApplication BuildApp()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSingleton<IConfiguration>(builder.Configuration);
        builder.Services.AddApiEndpoints();
        var app = builder.Build();
        app.MapApiEndpoints();
        return app;
    }

    private static List<string?> RouteTexts(WebApplication app) => ((IEndpointRouteBuilder)app).DataSources
        .SelectMany(source => source.Endpoints)
        .OfType<RouteEndpoint>()
        .Select(endpoint => endpoint.RoutePattern.RawText)
        .ToList();
}
