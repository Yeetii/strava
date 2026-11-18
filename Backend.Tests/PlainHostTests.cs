using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Azure.Functions.Worker;
using Shared.Hosting;

namespace Backend.Tests;

/// <summary>
/// Guards the plain-host envelope introduced by the Azure Functions Runtime
/// Exit plan: every trigger must be discovered, routes must stay identical, and
/// the Hangfire shim must produce a usable queue envelope.
/// </summary>
public class PlainHostTests
{
    [Fact]
    public void Discover_Backend_FindsFullTriggerInventory()
    {
        var registry = FunctionWorkerRegistry.Discover(typeof(BackendServices).Assembly);

        Assert.Equal(11, registry.Queues.Count);
        Assert.Equal(2, registry.ChangeFeeds.Count);
        Assert.Equal(12, registry.Timers.Count);
    }

    [Theory]
    [InlineData("0 0 2 * * 1", 10)]
    [InlineData("0 0 3 * * 1", 1)]
    [InlineData("0 0 3 * * 2", 1)]
    public void Timers_KeepCronStringsVerbatim(string cron, int expectedCount)
    {
        var registry = FunctionWorkerRegistry.Discover(typeof(BackendServices).Assembly);

        Assert.Equal(expectedCount, registry.Timers.Count(d =>
            FunctionAttributeLookup.Get<TimerTriggerAttribute>(d.Method)!.Schedule == cron));
    }

    [Fact]
    public async Task MapFunctions_PreservesRoutePrefixAndRoutes()
    {
        var builder = WebApplication.CreateBuilder();
        await using var app = builder.Build();

        app.MapFunctions(typeof(API.Endpoints.User.GetActivities).Assembly);

        var routes = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(endpoint => endpoint.RoutePattern.RawText)
            .ToList();

        Assert.Contains("/api/activities", routes);
        Assert.DoesNotContain("/api/connectSignalR", routes);
        Assert.All(routes, route => Assert.StartsWith("/api/", route));
    }

    [Fact]
    public void HttpFunctions_HaveUniqueRouteAndVerbPairs()
    {
        foreach (var assembly in new[]
                 {
                     typeof(API.Endpoints.User.GetActivities).Assembly,
                     typeof(BackendServices).Assembly,
                 })
        {
            var routes = assembly.GetTypes()
                .Where(type => type.IsClass && !type.IsAbstract)
                .SelectMany(type => type.GetMethods(
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.DeclaredOnly))
                .Select(method => (Trigger: FunctionAttributeLookup.Get<HttpTriggerAttribute>(method), Method: method))
                .Where(item => item.Trigger is not null)
                .SelectMany(item =>
                {
                    var route = string.IsNullOrWhiteSpace(item.Trigger!.Route)
                        ? item.Method.GetCustomAttribute<FunctionAttribute>()?.Name ?? item.Method.Name
                        : item.Trigger.Route!;
                    return item.Trigger.Methods.Select(verb =>
                        $"{verb.ToUpperInvariant()} /api/{route}");
                })
                .ToList();

            var duplicates = routes.GroupBy(route => route)
                .Where(group => group.Count() > 1)
                .Select(group => group.Key)
                .ToList();
            Assert.True(duplicates.Count == 0,
                $"Duplicate route/verb pairs in {assembly.GetName().Name}: {string.Join(", ", duplicates)}");
        }
    }

    [Fact]
    public void MergedHost_HasNoCrossAssemblyRouteAndVerbCollisions()
    {
        var routes = new[]
                     {
                         typeof(API.Endpoints.User.GetActivities).Assembly,
                         typeof(BackendServices).Assembly,
                     }
                     .SelectMany(assembly => assembly.GetTypes()
                         .Where(type => type.IsClass && !type.IsAbstract)
                         .SelectMany(type => type.GetMethods(
                             BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                         .Select(method => (Trigger: FunctionAttributeLookup.Get<HttpTriggerAttribute>(method), Method: method)))
                     .Where(item => item.Trigger is not null)
                     .SelectMany(item =>
                     {
                         var name = item.Method.GetCustomAttribute<FunctionAttribute>()?.Name ?? item.Method.Name;
                         var route = string.IsNullOrWhiteSpace(item.Trigger!.Route) ? name : item.Trigger.Route!;
                         var verbs = item.Trigger.Methods.Length == 0 ? new[] { "get" } : item.Trigger.Methods;
                         return verbs.Select(verb => $"{verb.ToUpperInvariant()} /api/{route}");
                     })
                     .ToList();

        Assert.Contains("GET /api/StravaWebhook", routes);
        Assert.Contains("POST /api/StravaWebhook", routes);

        var collisions = routes.GroupBy(route => route)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToList();

        Assert.True(collisions.Count == 0,
            $"Cross-assembly route/verb collisions: {string.Join(", ", collisions)}");
    }

    [Fact]
    public async Task AddFunctionTimers_RegistersRecurringJobsViaInjectedManager()
    {
        var registry = FunctionWorkerRegistry.Discover(typeof(BackendServices).Assembly);
        var manager = new Moq.Mock<Hangfire.IRecurringJobManager>();

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<Hangfire.IRecurringJobManager>(manager.Object);
        builder.AddFunctionTimers(registry, (_, _, _) => Task.CompletedTask);

        using var host = builder.Build();
        var registration = host.Services
            .GetRequiredService<IEnumerable<Microsoft.Extensions.Hosting.IHostedService>>()
            .OfType<RecurringJobRegistrationHostedService>()
            .Single();

        await registration.StartAsync(CancellationToken.None);

        manager.Verify(m => m.AddOrUpdate(
            Moq.It.IsAny<string>(),
            Moq.It.IsAny<Hangfire.Common.Job>(),
            Moq.It.IsAny<string>(),
            Moq.It.IsAny<Hangfire.RecurringJobOptions>()), Moq.Times.Exactly(12));
    }

    [Fact]
    public void QueueEnvelope_RoundTripsBodyAndMetadata()
    {
        var envelope = QueueMessageEnvelope.From(new QueueMessage(BinaryData.FromString("activity-1"))
        {
            MessageId = "m1",
            ContentType = "application/json",
            ApplicationProperties = { ["country"] = "AF" },
        });

        var message = envelope.ToMessage();

        Assert.Equal("activity-1", message.Body.ToString());
        Assert.Equal("m1", message.MessageId);
        Assert.Equal("application/json", message.ContentType);
        Assert.Equal("AF", message.ApplicationProperties["country"]);
    }
}
