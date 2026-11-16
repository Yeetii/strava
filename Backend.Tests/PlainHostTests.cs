using System.Reflection;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Azure.Functions.Worker;
using Shared.Hosting;

namespace Backend.Tests;

/// <summary>
/// Guards the plain-host envelope introduced by the Azure Functions Runtime
/// Exit plan: every trigger must be discovered, routes must stay identical, and
/// the Hangfire shim must produce a usable Service Bus envelope.
/// </summary>
public class PlainHostTests
{
    [Fact]
    public void Discover_Backend_FindsFullTriggerInventory()
    {
        var registry = FunctionWorkerRegistry.Discover(typeof(BackendServices).Assembly);

        Assert.Equal(12, registry.ServiceBus.Count);
        Assert.Equal(3, registry.ChangeFeeds.Count);
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

        app.MapFunctions(typeof(API.SignalRNegotiate).Assembly);

        var routes = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(endpoint => endpoint.RoutePattern.RawText)
            .ToList();

        Assert.Contains("/api/activities", routes);
        Assert.Contains("/api/connectSignalR", routes);
        Assert.All(routes, route => Assert.StartsWith("/api/", route));
    }

    [Fact]
    public void HttpFunctions_HaveUniqueRouteAndVerbPairs()
    {
        foreach (var assembly in new[]
                 {
                     typeof(API.SignalRNegotiate).Assembly,
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
    public void HangfireShim_ProducesUsableReceivedMessage()
    {
        var factory = typeof(QueueTransport).Assembly
            .GetType("Shared.Hosting.FakeReceivedMessage", throwOnError: true)!
            .GetMethod("Create", BindingFlags.Public | BindingFlags.Static)!;

        var message = (ServiceBusReceivedMessage)factory.Invoke(null, ["activity-1"])!;

        Assert.Equal("activity-1", message.Body.ToString());
        Assert.Equal(1, message.DeliveryCount);
        Assert.False(string.IsNullOrEmpty(message.LockToken));
    }
}
