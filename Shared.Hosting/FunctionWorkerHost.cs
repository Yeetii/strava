// Worker-side "envelope" for the Azure Functions Runtime Exit campaign (P1a).
// Queue consumers use the Service Bus SDK processor, timers become Hangfire
// recurring jobs, and Cosmos change-feed triggers become in-process
// ChangeFeedProcessors. Function bodies are invoked unchanged.

using System.Reflection;
using Azure.Messaging.ServiceBus;
using Hangfire;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Hosting;

public sealed class FunctionDescriptor
{
    public required string Name { get; init; }
    public required Type DeclaringType { get; init; }
    public required MethodInfo Method { get; init; }
    public string? Cron { get; init; }
}

public sealed class FunctionWorkerRegistry
{
    public List<FunctionDescriptor> ServiceBus { get; } = [];
    public List<FunctionDescriptor> Timers { get; } = [];
    public List<FunctionDescriptor> ChangeFeeds { get; } = [];

    public static FunctionWorkerRegistry Discover(Assembly assembly)
    {
        var registry = new FunctionWorkerRegistry();
        foreach (var type in assembly.GetTypes().Where(t => t.IsClass && !t.IsAbstract))
        {
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                var name = method.GetCustomAttribute<FunctionAttribute>()?.Name ?? method.Name;
                var descriptor = new FunctionDescriptor
                {
                    Name = name,
                    DeclaringType = type,
                    Method = method,
                };

                if (FunctionAttributeLookup.Get<ServiceBusTriggerAttribute>(method) is not null)
                    registry.ServiceBus.Add(descriptor);
                else if (FunctionAttributeLookup.Get<TimerTriggerAttribute>(method) is { } timer)
                    registry.Timers.Add(new FunctionDescriptor
                    {
                        Name = name,
                        DeclaringType = type,
                        Method = method,
                        Cron = timer.Schedule,
                    });
                else if (FunctionAttributeLookup.Get<CosmosDBTriggerAttribute>(method) is not null)
                    registry.ChangeFeeds.Add(descriptor);
            }
        }
        return registry;
    }
}

public interface ITimerJobDispatcher
{
    Task RunAsync(string functionName);
}

public sealed class TimerJobDispatcher(
    IServiceProvider services,
    FunctionWorkerRegistry registry,
    Func<FunctionDescriptor, IServiceProvider, CancellationToken, Task> invoker,
    ILogger<TimerJobDispatcher> logger) : ITimerJobDispatcher
{
    public async Task RunAsync(string functionName)
    {
        var descriptor = registry.Timers.FirstOrDefault(x => x.Name == functionName)
                         ?? throw new InvalidOperationException($"Unknown timer function '{functionName}'");
        logger.LogInformation("Executing scheduled function {Function}", functionName);
        await invoker(descriptor, services, CancellationToken.None);
    }
}

public static class FunctionWorkerHostExtensions
{
    /// <summary>
    /// Registers Hangfire recurring jobs for every [TimerTrigger] function in
    /// the assembly. Cron strings carry over verbatim.
    /// </summary>
    public static IHostApplicationBuilder AddFunctionTimers(
        this IHostApplicationBuilder builder,
        FunctionWorkerRegistry registry,
        Func<FunctionDescriptor, IServiceProvider, CancellationToken, Task> invoker)
    {
        builder.Services.AddSingleton(registry);
        builder.Services.AddSingleton<ITimerJobDispatcher>(sp =>
            new TimerJobDispatcher(sp, registry, invoker,
                sp.GetRequiredService<ILogger<TimerJobDispatcher>>()));

        foreach (var timer in registry.Timers)
        {
            var cron = FunctionAttributeLookup.Get<TimerTriggerAttribute>(timer.Method)!.Schedule;
            RecurringJob.AddOrUpdate<ITimerJobDispatcher>(
                timer.Name,
                dispatcher => dispatcher.RunAsync(timer.Name),
                cron,
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc });
        }

        return builder;
    }

    /// <summary>
    /// Registers every discovered Service Bus queue consumer and Cosmos change
    /// feed processor as an <see cref="IHostedService"/>. Call
    /// <see cref="AddFunctionTimers"/> as well to register the timer jobs.
    /// </summary>
    public static IHostApplicationBuilder AddFunctionWorkers(
        this IHostApplicationBuilder builder,
        FunctionWorkerRegistry registry,
        Func<FunctionDescriptor, IServiceProvider, CancellationToken, Task> invoker,
        bool enableQueues = true,
        bool enableChangeFeeds = true,
        Func<string, bool>? includeQueue = null)
    {
        builder.Services.AddSingleton(registry);

        if (enableQueues)
        foreach (var descriptor in registry.ServiceBus)
        {
            var trigger = FunctionAttributeLookup.Get<ServiceBusTriggerAttribute>(descriptor.Method)!;
            if (includeQueue is not null && !includeQueue(trigger.QueueName))
                continue;
            var options = new ServiceBusQueueConsumer.Options
            {
                QueueName = trigger.QueueName,
                FunctionName = descriptor.Name,
                BatchSize = trigger.IsBatched ? 20 : 1,
            };
            builder.Services.AddSingleton<IHostedService>(sp =>
                new ServiceBusQueueConsumer(
                    sp.GetRequiredService<Azure.Messaging.ServiceBus.ServiceBusClient>(),
                    options,
                    sp,
                    registry,
                    sp.GetRequiredService<ILogger<ServiceBusQueueConsumer>>()));
        }

        if (enableChangeFeeds)
        {
            foreach (var descriptor in registry.ChangeFeeds)
            {
                var trigger = FunctionAttributeLookup.Get<CosmosDBTriggerAttribute>(descriptor.Method)!;
                var options = new ChangeFeedWorker.Options
                {
                    FunctionName = descriptor.Name,
                    DatabaseName = trigger.DatabaseName,
                    ContainerName = trigger.ContainerName,
                    LeasePrefix = string.IsNullOrWhiteSpace(trigger.LeaseContainerPrefix)
                        ? descriptor.Name
                        : trigger.LeaseContainerPrefix!,
                };
                builder.Services.AddSingleton<IHostedService>(sp =>
                    new ChangeFeedWorker(
                        sp.GetRequiredService<CosmosClient>(),
                        options,
                        sp,
                        registry,
                        sp.GetRequiredService<Microsoft.Extensions.Configuration.IConfiguration>(),
                        sp.GetRequiredService<ILogger<ChangeFeedWorker>>()));
            }
        }

        return builder;
    }
}
