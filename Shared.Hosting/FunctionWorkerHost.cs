// Worker-side "envelope" for the Azure Functions Runtime Exit campaign.
// Queues are Hangfire jobs produced by QueueClient, timers are Hangfire
// recurring jobs, and Cosmos change-feed triggers are in-process
// ChangeFeedProcessors. Function bodies are invoked unchanged.

using System.Reflection;
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
    public List<FunctionDescriptor> Queues { get; } = [];
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

                if (FunctionAttributeLookup.Get<QueueTriggerAttribute>(method) is not null)
                    registry.Queues.Add(descriptor);
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

/// <summary>
/// Registers each discovered timer as a Hangfire recurring job once the host
/// starts, using the DI-based manager rather than the static API.
/// </summary>
public sealed class RecurringJobRegistrationHostedService(
    IRecurringJobManager recurringJobs,
    FunctionWorkerRegistry registry,
    ILogger<RecurringJobRegistrationHostedService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var timer in registry.Timers)
        {
            var cron = FunctionAttributeLookup.Get<TimerTriggerAttribute>(timer.Method)!.Schedule;
            recurringJobs.AddOrUpdate<ITimerJobDispatcher>(
                timer.Name,
                dispatcher => dispatcher.RunAsync(timer.Name),
                cron,
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc });
        }

        logger.LogInformation("Registered {Count} Hangfire recurring jobs", registry.Timers.Count);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public static class FunctionWorkerHostExtensions
{
    /// <summary>
    /// Registers Hangfire recurring jobs for every [TimerTrigger] function in
    /// the assembly. Cron strings carry over verbatim. Registration happens in
    /// a hosted service via the DI-based IRecurringJobManager: the static
    /// RecurringJob API needs JobStorage.Current, which a Generic Host never
    /// initializes.
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
        builder.Services.AddSingleton<IHostedService>(sp =>
            new RecurringJobRegistrationHostedService(
                sp.GetRequiredService<IRecurringJobManager>(),
                registry,
                sp.GetRequiredService<ILogger<RecurringJobRegistrationHostedService>>()));

        return builder;
    }

    /// <summary>
    /// Registers every discovered Cosmos change-feed processor as an
    /// <see cref="IHostedService"/>. Queues are Hangfire jobs dispatched by
    /// QueueJobDispatcher; call <see cref="AddFunctionTimers"/> as well to
    /// register the timer jobs.
    /// </summary>
    public static IHostApplicationBuilder AddFunctionWorkers(
        this IHostApplicationBuilder builder,
        FunctionWorkerRegistry registry,
        Func<FunctionDescriptor, IServiceProvider, CancellationToken, Task> invoker,
        bool enableChangeFeeds = true)
    {
        builder.Services.AddSingleton(registry);

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
