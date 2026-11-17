// Queues run exclusively on Hangfire/Redis. QueueClient.CreateSender forwards
// producer sends to QueueJobDispatcher, which re-invokes the original worker
// body with an SDK-free QueueMessage. No Service Bus connection is needed.

using Hangfire;
using Hangfire.Redis.StackExchange;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shared.Hosting;

public static class QueueTransport
{
    /// <summary>
    /// Registers Hangfire storage exactly once per host. Call this from the host
    /// entry point before resolving any queue producer.
    /// </summary>
    public static void AddHangfireStorage(IServiceCollection services, IConfiguration configuration)
    {
        var redis = configuration.GetValue<string>("RedisConnectionString") ?? "redis:6379,abortConnect=false";
        services.AddHangfire(config => config
            .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
            .UseSimpleAssemblyNameTypeSerializer()
            .UseRecommendedSerializerSettings()
            .UseRedisStorage(redis));
    }

    /// <summary>Registers the Hangfire-backed queue producer in a host that produces jobs.</summary>
    public static void AddHangfireQueueProducer(IServiceCollection services)
    {
        services.AddTransient<QueueJobDispatcher>();
        services.AddSingleton<QueueClient>(sp =>
            new QueueClient(sp.GetRequiredService<IBackgroundJobClient>()));
    }
}

/// <summary>
/// Hangfire job target. Reuses the discovered worker bodies by building the
/// SDK-free QueueMessage envelope they expect; settlements are no-ops because
/// Hangfire owns retries and the dead-letter list.
/// </summary>
public sealed class QueueJobDispatcher(
    FunctionWorkerRegistry registry,
    IServiceProvider services)
{
    [AutomaticRetry(Attempts = 5, DelaysInSeconds = [60, 300, 900, 1800, 3600])]
    [JobDisplayName("{0}")]
    public async Task Execute(string queueName, QueueMessageEnvelope envelope)
    {
        var descriptor = registry.Queues.FirstOrDefault(d =>
            string.Equals(FunctionAttributeLookup.Get<QueueTriggerAttribute>(d.Method)?.QueueName,
                queueName, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"No worker function for queue '{queueName}'");

        await FunctionInvoker.InvokeAsync(descriptor, services, CancellationToken.None,
            messages: [envelope.ToMessage()],
            actions: new QueueMessageActions());
    }
}
