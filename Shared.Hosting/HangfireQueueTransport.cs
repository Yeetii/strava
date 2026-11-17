// P2: per-queue Service Bus -> Hangfire swap.
//
// A ServiceBusClient subclass is registered in DI when a queue is flagged for
// Hangfire. Its CreateSender returns a sender whose SendMessageAsync enqueues a
// Hangfire job instead of writing to Service Bus. Producer bodies therefore
// stay byte-for-byte unchanged; only the transport envelope changes.
//
// Feature flag: HANGFIRE_QUEUES is a comma-separated list of queue names, or
// "*" for all queues. Empty (default) keeps Service Bus for every queue.

using System.Reflection;
using System.Text;
using Azure.Messaging.ServiceBus;
using Hangfire;
using Hangfire.Redis.StackExchange;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shared.Hosting;

public static class QueueTransport
{
    public const string FlagName = "HANGFIRE_QUEUES";

    private static readonly HashSet<string> HangfireQueues = Build();

    private static HashSet<string> Build()
    {
        var value = Environment.GetEnvironmentVariable(FlagName) ?? string.Empty;
        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public static bool IsHangfire(string queueName) =>
        HangfireQueues.Contains("*") || HangfireQueues.Contains(queueName);

    public static bool AnyHangfireQueues => HangfireQueues.Count > 0;

    /// <summary>
    /// Registers Hangfire storage exactly once per host. Call this from the host
    /// entry point; the producer shim below deliberately does not call it.
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

    /// <summary>Register the transport shim in a host that produces jobs.</summary>
    public static void AddHangfireQueueProducer(IServiceCollection services)
    {
        if (!AnyHangfireQueues)
            return;

        services.AddTransient<QueueJobDispatcher>();
        services.AddSingleton<ServiceBusClient>(sp =>
            new HangfireServiceBusClient(sp.GetRequiredService<IBackgroundJobClient>()));
    }
}

public sealed class HangfireServiceBusClient(IBackgroundJobClient jobs) : ServiceBusClient
{
    public override ServiceBusSender CreateSender(string queueName) =>
        new HangfireServiceBusSender(queueName, jobs);

    public override ServiceBusSender CreateSender(string queueName, ServiceBusSenderOptions options) =>
        new HangfireServiceBusSender(queueName, jobs);
}

public sealed class HangfireServiceBusSender(string queueName, IBackgroundJobClient jobs) : ServiceBusSender
{
    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        jobs.Enqueue<QueueJobDispatcher>(dispatcher =>
            dispatcher.Execute(queueName, message.Body.ToString()));
        return Task.CompletedTask;
    }

    public override Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages,
        CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            SendMessageAsync(message, cancellationToken);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Hangfire job target. Reuses the discovered worker bodies by fabricating the
/// ServiceBusReceivedMessage envelope they expect; settlement calls are no-ops
/// because Hangfire owns retries and the dead-letter list.
/// </summary>
public sealed class QueueJobDispatcher(
    FunctionWorkerRegistry registry,
    IServiceProvider services)
{
    [AutomaticRetry(Attempts = 5, DelaysInSeconds = [60, 300, 900, 1800, 3600])]
    public async Task Execute(string queueName, string payload)
    {
        var descriptor = registry.ServiceBus.FirstOrDefault(d =>
            string.Equals(FunctionAttributeLookup.Get<ServiceBusTriggerAttribute>(d.Method)?.QueueName,
                queueName, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"No worker function for queue '{queueName}'");

        var message = FakeReceivedMessage.Create(payload);
        await FunctionInvoker.InvokeAsync(descriptor, services, CancellationToken.None,
            messages: [message],
            actions: new ServiceBusMessageActions());
    }
}

internal static class FakeReceivedMessage
{
    public static ServiceBusReceivedMessage Create(string payload)
    {
        // ServiceBusReceivedMessage has an internal (ReadOnlyMemory<byte>) ctor.
        // Hangfire payloads are plain body strings, so that is sufficient: the
        // bodies only read Body.ToString() and MessageId.
        var ctor = typeof(ServiceBusReceivedMessage).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(ReadOnlyMemory<byte>)],
            modifiers: null)
            ?? throw new InvalidOperationException("ServiceBusReceivedMessage factory ctor not found");

        var message = (ServiceBusReceivedMessage)ctor.Invoke(new object[]
        {
            new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(payload)),
        });

        // DeliveryCount is an internal settable property; several worker bodies
        // log it on failure, and reading an unset value throws.
        typeof(ServiceBusReceivedMessage)
            .GetProperty("DeliveryCount", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?
            .SetValue(message, 1);

        return message;
    }
}
