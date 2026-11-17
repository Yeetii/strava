using System.Reflection;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Hosting;

/// <summary>Invokes a discovered function body with the arguments its signature asks for.</summary>
public static class FunctionInvoker
{
    public static async Task InvokeAsync(
        FunctionDescriptor descriptor,
        IServiceProvider services,
        CancellationToken ct,
        ServiceBusReceivedMessage[]? messages = null,
        ServiceBusMessageActions? actions = null,
        IReadOnlyList<object>? feedChanges = null)
    {
        var instance = ActivatorUtilities.CreateInstance(services, descriptor.DeclaringType);
        var args = descriptor.Method.GetParameters().Select(p =>
        {
            if (p.ParameterType == typeof(ServiceBusReceivedMessage[]))
                return messages ?? [];
            if (p.ParameterType == typeof(ServiceBusMessageActions))
                return actions ?? new ServiceBusMessageActions();
            if (p.ParameterType == typeof(CancellationToken))
                return ct;
            if (p.ParameterType == typeof(IReadOnlyList<object>))
                return feedChanges ?? (IReadOnlyList<object>)Array.Empty<object>();
            if (p.ParameterType.IsGenericType &&
                p.ParameterType.GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
                return feedChanges ?? (object)Array.CreateInstance(p.ParameterType.GetGenericArguments()[0], 0);
            return p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
        }).ToArray();

        var result = descriptor.Method.Invoke(instance, args);
        if (result is Task task)
            await task;
    }
}

/// <summary>
/// P1a queue consumer. Uses the Service Bus SDK processor (one per queue,
/// maxConcurrentCalls = 1, prefetch 20) and batches messages up to the size the
/// original trigger used before invoking the unchanged worker body.
/// </summary>
public sealed class ServiceBusQueueConsumer(
    ServiceBusClient client,
    ServiceBusQueueConsumer.Options options,
    IServiceProvider services,
 FunctionWorkerRegistry registry,
    ILogger<ServiceBusQueueConsumer> logger) : BackgroundService
{
    public sealed class Options
    {
        public required string QueueName { get; init; }
        public required string FunctionName { get; init; }
        public int BatchSize { get; init; } = 20;
        public TimeSpan BatchWindow { get; init; } = TimeSpan.FromMilliseconds(500);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var descriptor = registry.ServiceBus.First(x => x.Name == options.FunctionName);
        var processor = client.CreateProcessor(options.QueueName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            PrefetchCount = 20,
            AutoCompleteMessages = false,
        });

        var channel = Channel.CreateBounded<(ServiceBusReceivedMessage Message, ProcessMessageEventArgs Args)>(1000);
        processor.ProcessMessageAsync += async args =>
            await channel.Writer.WriteAsync((args.Message, args), stoppingToken);
        processor.ProcessErrorAsync += args =>
        {
            logger.LogError(args.Exception, "Service Bus processor error on {Queue}", options.QueueName);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync(stoppingToken);
        try
        {
            await ConsumeAsync(channel.Reader, descriptor, stoppingToken);
        }
        finally
        {
            await processor.StopProcessingAsync(CancellationToken.None);
            await processor.DisposeAsync();
        }
    }

    private async Task ConsumeAsync(
        ChannelReader<(ServiceBusReceivedMessage Message, ProcessMessageEventArgs Args)> reader,
        FunctionDescriptor descriptor,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (!await reader.WaitToReadAsync(ct))
                break;

            var batch = new List<(ServiceBusReceivedMessage Message, ProcessMessageEventArgs Args)>();
            using var windowCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var window = Task.Delay(options.BatchWindow, windowCts.Token);

            while (batch.Count < options.BatchSize)
            {
                if (!reader.TryRead(out var item))
                {
                    var wait = reader.WaitToReadAsync(windowCts.Token).AsTask();
                    if (await Task.WhenAny(wait, window) == window)
                        break;
                    if (!await wait)
                        break;
                    continue;
                }
                batch.Add(item);
            }
            windowCts.Cancel();

            var handlers = batch.ToDictionary(x => x.Message, x => x.Args);
            var actions = new ServiceBusMessageActions(
                (m, t) => handlers[m].CompleteMessageAsync(m, t),
                (m, t) => handlers[m].AbandonMessageAsync(m, null, t),
                (m, r, d, t) => handlers[m].DeadLetterMessageAsync(m, null, r, d, t),
                (m, t) => handlers[m].RenewMessageLockAsync(m, t));

            try
            {
                await FunctionInvoker.InvokeAsync(descriptor, services, ct,
                    messages: batch.Select(x => x.Message).ToArray(), actions: actions);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to process a batch from {Queue}", options.QueueName);
                foreach (var (message, _) in batch)
                {
                    try { await actions.AbandonMessageAsync(message, ct); }
                    catch { /* message lock may already be gone; it will be redelivered */ }
                }
            }
        }
    }
}

/// <summary>
/// P1a in-process Cosmos change-feed processor. Uses a fresh lease prefix so it
/// does not adopt Functions-managed lease documents (see the plan's semantics
/// mapping).
/// </summary>
public sealed class ChangeFeedWorker(
    CosmosClient client,
    ChangeFeedWorker.Options options,
    IServiceProvider services,
    FunctionWorkerRegistry registry,
    IConfiguration configuration,
    ILogger<ChangeFeedWorker> logger) : BackgroundService
{
    public sealed class Options
    {
        public required string FunctionName { get; init; }
        public required string DatabaseName { get; init; }
        public required string ContainerName { get; init; }
        public required string LeasePrefix { get; init; }
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var descriptor = registry.ChangeFeeds.First(x => x.Name == options.FunctionName);
        var container = client.GetContainer(options.DatabaseName, options.ContainerName);
        var leaseContainerName = $"{options.ContainerName}-leases";
        await EnsureLeaseContainerAsync(client.GetDatabase(options.DatabaseName), options.ContainerName, ct);
        var leaseContainer = client.GetContainer(options.DatabaseName, leaseContainerName);
        var instanceName = $"{Environment.MachineName}-{Guid.NewGuid():N}";

        var parameterType = descriptor.Method.GetParameters()
            .First(p => p.ParameterType.IsGenericType &&
                        p.ParameterType.GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
            .ParameterType.GetGenericArguments()[0];

        var builder = container.GetChangeFeedProcessorBuilder<object>(
                processorName: $"plain-{options.LeasePrefix}",
                onChangesDelegate: async (changes, cancellation) =>
                {
                    var typed = CastChanges(changes, parameterType);
                    await FunctionInvoker.InvokeAsync(descriptor, services, cancellation, feedChanges: typed);
                })
            .WithInstanceName(instanceName)
            .WithLeaseContainer(leaseContainer);

        var processor = builder.Build();
        logger.LogInformation("Starting change feed processor {Function} on {Container}",
            options.FunctionName, options.ContainerName);
        await processor.StartAsync();
        try
        {
            while (!ct.IsCancellationRequested)
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
        }
        finally
        {
            await processor.StopAsync();
        }
    }

    /// <summary>
    /// The Functions runtime created lease containers for CosmosDBTrigger
    /// (CreateLeaseContainerIfNotExists = true). The plain host uses fresh
    /// "{container}-leases" containers, so provision them before starting the
    /// processor or Cosmos answers the lease lookup with 404 and StopHost kills
    /// the whole worker.
    /// </summary>
    internal static async Task EnsureLeaseContainerAsync(
        Database database, string containerName, CancellationToken ct)
    {
        await database.CreateContainerIfNotExistsAsync(
            $"{containerName}-leases", "/id", cancellationToken: ct);
    }

    private static IReadOnlyList<object> CastChanges(IReadOnlyCollection<object> changes, Type target)
    {
        var result = new List<object>(changes.Count);
        foreach (var change in changes)
        {
            if (target.IsInstanceOfType(change))
            {
                result.Add(change);
                continue;
            }
            // With an `object` feed the SDK hands us a JsonElement; materialize
            // it into the concrete document type the function body expects.
            var json = change is JsonElement element
                ? element.GetRawText()
                : change.ToString() ?? "{}";
            var typed = JsonSerializer.Deserialize(json, target, FunctionsJson.Options);
            if (typed is not null)
                result.Add(typed);
        }
        return result;
    }
}
