using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Backend;

public class RaceDiscoveryWorker(
    DiscoverDuvRaces duvDiscovery,
    DiscoverTraceDeTrailRaces traceDeTrailDiscovery,
    DiscoverItraRaces itraDiscovery,
    DiscoverSkyrunningRaces skyrunningDiscovery,
    RaceDiscoveryService discoveryService,
    ILogger<RaceDiscoveryWorker> logger,
    ServiceBusAdministrationClient serviceBusAdministrationClient,
    ServiceBusClient serviceBusClient)
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(10);
    private static readonly JsonSerializerOptions JsonSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    [Function(nameof(RaceDiscoveryWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.RaceDiscoveryJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        RaceDiscoveryMessage? discoveryMessage;
        try
        {
            discoveryMessage = DeserializeMessage(message);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Race discovery message {MessageId} is not valid JSON", message.MessageId);
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: "InvalidMessage",
                deadLetterErrorDescription: ex.Message,
                cancellationToken: cancellationToken);
            return;
        }

        if (discoveryMessage is null || string.IsNullOrWhiteSpace(discoveryMessage.Agent))
        {
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: "InvalidMessage",
                deadLetterErrorDescription: "Race discovery message requires an agent.",
                cancellationToken: cancellationToken);
            return;
        }

        var normalizedMessage = discoveryMessage with { Agent = discoveryMessage.Agent.Trim().ToLowerInvariant() };

        if (await ServiceBusRescheduler.TryDeferForBackpressureAsync(
                serviceBusAdministrationClient,
                serviceBusClient,
                ServiceBusConfig.RaceDiscoveryJobs,
                message,
                actions,
                logger,
                cancellationToken))
        {
            return;
        }

        try
        {
            RaceDiscoveryMessage? nextMessage = normalizedMessage.Agent switch
            {
                "duv" => await duvDiscovery.ProcessPageAsync(normalizedMessage.CurrentPage, cancellationToken)
                    ? normalizedMessage with { Page = normalizedMessage.CurrentPage + 1 }
                    : null,
                "tracedetrail" => await traceDeTrailDiscovery.ProcessPageAsync(normalizedMessage.CurrentPage, cancellationToken)
                    ? normalizedMessage with { Page = normalizedMessage.CurrentPage + 1 }
                    : null,
                "itra" => await itraDiscovery.ProcessPageAsync(normalizedMessage, cancellationToken),
                "skyrunning" => await skyrunningDiscovery.ProcessPageAsync(normalizedMessage.CurrentPage, cancellationToken)
                    ? normalizedMessage with { Page = normalizedMessage.CurrentPage + 1 }
                    : null,
                _ => throw new NotSupportedException($"Unknown race discovery agent '{normalizedMessage.Agent}'.")
            };

            await actions.CompleteMessageAsync(message, cancellationToken);

            if (nextMessage is not null)
                await discoveryService.EnqueueDiscoveryMessageAsync(nextMessage, delay: null, cancellationToken);
        }
        catch (NotSupportedException ex)
        {
            logger.LogWarning(ex, "Unsupported race discovery agent {Agent}", normalizedMessage.Agent);
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: "UnsupportedAgent",
                deadLetterErrorDescription: ex.Message,
                cancellationToken: cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await discoveryService.EnqueueDiscoveryMessageAsync(normalizedMessage, RetryDelay, cancellationToken);
            logger.LogWarning(ex,
                "Race discovery {Agent} page {Page} failed; rescheduled same page after {Delay}",
                normalizedMessage.Agent, normalizedMessage.CurrentPage, RetryDelay);
            await actions.CompleteMessageAsync(message, cancellationToken);
        }
    }

    internal static RaceDiscoveryMessage? DeserializeMessage(ServiceBusReceivedMessage message)
        => JsonSerializer.Deserialize<RaceDiscoveryMessage>(message.Body, JsonSerializerOptions);
}
