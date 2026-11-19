using Stigar.Core.Services;
using Stigar.Core.Jobs;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Stigar.Workers;

public class RaceDiscoveryWorker(
    DiscoverDuvRaces duvDiscovery,
    DiscoverTraceDeTrailRaces traceDeTrailDiscovery,
    DiscoverItraRaces itraDiscovery,
    DiscoverSkyrunningRaces skyrunningDiscovery,
    DiscoverMittloppRaces mittloppDiscovery,
    RaceDiscoveryService discoveryService,
    ILogger<RaceDiscoveryWorker> logger) : IRaceDiscoveryJob
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(10);
    private static readonly JsonSerializerOptions JsonSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task ExecuteAsync(RaceDiscoveryMessage job, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(job.Agent))
        {
            logger.LogWarning("Race discovery message requires an agent.");
            return;
        }

        var normalizedMessage = job with { Agent = job.Agent.Trim().ToLowerInvariant() };

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
                "mittlopp" => await mittloppDiscovery.ProcessPageAsync(normalizedMessage.CurrentPage, cancellationToken)
                    ? normalizedMessage with { Page = normalizedMessage.CurrentPage + 1 }
                    : null,
                _ => throw new NotSupportedException($"Unknown race discovery agent '{normalizedMessage.Agent}'.")
            };

            if (nextMessage is not null)
                await discoveryService.EnqueueDiscoveryMessageAsync(nextMessage, delay: null, cancellationToken);
        }
        catch (NotSupportedException ex)
        {
            logger.LogWarning(ex, "Unsupported race discovery agent {Agent}", normalizedMessage.Agent);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await discoveryService.EnqueueDiscoveryMessageAsync(normalizedMessage, RetryDelay, cancellationToken);
            logger.LogWarning(ex,
                "Race discovery {Agent} page {Page} failed; rescheduled same page after {Delay}",
                normalizedMessage.Agent, normalizedMessage.CurrentPage, RetryDelay);
        }
    }

    // Retained for the existing deserialization tests. The typed job no longer needs it at
    // runtime; the shim type is fully qualified so no Azure Functions using is required.
    internal static RaceDiscoveryMessage? DeserializeMessage(string body)
        => JsonSerializer.Deserialize<RaceDiscoveryMessage>(body, JsonSerializerOptions);
}
