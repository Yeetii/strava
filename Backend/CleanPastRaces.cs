using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

public class CleanPastRaces(
    RaceCollectionClient racesCollectionClient,
    ILogger<CleanPastRaces> logger)
{
    [Function(nameof(CleanPastRaces))]
    public async Task Run(
        [TimerTrigger("0 0 4 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var (expired, cutoff) = await racesCollectionClient.ExpirePastRacesAsync(cancellationToken);
        logger.LogInformation("CleanPastRaces: marked {Count} past race(s) for TTL expiry with date before {Cutoff}", expired, cutoff);
    }
}
